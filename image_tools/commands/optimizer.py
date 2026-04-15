import os
import shutil
import argparse
import zipfile
import rarfile
import py7zr
import send2trash
import time
import psutil
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from PIL import Image, ImageSequence, UnidentifiedImageError
import io

from image_tools import settings as app_settings

# ---------------------------------------------------------
# 設定 (Configuration)
# ---------------------------------------------------------
CONFIG = {
    "WORKER_COUNT": 2,
    "AVIF_QUALITY": 55,
    "AVIF_SPEED": 5,
    "IMAGE_EXTS": {'.avif', '.bmp', '.gif', '.jfif', '.jpg', '.jpeg', '.png', '.webp', '.tiff'},
    "TARGET_EXTS": {'.bmp', '.gif', '.jfif', '.jpg', '.jpeg', '.png', '.webp', '.tiff'},
    "ARCHIVE_EXTS": {'.zip', '.rar', '.7z'},
    "EXIFTOOL_PATH": app_settings.load_settings().get("EXIFTOOL_PATH") or "",
}

# ---------------------------------------------------------
# 統計情報クラス
# ---------------------------------------------------------
class Stats:
    def __init__(self):
        self.replaced_count = 0
        self.saved_bytes = 0
        self.uncompressed_zips = 0
        self.skipped_zips = 0
        self.start_time = time.time()

    def format_bytes(self, size):
        power = 2**10
        n = 0
        power_labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
        while abs(size) >= power and n < 4:
            size /= power
            n += 1
        return f"{size:.2f} {power_labels[n]}"

    def print_summary(self):
        elapsed = time.time() - self.start_time
        print("\n" + "="*50)
        print(" [処理完了 - 統計レポート]")
        print(f" ⏱️  処理時間: {elapsed:.2f}秒")
        print(f" 🖼️  置き換えた画像数: {self.replaced_count}枚")
        print(f" 💾  削減した容量: {self.format_bytes(self.saved_bytes)}")
        print(f" 📦  無圧縮化したZip数: {self.uncompressed_zips}")
        print(f" ⏭️  スキップしたZip数: {self.skipped_zips}")
        print("="*50 + "\n")

global_stats = Stats()

# ---------------------------------------------------------
# システム設定・ヘルパー
# ---------------------------------------------------------
def set_low_priority():
    try:
        p = psutil.Process(os.getpid())
        if os.name == 'nt':
            p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        else:
            os.nice(10)
    except Exception:
        pass

def is_image(path):
    return path.suffix.lower() in CONFIG['IMAGE_EXTS']

def is_archive(path):
    return path.suffix.lower() in CONFIG['ARCHIVE_EXTS']

def safe_delete(path):
    try:
        if not path.exists():
            return
        if path.is_file() and is_image(path):
            os.remove(path) 
        else:
            send2trash.send2trash(str(path))
    except Exception as e:
        print(f"Warning: Could not delete {path}: {e}")

def get_folder_contents(folder_path):
    images = []
    archives = []
    folders = []
    try:
        if folder_path.exists(): 
            for item in folder_path.iterdir():
                if item.is_dir():
                    folders.append(item)
                elif is_image(item):
                    images.append(item)
                elif is_archive(item):
                    archives.append(item)
    except FileNotFoundError:
        pass
    return folders, images, archives

def count_total_folders(root_path):
    count = 0
    for _, dirs, _ in os.walk(root_path):
        count += len(dirs)
    return count + 1 

# ---------------------------------------------------------
# 画像変換処理
# ---------------------------------------------------------
def process_single_image(file_path, as_grayscale=False, min_size_mb=0, max_size_mb=None):
    if not file_path.exists():
        return False, 0, None, file_path

    # 一時ファイルのパスを初期化（エラー時のクリーンアップ用）
    temp_path = None
    
    try:
        original_size = file_path.stat().st_size
        size_mb = original_size / (1024 * 1024)

        # 容量条件の判定
        if size_mb < min_size_mb:
            # 最小サイズ未満はスキップ
            return False, 0, None, file_path
        
        if max_size_mb is not None and size_mb > max_size_mb:
            # 最大サイズ超過はスキップ
            return False, 0, None, file_path
        
        with Image.open(file_path) as img:
            is_animated = getattr(img, "is_animated", False)
            buffer = io.BytesIO()
            save_kwargs = {
                "format": "AVIF",
                "quality": CONFIG['AVIF_QUALITY'],
                "speed": CONFIG['AVIF_SPEED'],
                "optimize": True,
            }

            if img.mode in ('P', 'PA') or (img.mode == 'RGBA') or ('transparency' in img.info):
                img = img.convert("RGBA")
            elif img.mode == 'CMYK':
                img = img.convert("RGB")
            
            icc = img.info.get('icc_profile')
            if icc:
                save_kwargs['icc_profile'] = icc
            
            if is_animated:
                frames = []
                for frame in ImageSequence.Iterator(img):
                    f = frame.copy().convert("RGBA")
                    if as_grayscale:
                        f = f.convert("LA")
                    frames.append(f)
                frames[0].save(buffer, save_all=True, append_images=frames[1:], **save_kwargs)
            else:
                if as_grayscale:
                    if img.mode == 'RGBA' or 'transparency' in img.info:
                        img = img.convert("LA")
                    else:
                        img = img.convert("L")
                img.save(buffer, **save_kwargs)

            new_size = buffer.tell()
            
            if new_size < original_size:
                new_path = file_path.with_suffix('.avif')
                
                # 同名ファイル(既に.avif)の場合は一時ファイルを経由する
                is_same_file = (new_path.resolve() == file_path.resolve())
                temp_path = file_path.with_name(file_path.stem + "_temp_avif.tmp") if is_same_file else new_path

                with open(temp_path, "wb") as f:
                    f.write(buffer.getvalue())
                
                # ExifToolでメタデータコピー (絶対パスを使用)
                exiftool_path = CONFIG.get('EXIFTOOL_PATH', '')
                if os.path.exists(exiftool_path):
                    cmd = [
                        exiftool_path,
                        "-TagsFromFile", str(file_path.resolve()),
                        "-all:all",
                        "-overwrite_original",
                        str(temp_path.resolve())
                    ]
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

                # 同名ファイルだった場合は、一時ファイルで元ファイルを上書き
                if is_same_file:
                    temp_path.replace(new_path)

                return True, (original_size - new_size), new_path, file_path
            else:
                return False, 0, None, file_path

    except (UnidentifiedImageError, OSError, Exception) as e:
        # 処理中にエラーが起きた場合、作りかけの一時ファイルがあれば削除する
        if temp_path and temp_path.exists():
            try:
                os.remove(temp_path)
            except Exception:
                pass
        return False, 0, None, file_path

# ---------------------------------------------------------
# コアロジック
# ---------------------------------------------------------
def handle_archive(file_path, executor, args, pbar_global):
    should_extract = True
    is_zip = file_path.suffix.lower() == '.zip'

    if is_zip:
        try:
            with zipfile.ZipFile(file_path, 'r') as z:
                infolist = z.infolist()
                all_stored = all(info.compress_type == zipfile.ZIP_STORED for info in infolist if not info.is_dir())
                has_target_img = any(os.path.splitext(n)[1].lower() in CONFIG['TARGET_EXTS'] for n in z.namelist())
                
                if all_stored and not has_target_img:
                    should_extract = False
                    global_stats.skipped_zips += 1
        except zipfile.BadZipFile:
            print(f"⚠️ Bad zip file: {file_path}")
            return

    if should_extract:
        # 展開先フォルダの競合を回避（連番付与）
        extract_dir = file_path.parent / file_path.stem
        if extract_dir.exists():
            counter = 1
            while (file_path.parent / f"{file_path.stem}_extracted_{counter}").exists():
                counter += 1
            extract_dir = file_path.parent / f"{file_path.stem}_extracted_{counter}"
        
        extract_dir.mkdir(parents=True, exist_ok=True)

        try:
            if is_zip:
                with zipfile.ZipFile(file_path, 'r') as z:
                    z.extractall(extract_dir)
            elif file_path.suffix.lower() == '.rar':
                with rarfile.RarFile(file_path) as r:
                    r.extractall(extract_dir)
            elif file_path.suffix.lower() == '.7z':
                with py7zr.SevenZipFile(file_path, mode='r') as z:
                    z.extractall(path=extract_dir)
            
            safe_delete(file_path)
            process_directory(extract_dir, executor, args, pbar_global)

        except Exception as e:
            print(f"⚠️ Failed to extract {file_path}: {e}")
            if extract_dir.exists() and not any(extract_dir.iterdir()):
                shutil.rmtree(extract_dir)

def flatten_directory(current_path):
    folders, images, archives = get_folder_contents(current_path)
    
    for subfolder in folders:
        sub_folders, sub_images, sub_archives = get_folder_contents(subfolder)
        total_sub_items = len(sub_folders) + len(sub_images) + len(sub_archives)

        if total_sub_items == 0:
            safe_delete(subfolder)
            return True

        if len(sub_images) == 0 and len(sub_archives) == 0 and len(sub_folders) == 1:
            target_inner = sub_folders[0]
            dest_path = current_path / target_inner.name
            
            if not dest_path.exists():
                try:
                    shutil.move(str(target_inner), str(current_path))
                    safe_delete(subfolder)
                    return True
                except Exception as e:
                    print(f"⚠️ Flatten folder error: {e}")

        if len(sub_images) == 0 and len(sub_folders) == 0 and len(sub_archives) == 1:
            target_archive = sub_archives[0]
            dest_path = current_path / target_archive.name
            
            if not dest_path.exists():
                try:
                    shutil.move(str(target_archive), str(dest_path))
                    safe_delete(subfolder)
                    return True
                except Exception as e:
                    print(f"⚠️ Flatten archive error: {e}")
    
    return False

def pack_to_zip(folder_path):
    folders, images, archives = get_folder_contents(folder_path)
    
    if len(folders) > 0 or len(archives) > 0:
        return
    if not images:
        return

    zip_path = folder_path.parent / (folder_path.name + ".zip")
    
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED) as z:
            for img in images:
                if img.exists():
                    z.write(img, arcname=img.name)
        
        safe_delete(folder_path)
        global_stats.uncompressed_zips += 1
        
    except Exception as e:
        print(f"Failed to zip {folder_path}: {e}")
        if zip_path.exists():
            safe_delete(zip_path)

def process_images_in_folder(folder_path, executor, args):
    _, images, _ = get_folder_contents(folder_path)
    candidates = [img for img in images if img.suffix.lower() in CONFIG['TARGET_EXTS']]
    
    if not candidates:
        return

    folder_saved_bytes = 0
    folder_replaced_count = 0
    to_delete = []
    
    futures = {
        executor.submit(
            process_single_image, img, args.grayscale, args.min_size, args.max_size
        ): img 
        for img in candidates
    }
    
    desc = f" Converting imgs"
    
    for future in tqdm(as_completed(futures), total=len(candidates), desc=desc, leave=False, unit="img"):
        success, saved, avif_path, original_path = future.result()
        
        if success and avif_path:
            try:
                # 変換されたファイルが正常に開けるかテスト
                with Image.open(avif_path) as test_img:
                    test_img.load()
                
                folder_saved_bytes += saved
                folder_replaced_count += 1
                
                # パスが異なる場合のみ（元が.avif以外の場合のみ）旧ファイルを削除リストに追加
                if str(original_path.resolve()) != str(avif_path.resolve()):
                    to_delete.append(original_path)
                    
            except Exception as e:
                # テスト失敗時は破損している可能性があるので削除
                if avif_path.exists():
                    try:
                        os.remove(avif_path)
                    except Exception:
                        pass

    # 不要になった元ファイルを削除
    if to_delete:
        for p in to_delete:
            safe_delete(p)

    if folder_replaced_count > 0:
        global_stats.replaced_count += folder_replaced_count
        global_stats.saved_bytes += folder_saved_bytes

def process_directory(current_path, executor, args, pbar_global):
    if not current_path.exists():
        return

    while True:
        changed = flatten_directory(current_path)
        if not changed:
            break

    folders, images, archives = get_folder_contents(current_path)

    for arc in archives:
        if arc.exists():
            handle_archive(arc, executor, args, pbar_global)
    
    folders_refreshed, _, _ = get_folder_contents(current_path)
    
    for folder in folders_refreshed:
        process_directory(folder, executor, args, pbar_global)

    while True:
        changed = flatten_directory(current_path)
        if not changed:
            break

    process_images_in_folder(current_path, executor, args)

    if args.zip:
        pack_to_zip(current_path)

    if pbar_global:
        pbar_global.update(1)
        pbar_global.set_description(f"Scanning: {current_path.name[:20]}")

# ---------------------------------------------------------
# メイン
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Image & Archive Optimizer Script V4.2")
    parser.add_argument("root_dir", type=str, help="Target root directory path")
    parser.add_argument("--zip", action="store_true", help="Pack folders into uncompressed zip after processing")
    parser.add_argument("--grayscale", action="store_true", help="Convert images to grayscale (L/LA)")
    parser.add_argument("--workers", type=int, default=CONFIG['WORKER_COUNT'], help="Number of worker processes")
    parser.add_argument("--min-size", type=float, default=0, help="Minimum file size in MB to process")
    parser.add_argument("--max-size", type=float, default=None, help="Maximum file size in MB to process")
    parser.add_argument("--ext", nargs="+", help="Target extensions to process (e.g., --ext jpg png)")

    args = parser.parse_args()
    CONFIG['WORKER_COUNT'] = args.workers

    if args.ext:
        new_targets = set()
        for ext in args.ext:
            ext = ext.lower()
            if not ext.startswith("."):
                ext = "." + ext
            new_targets.add(ext)
        CONFIG['TARGET_EXTS'] = new_targets
        # 指定された拡張子を確実に画像として認識させるため IMAGE_EXTS にも追加
        CONFIG['IMAGE_EXTS'] = CONFIG['IMAGE_EXTS'].union(new_targets)

    set_low_priority()
    target_root = Path(args.root_dir).resolve()
    
    if not target_root.exists():
        print("❌ 指定されたフォルダが存在しません。")
        return

    print(f"🚀 Processing Start: {target_root}")
    mode_info = []
    if args.zip: mode_info.append("Zip:ON")
    if args.grayscale: mode_info.append("Grayscale:ON")
    
    print("📂 フォルダ総数を計算中...")
    total_folders = count_total_folders(target_root)
    print(f"ℹ️  Target Folders: {total_folders}, Modes: {', '.join(mode_info) if mode_info else 'Normal'}")

    try:
        with tqdm(total=total_folders, unit="dir", position=0, leave=True) as pbar:
            with ProcessPoolExecutor(max_workers=CONFIG['WORKER_COUNT']) as executor:
                process_directory(target_root, executor, args, pbar)
            
    except KeyboardInterrupt:
        print("\n⚠️  処理が中断されました。")
    except Exception as e:
        print(f"\n❌ 予期せぬエラー: {e}")
        import traceback
        traceback.print_exc()
    
    global_stats.print_summary()

if __name__ == "__main__":
    main()