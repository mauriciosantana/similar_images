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
import traceback
import sqlite3
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from PIL import Image, ImageSequence, UnidentifiedImageError, ImageOps, ImageFile
import io

from image_tools import settings as app_settings
from image_tools.commands.similar import load_config # config.json を読み込むため
from image_tools.paths import hash_cache_db

# ---------------------------------------------------------
# Pillow 設定 (Pillow Settings)
# ---------------------------------------------------------
ImageFile.LOAD_TRUNCATED_IMAGES = True
# 非常に大きい画像でもエラーにせず処理を試みる（メモリ不足は別途 catch する）
Image.MAX_IMAGE_PIXELS = 5_000_000_000

# ---------------------------------------------------------
# 設定 (Configuration)
# ---------------------------------------------------------
CONFIG = {
    "WORKER_COUNT": 2,
    "AVIF_QUALITY": 55,
    "AVIF_SPEED": 5,
    "IMAGE_EXTS": {'.avif', '.bmp', '.gif', '.jfif', '.jpg', '.jpeg', '.png', '.webp', '.tiff'},
    "TARGET_EXTS": {'.bmp', '.jfif', '.jpg', '.jpeg', '.png', '.webp', '.avif'},
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
class FastExifTool:
    """ExifToolを常駐させて高速に処理するクラス"""
    def __init__(self, executable):
        self.executable = executable
        self.process = None

    def start(self):
        if not self.executable or not os.path.exists(self.executable):
            return
        creationflags = 0x08000000 if os.name == 'nt' else 0
        self.process = subprocess.Popen(
            [self.executable, "-stay_open", "True", "-@", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", bufsize=1, creationflags=creationflags
        )

    def execute(self, *args):
        if not self.process or self.process.poll() is not None:
            self.start()
        if not self.process:
            return False
        
        for arg in args:
            self.process.stdin.write(arg + "\n")
        self.process.stdin.write("-execute\n")
        self.process.stdin.flush()
        
        output = ""
        while True:
            line = self.process.stdout.readline()
            if not line or line.strip() == "{ready}": break
            output += line
        return "files updated" in output or "image files read" in output

    def stop(self):
        if self.process:
            try:
                self.process.stdin.write("-stay_open\nFalse\n")
                self.process.stdin.flush()
                self.process.wait(timeout=2)
            except:
                self.process.kill()

# 各ワーカープロセスごとのグローバルインスタンス
worker_exiftool = None

def init_worker(exiftool_path):
    global worker_exiftool
    worker_exiftool = FastExifTool(exiftool_path)

def set_low_priority():
    try:
        p = psutil.Process(os.getpid())
        if os.name == 'nt':
            p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        else:
            os.nice(10)
    except Exception:
        pass

def get_db_cache():
    """DBからパス、サイズ、更新日時を取得して辞書で返す"""
    db_path = hash_cache_db()
    if not db_path.exists():
        return {}
    
    cache = {}
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # similar.py のスキーマ: 0:path, 1:mtime, 2:filesize
        cursor.execute("SELECT path, filesize, mtime FROM images")
        for row in cursor.fetchall():
            cache[row[0]] = {"filesize": row[1], "mtime": row[2]}
        conn.close()
    except Exception as e:
        print(f"⚠️  DB Cache load failed: {e}")
    return cache

def get_optimizer_mtimes():
    """最適化済みのフォルダMTimeキャッシュを取得"""
    db_path = hash_cache_db()
    if not db_path.exists(): return {}
    cache = {}
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS optimizer_mtimes (path TEXT PRIMARY KEY, mtime REAL)")
        c.execute("SELECT path, mtime FROM optimizer_mtimes")
        cache = dict(c.fetchall())
        conn.close()
    except Exception: pass
    return cache

def update_optimizer_mtime(folder_path, mtime):
    db_path = hash_cache_db()
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO optimizer_mtimes VALUES (?, ?)", (str(folder_path), mtime))
        conn.commit()
        conn.close()
    except Exception: pass

def safe_delete(path):
    try:
        if not path.exists():
            return
        if path.is_file() and path.suffix.lower() in CONFIG['IMAGE_EXTS']:
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
        with os.scandir(folder_path) as it:
            for entry in it:
                p = Path(entry.path)
                if entry.is_dir():
                    folders.append(p)
                elif entry.is_file():
                    ext = p.suffix.lower()
                    if ext in CONFIG['IMAGE_EXTS']:
                        images.append(p)
                    elif ext in CONFIG['ARCHIVE_EXTS']:
                        archives.append(p)
    except FileNotFoundError:
        pass
    return folders, images, archives

def _save_avif_robust(img, buffer, save_kwargs, is_animated=False, frames=None):
    """
    AVIF保存の試行錯誤（4:4:4失敗時の4:2:0フォールバック、およびLAモード失敗時のRGBA変換）
    """
    # 再帰の深さを制限
    attempt = save_kwargs.get("_retry_count", 0)
    if attempt > 3:
        raise MemoryError("AVIF conversion failed after multiple retries due to memory constraints.")

    try:
        if is_animated:
            img.save(buffer, save_all=True, append_images=frames[1:], **save_kwargs)
        else:
            img.save(buffer, **save_kwargs)
    except (Exception, MemoryError) as e:
        save_kwargs["_retry_count"] = attempt + 1
        err_msg = str(e).lower()
        
        # 1. 画像サイズが奇数の場合、偶数に微調整（エンコーダの制限回避）
        w, h = img.size
        if w % 2 != 0 or h % 2 != 0:
            new_size = (w + (w % 2), h + (h % 2))
            img = img.resize(new_size, resample=Image.LANCZOS)
            if is_animated and frames:
                frames = [f.resize(new_size, resample=Image.LANCZOS) for f in frames]
            buffer.seek(0); buffer.truncate()
            return _save_avif_robust(img, buffer, save_kwargs, is_animated, frames)

        # 2. メモリ不足エラーの場合、タイリング(分割処理)を有効化し、サンプリングを下げる
        if "tile_rows" not in save_kwargs:
            save_kwargs["tile_rows"] = 2
            save_kwargs["tile_cols"] = 2
            # subsampling は 4:4:4 のまま維持し、タイル分割だけでメモリ削減を試みる
            buffer.seek(0); buffer.truncate()
            return _save_avif_robust(img, buffer, save_kwargs, is_animated, frames)
        
        # それ以外のエラー、またはフォールバック後も失敗した場合は呼び出し元へ
        raise e

# ---------------------------------------------------------
# 画像変換処理
# ---------------------------------------------------------
def process_single_image(file_path, as_grayscale=None, min_size_mb=0, max_size_mb=None, target_size=None):
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

            # 白黒化の判定（全対象 or 指定色数以下）
            do_gs = False
            if as_grayscale is True:
                do_gs = True
            elif isinstance(as_grayscale, int) and as_grayscale != 0:
                if as_grayscale == -1:
                    do_gs = True
                else:
                    # ユニーク色数が指定値以下なら白黒化の対象とする
                    if img.getcolors(maxcolors=as_grayscale) is not None:
                        do_gs = True

            buffer = io.BytesIO()
            save_kwargs = {
                "format": "AVIF",
                "quality": CONFIG['AVIF_QUALITY'],
                "speed": CONFIG['AVIF_SPEED'],
                "optimize": True,
                "subsampling": "4:4:4",
            }

            if img.mode in ('P', 'PA') or (img.mode == 'RGBA') or ('transparency' in img.info):
                img = img.convert("RGBA")
            elif img.mode == 'CMYK':
                img = img.convert("RGB")
            
            icc = img.info.get('icc_profile')
            if icc:
                save_kwargs['icc_profile'] = icc
            
            if is_animated:
                # メモリ節約のため、透過が必要ない場合は RGB にする
                target_mode = "RGBA" if (img.mode in ('RGBA', 'P', 'PA') or 'transparency' in img.info) else "RGB"
                if do_gs:
                    target_mode = "LA" if target_mode == "RGBA" else "L"

                frames = []
                for frame in ImageSequence.Iterator(img):
                    f = frame.copy()
                    # EXIFを反映した上でリサイズ
                    f = ImageOps.exif_transpose(f)
                    if target_size:
                        f = f.resize(target_size, Image.LANCZOS)
                    f = f.convert(target_mode)
                    frames.append(f)
                
                # オリジナルの巨大な img オブジェクトへの参照を早めに解放
                del img
                
                _save_avif_robust(frames[0], buffer, save_kwargs, is_animated=True, frames=frames)
            else:
                # EXIFを反映（回転など）
                img = ImageOps.exif_transpose(img)
                
                # フォルダ内の他画像とサイズを合わせる
                if target_size:
                    img = img.resize(target_size, Image.LANCZOS)

                if do_gs:
                    if img.mode == 'RGBA' or 'transparency' in img.info:
                        img = img.convert("LA")
                    else:
                        img = img.convert("L")
                
                _save_avif_robust(img, buffer, save_kwargs)

            new_size = buffer.tell()
            
            # 容量が減った場合、またはサイズ統一(align)指定がある場合は置き換える
            if new_size < original_size or target_size is not None:
                new_path = file_path.with_suffix('.avif')
                
                # 同名ファイル(既に.avif)の場合は一時ファイルを経由する
                is_same_file = (new_path.resolve() == file_path.resolve())
                temp_path = file_path.with_name(file_path.stem + "_temp_avif.tmp") if is_same_file else new_path

                with open(temp_path, "wb") as f:
                    f.write(buffer.getvalue())
                
                # ExifToolでメタデータコピー (絶対パスを使用)
                exiftool_path = CONFIG.get('EXIFTOOL_PATH', '')
                if worker_exiftool:
                    worker_exiftool.execute(
                        "-TagsFromFile", str(file_path.resolve()),
                        "-all:all",
                        "-overwrite_original",
                        str(temp_path.resolve())
                    )

                # 同名ファイルだった場合は、一時ファイルで元ファイルを上書き
                if is_same_file:
                    temp_path.replace(new_path)

                return True, (original_size - new_size), new_path, file_path
            else:
                return False, 0, None, file_path

    except (UnidentifiedImageError, SyntaxError, OSError) as e:
        # 画像が壊れている、または拡張子と中身が一致しない場合（PillowはJPEG不正でSyntaxErrorを出すことがある）
        print(f"\n⚠️  読み込み失敗（スキップ）: {file_path.name} ({e})")
        if temp_path and temp_path.exists():
            try: os.remove(temp_path)
            except: pass
        return False, 0, None, file_path
    except MemoryError:
        print(f"\n⚠️  メモリ不足によりスキップ: {file_path.name}")
        if temp_path and temp_path.exists():
            try: os.remove(temp_path)
            except: pass
        return False, 0, None, file_path
    except Exception as e:
        # その他の予期せぬシステムエラーなどはトレースを表示
        print(f"\n❌ 予期せぬエラー ({file_path.name}): {e}")
        traceback.print_exc()
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

def process_images_in_folder(folder_path, executor, args, db_cache=None, image_list=None, exclude_file_keywords=None):
    if image_list is not None:
        images = image_list
    else:
        _, images, _ = get_folder_contents(folder_path)
    
    folder_name = folder_path.name
    # 拡張子フィルタリング
    candidates = []
    for img in images:
        if img.suffix.lower() not in CONFIG['TARGET_EXTS']:
            continue
        
        # 除外ファイルキーワードのチェック
        if exclude_file_keywords and any(kw in img.name for kw in exclude_file_keywords):
            continue
            
        # DBキャッシュを利用した容量フィルタリングの高速化
        img_abs = str(img.resolve())
        if db_cache and img_abs in db_cache:
            info = db_cache[img_abs]
            size_mb = info['filesize'] / (1024 * 1024)
            if size_mb < args.min_size:
                continue
            if args.max_size is not None and size_mb > args.max_size:
                continue
                
        candidates.append(img)
    
    if not candidates:
        return

    # フォルダ内の最大サイズを算出 (プレ走査)
    target_size = None
    if getattr(args, 'align', False):
        max_w, max_h = 0, 0
        for img_path in candidates:
            try:
                with Image.open(img_path) as tmp:
                    # 正確なサイズ取得のためEXIF考慮
                    tmp = ImageOps.exif_transpose(tmp)
                    w, h = tmp.size
                    if w > max_w: max_w = w
                    if h > max_h: max_h = h
            except: continue
        if max_w > 0 and max_h > 0:
            target_size = (max_w, max_h)

    folder_saved_bytes = 0
    folder_replaced_count = 0
    to_delete = []
    
    futures = {
        executor.submit(
            process_single_image, img, args.grayscale, args.min_size, args.max_size, target_size
        ): img 
        for img in candidates
    }
    
    desc = f" 🖼️  Optimizing: {folder_name[:30]}"
    
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

def process_directory(current_path, executor, args, pbar_global, db_cache=None, opt_mtimes=None, exclude_dir_names=None, exclude_file_keywords=None):
    if not current_path.exists():
        return

    # 処理中のディレクトリをプログレスバーに表示
    if pbar_global is not None:
        pbar_global.set_description(f"📂 Processing: {current_path.name[:30]}")

    # フォルダの現在の更新日時を取得
    try:
        current_mtime = os.path.getmtime(current_path)
    except OSError:
        return

    current_path_str = str(current_path)
    # 除外ディレクトリのチェック (名前一致 または 絶対パス一致)
    if exclude_dir_names and (current_path.name in exclude_dir_names or current_path_str in exclude_dir_names):
        print(f"⏭️ 除外ディレクトリ: {current_path}")
        return

    # 変更がない場合はスキップ（ただし初回や強制実行時は除く）
    cached_mtime = (opt_mtimes or {}).get(str(current_path))
    needs_processing = getattr(args, 'force', False) or (cached_mtime != current_mtime)

    folders, images, archives = get_folder_contents(current_path)

    if needs_processing:
        # フォルダ構造の整理
        while True:
            if not flatten_directory(current_path): break

        # 圧縮ファイルの展開
        for arc in archives:
            if arc.exists():
                handle_archive(arc, executor, args, pbar_global)

    # 子フォルダは常に再帰的にチェック（その中身が変更されている可能性があるため）
    folders_refreshed, _, _ = get_folder_contents(current_path)
    for folder in folders_refreshed:
        process_directory(folder, executor, args, pbar_global, db_cache=db_cache, opt_mtimes=opt_mtimes, exclude_dir_names=exclude_dir_names, exclude_file_keywords=exclude_file_keywords)
            
    if needs_processing:
        # 画像の最適化（ここが抜けていました）
        process_images_in_folder(current_path, executor, args, db_cache=db_cache, exclude_file_keywords=exclude_file_keywords)

        if args.zip:
            pack_to_zip(current_path)
        
        # 処理が終わった時点のMTimeを記録
        try:
            final_mtime = os.path.getmtime(current_path)
            update_optimizer_mtime(current_path, final_mtime)
        except OSError: pass

    if pbar_global is not None:
        pbar_global.update(1)

# ---------------------------------------------------------
# メイン
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Image & Archive Optimizer Script V4.2")
    parser.add_argument("root_dir", type=str, help="Target root directory path")
    parser.add_argument("--zip", action="store_true", help="Pack folders into uncompressed zip after processing")
    parser.add_argument("-g", "--grayscale", type=int, nargs="?", const=-1, help="Convert to grayscale. If a number is specified (e.g., -g 16), only images with color counts below that threshold are converted.")
    parser.add_argument("--align", action="store_true", help="Unify image dimensions to the maximum found in each folder")
    parser.add_argument("--workers", type=int, default=CONFIG['WORKER_COUNT'], help="Number of worker processes")
    parser.add_argument("--min-size", type=float, default=0, help="Minimum file size in MB to process")
    parser.add_argument("--max-size", type=float, default=None, help="Maximum file size in MB to process")
    parser.add_argument("--ext", nargs="+", help="Target extensions to process (e.g., --ext jpg png)")
    parser.add_argument("-f", "--force", action="store_true", help="Force process even if the directory hasn't changed")
    parser.add_argument("--db-only", action="store_true", help="Only process images already registered in the database")

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
    
    config = load_config() # config.json を読み込む
    exclude_dir_names = set()
    for d in config.get("EXCLUDE_DIR_NAMES", []):
        exclude_dir_names.add(d)
        try:
            exclude_dir_names.add(str(Path(d).resolve()))
        except: pass

    exclude_file_keywords = config.get("EXCLUDE_FILE_KEYWORDS", [])

    if not target_root.exists():
        print("❌ 指定されたフォルダが存在しません。")
        return

    print(f"🚀 Processing Start: {target_root}")
    print(f"⚙️  設定: 画質={CONFIG['AVIF_QUALITY']}, 速度={CONFIG['AVIF_SPEED']}")
    mode_info = []
    if args.zip: mode_info.append("Zip:ON")
    if args.grayscale: mode_info.append("Grayscale:ON")
    
    db_cache = get_db_cache()
    opt_mtimes = get_optimizer_mtimes()
    images_by_folder = None

    if args.db_only:
        print("🗄️  データベースから対象画像を抽出中 (DB-ONLY モード)...")
        images_by_folder = defaultdict(list)
        target_prefix = str(target_root.resolve()) + os.sep
        for path_str, info in db_cache.items():
            if path_str.startswith(target_prefix):
                p = Path(path_str)
                images_by_folder[p.parent].append(p)
        total_folders = len(images_by_folder)
    else:
        total_folders = None # プレ走査をスキップ

    try:
        with tqdm(total=total_folders, unit="dir", position=0, leave=True) as pbar:
            # initializerを使用して各ワーカーでExifToolを起動
            with ProcessPoolExecutor(
                max_workers=CONFIG['WORKER_COUNT'],
                initializer=init_worker,
                initargs=(CONFIG['EXIFTOOL_PATH'],)
            ) as executor:
                if args.db_only:
                    # DBにあるパスのみを対象にする (実ファイルスキャンをスキップ)
                    # 注意: このモードではアーカイブ展開やディレクトリの平坦化は行われません
                    for folder_path, image_list in images_by_folder.items():
                        pbar.set_description(f"📂 Processing: {folder_path.name[:30]}")
                        process_images_in_folder(folder_path, executor, args, db_cache=db_cache, image_list=image_list)
                        if args.zip:
                            # ZIP化する場合は実際のフォルダ内容を確認する必要がある
                            pack_to_zip(folder_path)
                        pbar.update(1) # DB-ONLYモードでもプログレスバーを更新
                else:
                    process_directory(target_root, executor, args, pbar, db_cache=db_cache, opt_mtimes=opt_mtimes, exclude_dir_names=exclude_dir_names, exclude_file_keywords=exclude_file_keywords)
            
    except KeyboardInterrupt:
        print("\n⚠️  処理が中断されました。")
    except Exception as e:
        print(f"\n❌ 予期せぬエラー: {e}")
        import traceback
        traceback.print_exc()
    
    global_stats.print_summary()

if __name__ == "__main__":
    main()