import os
import argparse
import sqlite3
import sys
import multiprocessing
import gc
import warnings
import math
import datetime
from collections import defaultdict
from pathlib import Path
from PIL import Image, ImageOps, ImageStat, ImageFile
import imagehash
from send2trash import send2trash
import pillow_avif
import matplotlib.pyplot as plt
import concurrent.futures
import pybktree

# 破損した画像を開こうとしてクラッシュするのを防ぐ
ImageFile.LOAD_TRUNCATED_IMAGES = True
# Pillowの巨大画像に対する警告を黙らせる
warnings.simplefilter('ignore', Image.DecompressionBombWarning)

# 日本語フォントの設定
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Meiryo', 'Yu Gothic', 'MS Gothic']

# ==========================================
# 📂 対象・除外の設定（ここを編集してください）
# ==========================================
# 処理対象のフォルダパス
TARGET_DIRS = [
    r"E:\gaz 画像",
    r"E:\dow Download\sns_media_download\SNS画像",
]

# 除外するフォルダ名
EXCLUDE_DIR_NAMES = [
    "お気に入り",
    "除外フォルダ",
    "絶対消さない"
]

# 除外するファイル名のキーワード
EXCLUDE_FILE_KEYWORDS = [
    "_keep",
    "保護"
]

SUPPORTED_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.avif'}
ASPECT_TOLERANCE = 0.1 
SOLID_TOLERANCE = 2.0  
DB_FILENAME = ".image_hash_cache.db"

def get_format_priority(path):
    suffix = Path(path).suffix.lower()
    priorities = {
        '.avif': 100, '.png': 90, '.bmp': 89, '.jpg': 80, '.jpeg': 80, 
        '.webp': 70, '.gif': 60, 
    }
    return priorities.get(suffix, 0)

def get_sort_key(info):
    is_protected = 1 if "_protect" in Path(info['path']).name else 0
    return (is_protected, info['pixels'], get_format_priority(info['path']), info['filesize'])

def hex_hamming_distance(hex_str1, hex_str2):
    try:
        if len(hex_str1) != len(hex_str2):
            return 999
        return bin(int(hex_str1, 16) ^ int(hex_str2, 16)).count('1')
    except ValueError:
        return 999

def format_pixels(pixels):
    if pixels >= 10000:
        val = pixels / 10000
        if val.is_integer():
            return f"{int(val)}万"
        else:
            return f"{val:.1f}万"
    return str(pixels)

def format_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024.0:
            if unit == 'B':
                return f"{size_in_bytes} {unit}"
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} TB"

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS images (
            path TEXT PRIMARY KEY,
            hash_str TEXT,
            color_hash_str TEXT,
            pixels INTEGER,
            filesize INTEGER,
            aspect_ratio REAL,
            mtime REAL,
            checked INTEGER DEFAULT 0
        )
    ''')
    
    try:
        c.execute("ALTER TABLE images ADD COLUMN checked INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass

    c.execute('''
        CREATE TABLE IF NOT EXISTS similarity_edges (
            path1 TEXT,
            path2 TEXT,
            PRIMARY KEY(path1, path2)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_exact ON images(hash_str, color_hash_str, pixels, filesize)')
    conn.commit()
    return conn

def delete_db_records(c, paths):
    for i in range(0, len(paths), 1000):
        chunk = paths[i:i+1000]
        c.executemany('DELETE FROM images WHERE path = ?', [(p,) for p in chunk])
        c.executemany('DELETE FROM similarity_edges WHERE path1 = ? OR path2 = ?', [(p, p) for p in chunk])

def compute_image_info(path_str):
    path = Path(path_str)
    try:
        filesize = os.path.getsize(path)
        if filesize > 30 * 1024 * 1024:
            return path_str, None

        with Image.open(path) as img:
            orig_w, orig_h = img.size
            if orig_w * orig_h > 40000000:
                return path_str, None

            exif = img.getexif()
            if exif and exif.get(0x0112) in [5, 6, 7, 8]:
                orig_w, orig_h = orig_h, orig_w

            aspect_ratio = orig_w / orig_h if orig_h > 0 else 0

            img.draft('RGB', (256, 256))
            img = ImageOps.exif_transpose(img)
            img.thumbnail((256, 256))
            
            stat = ImageStat.Stat(img)
            if max(stat.stddev) < SOLID_TOLERANCE:
                return path_str, None 

            h = imagehash.phash(img)
            c = imagehash.colorhash(img) 
            
            return path_str, (
                path_str, str(h), str(c), orig_w * orig_h, filesize, aspect_ratio, os.path.getmtime(path)
            )
    except Exception:
        return path_str, None

def show_preview(group_infos, group_idx, total_groups, fig=None):
    total_images = len(group_infos)
    cols = min(total_images, 3)
    rows = math.ceil(total_images / cols)
    
    if fig is None or not plt.fignum_exists(fig.number):
        fig = plt.figure(figsize=(5.5 * cols, 5 * rows))
        fig.canvas.manager.set_window_title("類似画像チェッカー")
        try:
            mng = plt.get_current_fig_manager()
            if hasattr(mng.window, 'state'):
                mng.window.state('zoomed')
            elif hasattr(mng, 'full_screen_toggle'):
                mng.full_screen_toggle() 
        except Exception:
            pass 
    else:
        fig.clf()

    fig.canvas.manager.set_window_title(f"類似グループ {group_idx} / {total_groups} - 残す画像を選んでください")
    max_pixels = max((info['pixels'] for info in group_infos), default=1)
    axes = fig.subplots(rows, cols)
    
    if hasattr(axes, 'flatten'):
        axes_flat = axes.flatten()
    elif isinstance(axes, list) or isinstance(axes, tuple):
        axes_flat = axes if type(axes) is list else list(axes)
    else:
        axes_flat = [axes]

    ext_colors = {
        '.avif': '#e8f5e9', '.webp': '#f3e5f5', '.png':  '#e3f2fd',
        '.jpg':  '#fff3e0', '.jpeg': '#fff3e0', '.gif':  '#ffebee',
    }

    error_count = 0

    for i, info in enumerate(group_infos):
        display_idx = i + 1 
        ext = Path(info['path']).suffix.lower()
        bg_color = ext_colors.get(ext, '#ffffff')
        ax = axes_flat[i]

        formatted_pixels = format_pixels(info['pixels'])
        formatted_size = format_size(info['filesize'])
        ratio = info['pixels'] / max_pixels
        
        if "_protect" in Path(info['path']).name:
            res_color, res_weight, res_mark = "blue", "bold", "🛡️ 保護済"
        elif info['pixels'] == max_pixels:
            res_color, res_weight, res_mark = "darkred", "bold", "★ 最高画質"
        elif ratio >= 0.8:
            res_color, res_weight, res_mark = "forestgreen", "normal", "○ 近い画質"
        else:
            res_color, res_weight, res_mark = "dimgray", "normal", "△ 低画質"

        title_text = f"【 {display_idx} 】 {ext.upper()}\n{res_mark} ({formatted_pixels} px)\n{formatted_size}"

        try:
            with Image.open(info['path']) as img:
                img = ImageOps.exif_transpose(img)
                img.thumbnail((1024, 1024))
                ax.imshow(img)
            
            ax.set_title(
                title_text, color=res_color, fontweight=res_weight, fontsize=11, pad=10,
                bbox=dict(facecolor=bg_color, edgecolor='gray', boxstyle='round,pad=0.5', alpha=0.9)
            )
        except Exception:
            error_count += 1
            error_title = f"【 {display_idx} 】 {ext.upper()}\n⚠️ Load Error ({formatted_pixels} px)\n{formatted_size}"
            ax.set_title(
                error_title, color="red", fontweight="bold", fontsize=11, pad=10,
                bbox=dict(facecolor='#ffebee', edgecolor='red', boxstyle='round,pad=0.5', alpha=0.9)
            )
        ax.axis('off')
        
    for j in range(total_images, len(axes_flat)):
        axes_flat[j].axis('off')
        
    try:
        fig.tight_layout(w_pad=3.5, h_pad=3.5)
    except Exception:
        pass
    
    fig.subplots_adjust(left=0.05, right=0.95, bottom=max(0.15, 0.3 / rows), top=0.92)

    fig.canvas.draw()
    plt.show(block=False)
    plt.pause(0.01)
    
    all_failed = (error_count == total_images)
    return fig, all_failed

def bktree_distance(hash1, hash2):
    return hash1 - hash2

def process_trash_generic(files_to_trash_paths, args):
    count = 0
    for f_path in files_to_trash_paths:
        try:
            if not args.dry_run:
                send2trash(f_path)
            count += 1
        except Exception as e:
            print(f"\n⚠️ エラー: {Path(f_path).name} をごみ箱に移動できませんでした ({e})")
    return count

def main():
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="【進捗表示・完全版】類似画像を処理するスクリプト")
    parser.add_argument("directories", type=str, nargs='*', help="対象フォルダのパス")
    parser.add_argument("-l", "--level", type=int, choices=range(1, 17), default=1, help="構図の類似判定レベル")
    parser.add_argument("-c", "--color-level", type=int, default=10, help="色味の類似判定レベル")
    parser.add_argument("-e", "--exclude", type=str, default="", help="除外する追加のフォルダ名（カンマ区切り）")
    parser.add_argument("-a", "--auto", action="store_true", help="プレビューをスキップし、自動で一番良い画像を1枚残す")
    parser.add_argument("-d", "--dry-run", action="store_true", help="テストモード。実際には削除を行わない")
    parser.add_argument("-f", "--force-update", action="store_true", help="本日スキャン済みでも強制的にフォルダを再スキャンしてDBを更新する")
    
    args = parser.parse_args()
    
    if args.directories:
        target_dirs_paths = [Path(d).resolve() for d in args.directories]
    else:
        target_dirs_paths = [Path(d).resolve() for d in TARGET_DIRS if d.strip()]

    if not target_dirs_paths:
        print("❌ エラー: 対象フォルダが指定されていません。")
        return

    threshold = args.level - 1
    color_threshold = args.color_level
    
    cmd_exclude = [x.strip() for x in args.exclude.split(',')] if args.exclude else []
    full_exclude_dirs = set(EXCLUDE_DIR_NAMES + cmd_exclude)
    
    db_path = script_dir / DB_FILENAME
    conn = init_db(db_path)
    c = conn.cursor()

    c.execute("SELECT value FROM metadata WHERE key = 'last_level'")
    row_l = c.fetchone()
    c.execute("SELECT value FROM metadata WHERE key = 'last_color_level'")
    row_c = c.fetchone()

    last_l = int(row_l[0]) if row_l else -1
    last_c = int(row_c[0]) if row_c else -1

    if (last_l != -1 and last_l != args.level) or (last_c != -1 and last_c != args.color_level):
        print("⚠️ 判定レベル（-l または -c）が変更されたため、類似ペアのキャッシュをリセットします。")
        c.execute("DELETE FROM similarity_edges")
        c.execute("UPDATE images SET checked = 0")
        conn.commit()

    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_level', ?)", (str(args.level),))
    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_color_level', ?)", (str(args.color_level),))
    conn.commit()

    c.execute("SELECT value FROM metadata WHERE key = 'last_scan_date'")
    row = c.fetchone()
    last_scan_date = row[0] if row else ""
    today_str = datetime.date.today().isoformat()

    needs_scan = (last_scan_date != today_str) or args.force_update
    image_infos = []

    if needs_scan:
        print(f"🔍 以下のフォルダから画像ファイルを検索中...")
        target_files = []
        skipped_files_count = 0
        
        for target_dir in target_dirs_paths:
            if not target_dir.is_dir():
                continue
            print(f"  📂 {target_dir}")
            for p in target_dir.rglob('*'):
                if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
                    if any(ex in p.parts for ex in full_exclude_dirs):
                        skipped_files_count += 1
                        continue
                    if any(keyword in p.name for keyword in EXCLUDE_FILE_KEYWORDS):
                        skipped_files_count += 1
                        continue
                    if not os.access(p, os.W_OK):
                        skipped_files_count += 1
                        continue

                    target_files.append(p)
        
        if skipped_files_count > 0:
            print(f"  🛡️ 保護設定により {skipped_files_count} 個のファイルが処理対象から除外されました。")
            
        print(f"🗄️ データベースと実際のファイル状況を同期しています... (対象: {len(target_files)}件)")
        c.execute('SELECT path, mtime, filesize, hash_str, color_hash_str, pixels, aspect_ratio FROM images')
        db_cache = {row[0]: row for row in c.fetchall()}

        to_compute = []
        target_files_str_set = set()
        total_targets = len(target_files)

        for i, p in enumerate(target_files):
            if i % 100 == 0 or i == total_targets - 1:
                print(f"\r  🔄 同期チェック中... {i + 1} / {total_targets} 件", end="", flush=True)

            p_str = str(p)
            target_files_str_set.add(p_str)
            current_mtime = os.path.getmtime(p)
            current_filesize = os.path.getsize(p)
            
            if p_str in db_cache:
                row = db_cache[p_str]
                if row[1] == current_mtime and row[2] == current_filesize:
                    image_infos.append({
                        'path': p_str, 'hash_str': row[3], 'hash': imagehash.hex_to_hash(row[3]),
                        'color_hash_str': row[4], 'pixels': row[5], 'filesize': current_filesize, 'aspect_ratio': row[6]
                    })
                    continue
            to_compute.append(p_str)
        
        print() 

        db_paths_to_delete = []
        total_db_cache = len(db_cache)
        
        for i, p_str in enumerate(db_cache):
            if i % 1000 == 0 or i == total_db_cache - 1:
                print(f"\r  🧹 不要データの確認中... {i + 1} / {total_db_cache} 件", end="", flush=True)
                
            if p_str not in target_files_str_set:
                db_paths_to_delete.append(p_str)
                
        print() 
        
        if db_paths_to_delete:
            print(f"  🗑️ 削除済み・除外済みのデータ {len(db_paths_to_delete)} 件をデータベースから削除します...")
            delete_db_records(c, db_paths_to_delete)
            conn.commit()

        if to_compute:
            print(f"🚀 {len(to_compute)} 個の新規/更新ファイルを検出。解析してデータベースに保存します...")
            processed_count = 0
            batch_data = []

            try:
                safe_workers = min(4, max(1, multiprocessing.cpu_count() // 2))
                with concurrent.futures.ProcessPoolExecutor(max_workers=safe_workers) as executor:
                    for path_str, res in executor.map(compute_image_info, to_compute, chunksize=32):
                        processed_count += 1
                        if res:
                            batch_data.append((*res, 0))
                            image_infos.append({
                                'path': path_str, 'hash_str': res[1], 'hash': imagehash.hex_to_hash(res[1]),
                                'color_hash_str': res[2], 'pixels': res[3], 'filesize': res[4], 'aspect_ratio': res[5]
                            })
                        else:
                            delete_db_records(c, [path_str])

                        if len(batch_data) >= 500:
                            c.executemany('INSERT OR REPLACE INTO images (path, hash_str, color_hash_str, pixels, filesize, aspect_ratio, mtime, checked) VALUES (?,?,?,?,?,?,?,?)', batch_data)
                            conn.commit()
                            batch_data = []
                        print(f"\r  💾 {processed_count}件処理完了... データベースに記録しました。", end="", flush=True)
                
                if batch_data:
                    c.executemany('INSERT OR REPLACE INTO images (path, hash_str, color_hash_str, pixels, filesize, aspect_ratio, mtime, checked) VALUES (?,?,?,?,?,?,?,?)', batch_data)
                    conn.commit()
                print() 
            except KeyboardInterrupt:
                print("\n🛑 処理が中断されました。計算済みのデータは既に安全にDBに保存されています。")
                if batch_data:
                    c.executemany('INSERT OR REPLACE INTO images (path, hash_str, color_hash_str, pixels, filesize, aspect_ratio, mtime, checked) VALUES (?,?,?,?,?,?,?,?)', batch_data)
                    conn.commit()
                sys.exit(0)
                
        c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_scan_date', ?)", (today_str,))
        conn.commit()

    else:
        print("⏭️ 本日のデータベース更新は完了しているため、ファイルスキャンをスキップします。")
        print("   (新しく追加した画像をすぐ読み込ませたい場合は -f を付けて実行してください)")
        
        c.execute('SELECT path, hash_str, color_hash_str, pixels, filesize, aspect_ratio FROM images')
        db_rows = c.fetchall()
        
        print(f"🗄️ データベースから {len(db_rows)} 件の画像情報を読み込み中...")
        deleted_paths = []
        for row in db_rows:
            p_str = row[0]
            if os.path.exists(p_str):
                image_infos.append({
                    'path': p_str, 'hash_str': row[1], 'hash': imagehash.hex_to_hash(row[1]),
                    'color_hash_str': row[2], 'pixels': row[3], 'filesize': row[4], 'aspect_ratio': row[5]
                })
            else:
                deleted_paths.append(p_str)
                
        if deleted_paths:
            delete_db_records(c, deleted_paths)
            conn.commit()
            print(f"  🗑️ 外部で削除済みのファイル {len(deleted_paths)} 件をDBからクリアしました。")

    total_trashed = 0
    if args.dry_run:
        print("\n🧪 【ドライランモード】ファイルは実際には移動されません（テストモード）。")

    if len(image_infos) >= 2:
        print(f"\n✨ 【STEP 1】完全に同一（または特徴が完全一致する低解像度）の画像を検索し、自動で整理します...")
        exact_groups = defaultdict(list)
        for info in image_infos:
            exact_key = f"{info['hash_str']}_{info['color_hash_str']}"
            exact_groups[exact_key].append(info)

        filtered_image_infos = []
        exact_trashed_count = 0
        all_exact_trash_paths = []

        for exact_key, group in exact_groups.items():
            if len(group) > 1:
                group.sort(key=get_sort_key, reverse=True)
                max_pixels = group[0]['pixels']
                
                keep_for_step2 = [group[0]]
                trash_paths = []
                
                for info in group[1:]:
                    is_protected = "_protect" in Path(info['path']).name
                    is_exact_dup = (info['pixels'] == group[0]['pixels'] and info['filesize'] == group[0]['filesize'])
                    
                    if is_protected:
                        keep_for_step2.append(info)
                    elif is_exact_dup:
                        trash_paths.append(info['path'])
                        print(f"  🗑️ [自動削除: 完全一致コピー] {Path(info['path']).name}")
                    elif info['pixels'] <= max_pixels * 0.5:
                        trash_paths.append(info['path'])
                        print(f"  🗑️ [自動削除: 特徴完全一致＆低解像度] {Path(info['path']).name}")
                    else:
                        keep_for_step2.append(info)
                
                filtered_image_infos.extend(keep_for_step2)
                
                if trash_paths:
                    moved = process_trash_generic(trash_paths, args)
                    exact_trashed_count += moved
                    total_trashed += moved
                    all_exact_trash_paths.extend(trash_paths)
            else:
                filtered_image_infos.append(group[0])

        if all_exact_trash_paths:
            delete_db_records(c, all_exact_trash_paths)
            conn.commit()

        if exact_trashed_count > 0:
            print(f"  ✅ {exact_trashed_count} 枚の完全一致・低解像度画像を自動でごみ箱に送りました。")
        else:
            print("  ✅ 該当する画像は見つかりませんでした。")

        image_infos = filtered_image_infos
        num_images = len(image_infos)

        if num_images >= 2:
            image_infos.sort(key=get_sort_key, reverse=True)
            path_to_index = {info['path']: i for i, info in enumerate(image_infos)}

            c.execute("SELECT path FROM images WHERE checked = 0")
            unchecked_paths = set(row[0] for row in c.fetchall())
            unchecked_indices = [i for i, info in enumerate(image_infos) if info['path'] in unchecked_paths]

            adj = defaultdict(set)
            
            c.execute("SELECT path1, path2 FROM similarity_edges")
            for p1, p2 in c.fetchall():
                if p1 in path_to_index and p2 in path_to_index:
                    i, j = path_to_index[p1], path_to_index[p2]
                    adj[i].add(j)
                    adj[j].add(i)

            if unchecked_indices:
                print(f"\n🌲 【STEP 2】新しく追加された未検証の {len(unchecked_indices)} 枚を中心に類似判定を計算します...")
                tree = pybktree.BKTree(bktree_distance)
                hash_to_indices = defaultdict(list)
                
                for i, info in enumerate(image_infos):
                    h_str = info['hash_str']
                    if not hash_to_indices[h_str]: 
                        tree.add(info['hash'])
                    hash_to_indices[h_str].append(i)

                new_edges = []
                for count, i in enumerate(unchecked_indices):
                    if count > 0 and count % 100 == 0:
                        print(f"\r  🔍 {count} / {len(unchecked_indices)} 枚を比較中...   ", end="", flush=True)
                        
                    info = image_infos[i]
                    results = tree.find(info['hash'], threshold)
                    for dist, matched_hash in results:
                        for j in hash_to_indices[str(matched_hash)]:
                            if i != j:
                                color_dist = hex_hamming_distance(info['color_hash_str'], image_infos[j]['color_hash_str'])
                                if abs(info['aspect_ratio'] - image_infos[j]['aspect_ratio']) <= ASPECT_TOLERANCE and color_dist <= color_threshold:
                                    adj[i].add(j)
                                    adj[j].add(i)
                                    p1, p2 = info['path'], image_infos[j]['path']
                                    if p1 > p2: p1, p2 = p2, p1
                                    new_edges.append((p1, p2))
                print() 

                del tree
                del hash_to_indices
                gc.collect()

                if new_edges:
                    new_edges = list(set(new_edges))
                    c.executemany("INSERT OR IGNORE INTO similarity_edges VALUES (?, ?)", new_edges)
                
                c.executemany("UPDATE images SET checked = 1 WHERE path = ?", [(image_infos[i]['path'],) for i in unchecked_indices])
                conn.commit()
            else:
                print(f"\n🌲 【STEP 2】すべての画像の類似計算は完了済みです。キャッシュから瞬時にグループを読み込みます！⚡")

            visited = set()
            groups = []
            
            for i in range(num_images):
                if i not in visited and i in adj:
                    group_indices = []
                    queue = [i]
                    visited.add(i)
                    while queue:
                        node = queue.pop(0)
                        group_indices.append(node)
                        for neighbor in adj[node]:
                            if neighbor not in visited:
                                visited.add(neighbor)
                                queue.append(neighbor)
                                
                    if len(group_indices) > 1:
                        group_indices.sort(key=lambda idx: get_sort_key(image_infos[idx]), reverse=True)
                        groups.append(group_indices)

            total_groups = len(groups)
            print(f"\n合計 {total_groups} 個の類似グループが見つかりました。\n")
            
            main_fig = None 
            cid_key = None
            cid_close = None

            idx = 0
            history_stack = []
            trash_actions = {}
            protect_actions = {}
            at_actions = {}
            
            last_action_msg = ""

            while idx < total_groups:
                group_indices = groups[idx]
                
                if idx > 0 and idx % 256 == 0:
                    if main_fig is not None:
                        plt.close(main_fig)
                        main_fig = None
                        gc.collect()

                group_infos = [image_infos[j] for j in group_indices]
                
                if all("_protect" in Path(info['path']).name for info in group_infos):
                    trash_actions[idx] = []
                    protect_actions[idx] = []
                    at_actions[idx] = []
                    last_action_msg = f"📁 Group_{idx + 1}: 全て保護済みのためスキップしました"
                    idx += 1
                    continue

                group_id = f"Group_{idx + 1}"
                max_pixels = group_infos[0]['pixels']
                filtered_group_infos = []
                auto_trash_paths = []

                for info in group_infos:
                    if info['pixels'] <= max_pixels * 0.5 and info is not group_infos[0]:
                        if "_protect" in Path(info['path']).name:
                            filtered_group_infos.append(info)
                        else:
                            auto_trash_paths.append(info['path'])
                            print(f"  🗑️ [自動削除予定: 類似判定による低解像度] {Path(info['path']).name}")
                    else:
                        filtered_group_infos.append(info)
                
                if len(filtered_group_infos) == 1:
                    print(f"--- 📁 {group_id}/{total_groups} (Auto Skip: 他は低解像度のため自動処理) ---")
                    trash_actions[idx] = auto_trash_paths
                    protect_actions[idx] = []
                    at_actions[idx] = []
                    last_action_msg = f"📁 Group_{idx + 1}: 他は低解像度のため自動で1枚残しました"
                    idx += 1
                    continue
                
                if args.auto:
                    print(f"--- 📁 {group_id}/{total_groups} (Auto) ---")
                    current_trash = auto_trash_paths + [info['path'] for info in filtered_group_infos[1:]]
                    trash_actions[idx] = current_trash
                    protect_actions[idx] = []
                    at_actions[idx] = []
                    last_action_msg = f"📁 Group_{idx + 1}: Autoモードで1枚残しました"
                    idx += 1
                    continue

                main_fig, all_failed = show_preview(filtered_group_infos, idx + 1, total_groups, fig=main_fig)
                print(f"--- 📁 {group_id}/{total_groups} ---")
                
                for i, info in enumerate(filtered_group_infos):
                    print(f"  [{i+1}] {Path(info['path']).name}")
                
                if all_failed:
                    print("  ⚠️ 【スキップ】すべての画像のプレビュー読み込みに失敗しました。自動ですべて残します。")
                    ans = "a"
                else:
                    prompt_text = "【直接入力】 番号[@] / p 番号[@] / a[@]:残す / d:削除 / b:戻る / q:終了 / Enter\n入力 > "
                    
                    if last_action_msg:
                        display_prompt = f"【直前の結果】 {last_action_msg}\n-------------------------------------------------\n{prompt_text}"
                    else:
                        display_prompt = prompt_text
                    
                    text_obj = main_fig.text(0.5, 0.05, display_prompt, ha='center', va='bottom', fontsize=13,
                                        bbox=dict(facecolor='#ffffea', alpha=0.95, edgecolor='black', boxstyle='round,pad=0.5'))
                    main_fig.canvas.draw()

                    input_data = {"text": "", "done": False}

                    def on_key(event):
                        if event.key == 'enter':
                            input_data["done"] = True
                            main_fig.canvas.stop_event_loop()
                        elif event.key == 'backspace':
                            input_data["text"] = input_data["text"][:-1]
                            text_obj.set_text(display_prompt + input_data["text"])
                            main_fig.canvas.draw()
                        elif event.key is not None and len(event.key) == 1:
                            input_data["text"] += event.key
                            text_obj.set_text(display_prompt + input_data["text"])
                            main_fig.canvas.draw()

                    def on_close(event):
                        if not input_data["done"]:
                            input_data["text"] = "q"
                            input_data["done"] = True
                            try:
                                main_fig.canvas.stop_event_loop()
                            except Exception:
                                pass

                    if cid_key is not None:
                        main_fig.canvas.mpl_disconnect(cid_key)
                    if cid_close is not None:
                        main_fig.canvas.mpl_disconnect(cid_close)

                    cid_key = main_fig.canvas.mpl_connect('key_press_event', on_key)
                    cid_close = main_fig.canvas.mpl_connect('close_event', on_close)

                    main_fig.canvas.start_event_loop()
                    ans = input_data["text"].strip()
                    text_obj.remove()

                ans_str = ans.lower().strip()
                current_protect = []
                current_at = []
                is_protect = False
                is_all_at = False

                if ans_str.startswith('p'):
                    is_protect = True
                    ans_str = ans_str[1:].strip()
                    if not ans_str:
                        ans_str = '1'

                if ans_str == 'a@':
                    ans_str = 'a'
                    is_all_at = True

                if ans_str == 'q':
                    print("🛑 処理を中断します。これまでの判定を反映して終了します。")
                    break
                
                if ans_str == 'b':
                    if not history_stack:
                        print("  ⚠️ これ以上戻れません（最初の操作グループです）。")
                        continue
                    
                    last_manual_idx = history_stack.pop()
                    print(f"  ⏪ グループ {last_manual_idx + 1} の判定をやり直します...")
                    
                    for i in range(last_manual_idx, idx + 1):
                        if i in trash_actions:
                            del trash_actions[i]
                        if i in protect_actions:
                            del protect_actions[i]
                        if i in at_actions:
                            del at_actions[i]
                            
                    last_action_msg = f"⏪ グループ {last_manual_idx + 1} まで戻りました"
                    idx = last_manual_idx
                    continue
                
                action_text = "保護して" if is_protect else ""
                at_indices = []
                
                if ans_str == 'a':
                    keep_indices = list(range(len(filtered_group_infos)))
                    if is_all_at:
                        at_indices = keep_indices
                        action_text += "マーク(@)して"
                    print(f"  -> すべて{action_text}残しました。")
                    last_action_msg = f"📁 Group_{idx + 1}: すべて{action_text}残しました"
                elif ans_str == '0' or ans_str == 'd':
                    keep_indices = []
                    print("  -> このグループの画像をすべて削除予定にしました。")
                    last_action_msg = f"📁 Group_{idx + 1}: すべて削除予定にしました"
                else:
                    try:
                        parts = ans_str.split()
                        keep_indices = []
                        at_indices_temp = []
                        
                        for part in parts:
                            has_at = '@' in part
                            num_str = part.replace('@', '')
                            if not num_str:
                                continue
                            n = int(num_str)
                            idx_val = n - 1
                            if 0 <= idx_val < len(filtered_group_infos):
                                if idx_val not in keep_indices:
                                    keep_indices.append(idx_val)
                                if has_at and idx_val not in at_indices_temp:
                                    at_indices_temp.append(idx_val)
                        
                        keep_indices = sorted(list(set(keep_indices)))
                        at_indices = sorted(list(set(at_indices_temp)))
                        
                        if not keep_indices:
                            keep_indices = [0]
                            at_indices = []
                            print(f"  -> 有効な入力がなかったため、一番良い画像を1枚{action_text}残しました。")
                            last_action_msg = f"📁 Group_{idx + 1}: 一番良い画像を1枚{action_text}残しました"
                        else:
                            kept_names = [Path(filtered_group_infos[i]['path']).name for i in keep_indices]
                            at_text = "一部マーク(@)して" if at_indices else ""
                            print(f"  -> 指定された画像を{action_text}{at_text}残しました: {', '.join(kept_names)}")
                            
                            kept_names_str = ', '.join(kept_names)
                            if len(kept_names_str) > 50:
                                kept_names_str = kept_names_str[:47] + "..."
                            last_action_msg = f"📁 Group_{idx + 1}: {kept_names_str} を{action_text}{at_text}残しました"
                            
                    except ValueError:
                        keep_indices = [0]
                        at_indices = []
                        print(f"  -> 入力エラーのため、一番良い画像を1枚{action_text}残しました。")
                        last_action_msg = f"📁 Group_{idx + 1}: 入力エラーのため一番良い画像を1枚{action_text}残しました"
                
                if is_protect:
                    for i in keep_indices:
                        current_protect.append(filtered_group_infos[i]['path'])
                        
                for i in at_indices:
                    current_at.append(filtered_group_infos[i]['path'])

                current_trash = auto_trash_paths.copy()
                for i, info in enumerate(filtered_group_infos):
                    if i not in keep_indices:
                        current_trash.append(info['path'])

                trash_actions[idx] = current_trash
                protect_actions[idx] = current_protect
                at_actions[idx] = current_at
                history_stack.append(idx)
                idx += 1


            if main_fig is not None:
                plt.close(main_fig)

            # --- リネーム処理（保護 および @） ---
            rename_tasks = defaultdict(lambda: {"protect": False, "at": False})
            
            for paths in protect_actions.values():
                for p in paths:
                    rename_tasks[p]["protect"] = True
                    
            for paths in at_actions.values():
                for p in paths:
                    rename_tasks[p]["at"] = True

            if rename_tasks:
                print(f"\n🏷️ 選択された画像の保護・マーク処理（リネーム）を行っています...")
                for p_str, flags in rename_tasks.items():
                    old_path = Path(p_str)
                    if old_path.exists():
                        new_stem = old_path.stem
                        
                        if flags["protect"] and "_protect" not in new_stem:
                            new_stem += "_protect"
                        if flags["at"] and not new_stem.endswith("@"):
                            new_stem += "@"
                            
                        if new_stem != old_path.stem:
                            new_name = f"{new_stem}{old_path.suffix}"
                            new_path = old_path.with_name(new_name)
                            if args.dry_run:
                                print(f"  [Dry-Run] リネーム予定: {old_path.name} -> {new_name}")
                            else:
                                try:
                                    old_path.rename(new_path)
                                    delete_db_records(c, [p_str])
                                except Exception as e:
                                    print(f"  ⚠️ リネーム失敗: {old_path.name} ({e})")
                conn.commit()

            # --- ゴミ箱処理 ---
            all_files_to_trash = []
            for paths in trash_actions.values():
                all_files_to_trash.extend(paths)
                
            if all_files_to_trash:
                print(f"\n🗑️ 最終処理: 選択された合計 {len(all_files_to_trash)} 枚の画像をゴミ箱に移動しています...")
                moved = process_trash_generic(all_files_to_trash, args)
                total_trashed += moved
                delete_db_records(c, all_files_to_trash)
                conn.commit()

    conn.close()
    print(f"\n🎉 すべての処理が完了しました！")
    print(f"   処理された不要画像: {total_trashed} 枚")

if __name__ == "__main__":
    main()