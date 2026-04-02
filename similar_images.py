import os
import argparse
import sqlite3
import sys
import multiprocessing
import gc
import warnings
import math
import datetime
import json
import logging
from collections import defaultdict
from pathlib import Path

from PIL import Image, ImageOps, ImageStat, ImageFile, ImageTk
import tkinter as tk
from tkinter import ttk
import imagehash
from send2trash import send2trash
import pillow_avif
import pybktree
import concurrent.futures

# ==========================================
# 初期設定・準備
# ==========================================
ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.simplefilter('ignore', Image.DecompressionBombWarning)

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_FILE = SCRIPT_DIR / "config.json"
DB_FILENAME = SCRIPT_DIR / ".image_hash_cache.db"

# ログ設定
LOG_FILENAME = SCRIPT_DIR / f"similar_images_log_{datetime.date.today().strftime('%Y%m%d')}.txt"
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# デフォルト設定（初回起動時に config.json として保存されます）
DEFAULT_CONFIG = {
    "TARGET_DIRS": [
        r"E:\gaz 画像",
        r"E:\dow Download\sns_media_download\SNS画像"
    ],
    "EXCLUDE_DIR_NAMES": ["お気に入り", "除外フォルダ", "絶対消さない"],
    "EXCLUDE_FILE_KEYWORDS": ["_keep", "保護"],
    "ASPECT_TOLERANCE": 0.1,
    "SOLID_TOLERANCE": 2.0
}

SUPPORTED_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.avif'}

# ==========================================
# ユーティリティ関数
# ==========================================
def load_config():
    if not CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
        return DEFAULT_CONFIG
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_format_priority(path):
    suffix = Path(path).suffix.lower()
    priorities = {'.avif': 100, '.png': 90, '.bmp': 89, '.jpg': 80, '.jpeg': 80, '.webp': 70, '.gif': 60}
    return priorities.get(suffix, 0)

def get_sort_key(info):
    is_protected = 1 if "_protect" in Path(info['path']).name else 0
    return (is_protected, info['pixels'], get_format_priority(info['path']), info['filesize'])

def hex_hamming_distance(hex_str1, hex_str2):
    try:
        if len(hex_str1) != len(hex_str2): return 999
        return bin(int(hex_str1, 16) ^ int(hex_str2, 16)).count('1')
    except ValueError:
        return 999

def bktree_distance(hash1, hash2):
    return hash1 - hash2

def format_pixels(pixels):
    if pixels >= 10000:
        val = pixels / 10000
        return f"{int(val)}万" if val.is_integer() else f"{val:.1f}万"
    return str(pixels)

def format_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}" if unit != 'B' else f"{size_in_bytes} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} TB"

# ==========================================
# データベース処理
# ==========================================
def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS images 
                 (path TEXT PRIMARY KEY, hash_str TEXT, color_hash_str TEXT, pixels INTEGER, 
                  filesize INTEGER, aspect_ratio REAL, mtime REAL, checked INTEGER DEFAULT 0)''')
    try: c.execute("ALTER TABLE images ADD COLUMN checked INTEGER DEFAULT 0")
    except sqlite3.OperationalError: pass
    c.execute('''CREATE TABLE IF NOT EXISTS similarity_edges (path1 TEXT, path2 TEXT, PRIMARY KEY(path1, path2))''')
    c.execute('''CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_exact ON images(hash_str, color_hash_str, pixels, filesize)')
    conn.commit()
    return conn, c

def delete_db_records(c, paths):
    for i in range(0, len(paths), 1000):
        chunk = paths[i:i+1000]
        c.executemany('DELETE FROM images WHERE path = ?', [(p,) for p in chunk])
        c.executemany('DELETE FROM similarity_edges WHERE path1 = ? OR path2 = ?', [(p, p) for p in chunk])

# ==========================================
# 画像スキャン・ハッシュ計算処理
# ==========================================
def compute_image_info(args_tuple):
    path_str, solid_tol = args_tuple
    path = Path(path_str)
    try:
        filesize = os.path.getsize(path)
        if filesize > 30 * 1024 * 1024: return path_str, None
        with Image.open(path) as img:
            orig_w, orig_h = img.size
            if orig_w * orig_h > 40000000: return path_str, None
            exif = img.getexif()
            if exif and exif.get(0x0112) in [5, 6, 7, 8]: orig_w, orig_h = orig_h, orig_w
            aspect_ratio = orig_w / orig_h if orig_h > 0 else 0
            img.draft('RGB', (256, 256))
            img = ImageOps.exif_transpose(img)
            img.thumbnail((256, 256))
            stat = ImageStat.Stat(img)
            if max(stat.stddev) < solid_tol: return path_str, None 
            h, c = imagehash.phash(img), imagehash.colorhash(img) 
            return path_str, (path_str, str(h), str(c), orig_w * orig_h, filesize, aspect_ratio, os.path.getmtime(path))
    except Exception:
        return path_str, None

# ==========================================
# GUI アプリケーション (Tkinter)
# ==========================================
class SimilarImageApp(tk.Tk):
    def __init__(self, groups, image_infos, auto_mode=False):
        super().__init__()
        self.groups = groups
        self.image_infos = image_infos
        self.auto_mode = auto_mode
        self.current_idx = 0
        
        self.trash_actions = {}
        self.protect_actions = {}
        self.at_actions = {}
        self.history_stack = []
        self.current_filtered_infos = []
        self.current_auto_trash = []
        self.last_action_msg = ""
        
        self.title("類似画像チェッカー - キーボードでサクサク整理！")
        try:
            self.state('zoomed') # Windows用最大化
        except:
            self.attributes('-fullscreen', True) # Mac/Linux向けフォールバック
            
        self._setup_ui()
        self.bind_all("<Left>", self._handle_back_shortcut) # ←キーで戻る
        self.bind_all("<Escape>", lambda e: self.quit())    # Escで終了
        
        self.show_current_group()

    def _setup_ui(self):
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        self.image_frame = ttk.Frame(self.main_frame)
        self.image_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.bottom_frame = ttk.Frame(self.main_frame)
        self.bottom_frame.pack(fill=tk.X, side=tk.BOTTOM, padx=10, pady=10)

        self.status_label = ttk.Label(self.bottom_frame, text="", font=("Meiryo", 12, "bold"), foreground="blue")
        self.status_label.pack(side=tk.TOP, pady=2)

        self.guide_label = ttk.Label(self.bottom_frame, text="【入力例】 1@ 3 / p1 / pa@ / a (全て残す) / d (全て削除) / b (戻る) / q (終了) / Enter (1番を残す)", font=("Meiryo", 11))
        self.guide_label.pack(side=tk.TOP, pady=2)

        self.entry_var = tk.StringVar()
        self.cmd_entry = ttk.Entry(self.bottom_frame, textvariable=self.entry_var, font=("Meiryo", 16), width=30)
        self.cmd_entry.pack(side=tk.TOP, pady=10)
        self.cmd_entry.bind("<Return>", self.on_enter)
        self.cmd_entry.focus_set()
        
        self.photo_refs = [] # 画像のガベージコレクション回避用

    def _handle_back_shortcut(self, event):
        if str(self.focus_get()) != str(self.cmd_entry):
            return
        self.entry_var.set("b")
        self.on_enter(None)

    def show_current_group(self):
        while self.current_idx < len(self.groups):
            for widget in self.image_frame.winfo_children():
                widget.destroy()
            self.photo_refs.clear()

            group_indices = self.groups[self.current_idx]
            group_infos = [self.image_infos[j] for j in group_indices]
            group_id = f"Group_{self.current_idx + 1}"

            max_pixels = group_infos[0]['pixels']
            filtered_infos = []
            auto_trash_paths = []

            for info in group_infos:
                if info['pixels'] <= max_pixels * 0.5 and info is not group_infos[0]:
                    if "_protect" in Path(info['path']).name:
                        filtered_infos.append(info)
                    else:
                        auto_trash_paths.append(info['path'])
                        print(f"  🗑️ [自動削除予定: 類似判定による低解像度] {Path(info['path']).name}")
                else:
                    filtered_infos.append(info)
            
            if len(filtered_infos) == 1:
                print(f"--- 📁 {group_id}/{len(self.groups)} (Auto Skip: 他は低解像度のため自動処理) ---")
                self._record_action(self.current_idx, auto_trash_paths, [], [])
                self.last_action_msg = f"📁 {group_id}: 他は低解像度のため自動で1枚残しました"
                self.current_idx += 1
                continue
            
            if self.auto_mode:
                print(f"--- 📁 {group_id}/{len(self.groups)} (Auto) ---")
                current_trash = auto_trash_paths + [info['path'] for info in filtered_infos[1:]]
                self._record_action(self.current_idx, current_trash, [], [])
                self.last_action_msg = f"📁 {group_id}: Autoモードで1枚残しました"
                self.current_idx += 1
                continue

            # --- ターミナルに出力（親ディレクトリ名を追加） ---
            print(f"--- 📁 {group_id}/{len(self.groups)} ---")
            for i, info in enumerate(filtered_infos):
                p = Path(info['path'])
                print(f"  [{i+1}] {p.parent.name}/{p.name}")

            # --- UIに描画 ---
            self.current_filtered_infos = filtered_infos
            self.current_auto_trash = auto_trash_paths
            
            total_imgs = len(filtered_infos)
            screen_w = self.winfo_screenwidth() - 50
            screen_h = self.winfo_screenheight() - 250
            img_w = max(200, screen_w // total_imgs)
            
            # 拡張子ごとの色分け設定
            ext_colors = {
                '.avif': '#e8f5e9', '.webp': '#f3e5f5', '.png':  '#e3f2fd',
                '.jpg':  '#fff3e0', '.jpeg': '#fff3e0', '.gif':  '#ffebee',
            }
            
            for i, info in enumerate(filtered_infos):
                ext_lower = Path(info['path']).suffix.lower()
                bg_color = ext_colors.get(ext_lower, '#ffffff') # 拡張子から背景色を取得
                
                frame = tk.Frame(self.image_frame, bd=2, relief=tk.GROOVE, bg=bg_color)
                frame.pack(side=tk.LEFT, expand=True, fill=tk.BOTH, padx=5)
                
                ext = Path(info['path']).suffix.upper()
                ratio = info['pixels'] / max_pixels
                
                if "_protect" in Path(info['path']).name:
                    mark, color = "[保護済]", "blue"
                elif info['pixels'] == max_pixels:
                    mark, color = "★最高画質", "red"
                elif ratio >= 0.8:
                    mark, color = "○近い画質", "green"
                else:
                    mark, color = "△低画質", "gray"
                
                txt = f"[{i+1}] {ext} {mark}\n{format_pixels(info['pixels'])}px / {format_size(info['filesize'])}"
                lbl = tk.Label(frame, text=txt, font=("Meiryo", 12, "bold"), fg=color, bg=bg_color)
                lbl.pack(side=tk.TOP, pady=5)
                
                try:
                    with Image.open(info['path']) as img:
                        img = ImageOps.exif_transpose(img)
                        img.thumbnail((img_w, screen_h))
                        pimg = ImageTk.PhotoImage(img)
                        self.photo_refs.append(pimg)
                        img_lbl = tk.Label(frame, image=pimg, bg=bg_color)
                        img_lbl.pack(side=tk.TOP, expand=True)
                except Exception as e:
                    err_lbl = tk.Label(frame, text=f"読込エラー\n{e}", fg="red", bg=bg_color)
                    err_lbl.pack(side=tk.TOP, expand=True)
            
            status_text = f"Group {self.current_idx + 1} / {len(self.groups)}"
            if self.last_action_msg:
                status_text = f"【前回の操作】 {self.last_action_msg}  |  " + status_text
                
            self.status_label.config(text=status_text)
            self.entry_var.set("")
            self.cmd_entry.focus_set()
            return # 入力待ちへ

        # 全グループ完了時
        self.quit()

    def _record_action(self, idx, trash, protect, at):
        self.trash_actions[idx] = trash
        self.protect_actions[idx] = protect
        self.at_actions[idx] = at

    def on_enter(self, event):
        ans = self.entry_var.get().strip().lower()
        
        if ans == 'q':
            print("🛑 処理を中断します。これまでの判定を反映して終了します。")
            logger.info("ユーザー操作により処理を中断しました。")
            self.quit()
            return
        
        if ans == 'b':
            if self.history_stack:
                self.current_idx = self.history_stack.pop()
                print(f"  ⏪ グループ {self.current_idx + 1} の判定をやり直します...")
                self.last_action_msg = f"[⏪ 戻る] Group {self.current_idx + 1} をやり直します"
                self.show_current_group()
            else:
                print("  ⚠️ これ以上戻れません（最初の操作グループです）。")
            return

        is_protect = False
        is_all_at = False

        if ans.startswith('p'):
            is_protect = True
            ans = ans[1:].strip()
            # もし入力が「p」だけだった場合は一番良い画像を残す
            if not ans: ans = '1'

        if ans == 'a@':
            ans = 'a'
            is_all_at = True

        action_prefix = "[🛡️保護] " if is_protect else ""
        keep_indices = []
        at_indices = []
        
        if ans == 'a':
            keep_indices = list(range(len(self.current_filtered_infos)))
            if is_all_at: 
                at_indices = keep_indices
                action_prefix += "[＠マーク] "
            print(f"  -> すべてを残しました。")
            self.last_action_msg = f"{action_prefix}すべてを残しました"
        elif ans in ['0', 'd']:
            keep_indices = []
            print("  -> このグループの画像をすべて削除予定にしました。")
            self.last_action_msg = f"すべて削除予定にしました"
        else:
            try:
                for part in ans.split():
                    has_at = '@' in part
                    num_str = part.replace('@', '')
                    if not num_str: continue
                    idx_val = int(num_str) - 1
                    if 0 <= idx_val < len(self.current_filtered_infos):
                        if idx_val not in keep_indices: keep_indices.append(idx_val)
                        if has_at and idx_val not in at_indices: at_indices.append(idx_val)
                
                if not keep_indices:
                    keep_indices = [0]
                    at_indices = []
                    print(f"  -> 有効な入力がなかったため、一番良い画像を1枚残しました。")
                    self.last_action_msg = f"{action_prefix}一番良い画像を1枚残しました"
                else:
                    kept_names = [Path(self.current_filtered_infos[i]['path']).name for i in keep_indices]
                    at_text = "[＠マーク] " if at_indices else ""
                    print(f"  -> 指定された画像を残しました: {', '.join(kept_names)}")
                    
                    kept_names_str = ', '.join(kept_names)
                    if len(kept_names_str) > 40:
                        kept_names_str = kept_names_str[:37] + "..."
                    self.last_action_msg = f"{action_prefix}{at_text}残しました -> {kept_names_str}"

            except ValueError:
                keep_indices = [0]
                at_indices = []
                print(f"  -> 入力エラーのため、一番良い画像を1枚残しました。")
                self.last_action_msg = f"{action_prefix}エラーのため一番良い画像を1枚残しました"

        current_protect = [self.current_filtered_infos[i]['path'] for i in keep_indices] if is_protect else []
        current_at = [self.current_filtered_infos[i]['path'] for i in at_indices]
        current_trash = self.current_auto_trash.copy()
        
        for i, info in enumerate(self.current_filtered_infos):
            if i not in keep_indices:
                current_trash.append(info['path'])

        self._record_action(self.current_idx, current_trash, current_protect, current_at)
        self.history_stack.append(self.current_idx)
        self.current_idx += 1
        self.show_current_group()


# ==========================================
# メイン処理・アクション実行
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="類似画像整理スクリプト")
    parser.add_argument("-l", "--level", type=int, choices=range(1, 17), default=1)
    parser.add_argument("-c", "--color-level", type=int, default=10)
    parser.add_argument("-a", "--auto", action="store_true")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-f", "--force-update", action="store_true")
    args = parser.parse_args()

    config = load_config()
    target_dirs_paths = [Path(d).resolve() for d in config["TARGET_DIRS"] if d.strip()]
    if not target_dirs_paths:
        print("❌ config.json に対象フォルダ(TARGET_DIRS)を設定してください。")
        return

    logger.info("=== スクリプト開始 ===")
    
    conn, c = init_db(DB_FILENAME)
    today_str = datetime.date.today().isoformat()
    needs_scan = args.force_update

    c.execute("SELECT value FROM metadata WHERE key = 'last_scan_date'")
    row = c.fetchone()
    if not row or row[0] != today_str: needs_scan = True

    c.execute("SELECT value FROM metadata WHERE key = 'last_level'")
    row_l = c.fetchone()
    last_l = int(row_l[0]) if row_l else -1
    
    c.execute("SELECT value FROM metadata WHERE key = 'last_color_level'")
    row_c = c.fetchone()
    last_c = int(row_c[0]) if row_c else -1

    if (last_l != -1 and last_l != args.level) or (last_c != -1 and last_c != args.color_level):
        print("⚠️ 判定レベル変更のためキャッシュをリセットします。")
        c.execute("DELETE FROM similarity_edges")
        c.execute("UPDATE images SET checked = 0")
        conn.commit()

    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_level', ?)", (str(args.level),))
    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_color_level', ?)", (str(args.color_level),))
    conn.commit()

    image_infos = []

    # --- スキャンとハッシュ計算 ---
    if needs_scan:
        print(f"🔍 以下のフォルダから画像ファイルを検索中...")
        target_files = []
        skipped_files_count = 0
        full_exclude_dirs = set(config["EXCLUDE_DIR_NAMES"])
        exclude_keywords = config["EXCLUDE_FILE_KEYWORDS"]
        
        for t_dir in target_dirs_paths:
            if not t_dir.is_dir(): continue
            print(f"  📂 {t_dir}")
            for p in t_dir.rglob('*'):
                if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
                    if any(ex in p.parts for ex in full_exclude_dirs):
                        skipped_files_count += 1
                        continue
                    if any(kw in p.name for kw in exclude_keywords):
                        skipped_files_count += 1
                        continue
                    if os.access(p, os.W_OK): 
                        target_files.append(p)
                        # ファイル検索の進捗を表示
                        if len(target_files) % 1000 == 0:
                            print(f"\r  🔍 {len(target_files)} 件のファイルを検出...", end="", flush=True)
                    else:
                        skipped_files_count += 1
                        
        print(f"\r  🔍 合計 {len(target_files)} 件の対象ファイルを発見しました。      ")
        
        if skipped_files_count > 0:
            print(f"  🛡️ 保護設定により {skipped_files_count} 個のファイルが処理対象から除外されました。")
            
        print(f"🗄️ データベースと実際のファイル状況を同期しています... (対象: {len(target_files)}件)")
                    
        c.execute('SELECT path, mtime, filesize, hash_str, color_hash_str, pixels, aspect_ratio FROM images')
        db_cache = {row[0]: row for row in c.fetchall()}
        
        to_compute = []
        valid_paths = set()
        total_targets = len(target_files)
        
        for i, p in enumerate(target_files):
            if i % 100 == 0 or i == total_targets - 1:
                print(f"\r  🔄 同期チェック中... {i + 1} / {total_targets} 件", end="", flush=True)

            p_str = str(p)
            valid_paths.add(p_str)
            mtime, fsize = os.path.getmtime(p), os.path.getsize(p)
            if p_str in db_cache and db_cache[p_str][1] == mtime and db_cache[p_str][2] == fsize:
                r = db_cache[p_str]
                image_infos.append({'path': p_str, 'hash_str': r[3], 'hash': imagehash.hex_to_hash(r[3]), 'color_hash_str': r[4], 'pixels': r[5], 'filesize': fsize, 'aspect_ratio': r[6]})
            else:
                to_compute.append(p_str)
                
        print() 

        total_db_cache = len(db_cache)
        db_delete = []
        for i, p_str in enumerate(db_cache):
            if i % 1000 == 0 or i == total_db_cache - 1:
                print(f"\r  🧹 不要データの確認中... {i + 1} / {total_db_cache} 件", end="", flush=True)
            if p_str not in valid_paths:
                db_delete.append(p_str)
        print()
        
        if db_delete: 
            print(f"  🗑️ 削除済み・除外済みのデータ {len(db_delete)} 件をデータベースから削除します...")
            delete_db_records(c, db_delete)
        conn.commit()

        if to_compute:
            print(f"🚀 {len(to_compute)} 個の新規/更新ファイルを検出。解析してデータベースに保存します...")
            batch = []
            processed_count = 0
            args_list = [(p, config["SOLID_TOLERANCE"]) for p in to_compute]
            
            with concurrent.futures.ProcessPoolExecutor() as executor:
                for path_str, res in executor.map(compute_image_info, args_list, chunksize=32):
                    processed_count += 1
                    if res:
                        batch.append((*res, 0))
                        image_infos.append({'path': path_str, 'hash_str': res[1], 'hash': imagehash.hex_to_hash(res[1]), 'color_hash_str': res[2], 'pixels': res[3], 'filesize': res[4], 'aspect_ratio': res[5]})
                    else:
                        delete_db_records(c, [path_str])
                        
                    if len(batch) >= 500:
                        c.executemany('INSERT OR REPLACE INTO images VALUES (?,?,?,?,?,?,?,?)', batch)
                        conn.commit()
                        batch = []
                    print(f"\r  💾 解析進捗... {processed_count} / {len(to_compute)} 件完了", end="", flush=True)
            if batch:
                c.executemany('INSERT OR REPLACE INTO images VALUES (?,?,?,?,?,?,?,?)', batch)
                conn.commit()
            print("\n  💾 データベースの記録が完了しました。")
                
        c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_scan_date', ?)", (today_str,))
        conn.commit()
    else:
        print("⏭️ 本日のデータベース更新は完了しているため、ファイルスキャンをスキップします。")
        c.execute('SELECT path, hash_str, color_hash_str, pixels, filesize, aspect_ratio FROM images')
        db_rows = c.fetchall()
        print(f"🗄️ データベースから {len(db_rows)} 件の画像情報を読み込み中...")
        for i, r in enumerate(db_rows):
            # データベース読み込みの進捗を表示
            if i % 1000 == 0 or i == len(db_rows) - 1:
                print(f"\r  ⏳ ファイルの存在確認中... {i + 1} / {len(db_rows)} 件", end="", flush=True)
            if os.path.exists(r[0]):
                image_infos.append({'path': r[0], 'hash_str': r[1], 'hash': imagehash.hex_to_hash(r[1]), 'color_hash_str': r[2], 'pixels': r[3], 'filesize': r[4], 'aspect_ratio': r[5]})
        print() # 改行して次の表示へ

    # --- 完全一致の事前削除 ---
    print(f"\n✨ 【STEP 1】完全に同一（または特徴が完全一致する低解像度）の画像を検索し、自動で整理します...")
    exact_groups = defaultdict(list)
    for info in image_infos:
        exact_groups[f"{info['hash_str']}_{info['color_hash_str']}"].append(info)
        
    filtered_infos = []
    exact_trash = []
    for g in exact_groups.values():
        if len(g) > 1:
            g.sort(key=get_sort_key, reverse=True)
            filtered_infos.append(g[0])
            for info in g[1:]:
                if "_protect" in Path(info['path']).name:
                    filtered_infos.append(info)
                elif info['pixels'] <= g[0]['pixels'] * 0.5 or (info['pixels'] == g[0]['pixels'] and info['filesize'] == g[0]['filesize']):
                    exact_trash.append(info['path'])
                    print(f"  🗑️ [自動削除: 完全一致コピーまたは低解像度] {Path(info['path']).name}")
                else:
                    filtered_infos.append(info)
        else:
            filtered_infos.append(g[0])
            
    if exact_trash and not args.dry_run:
        print(f"  ✅ {len(exact_trash)} 枚の完全一致・低解像度画像を自動でごみ箱に送りました。")
        for p in exact_trash:
            try: send2trash(p)
            except Exception: pass
        delete_db_records(c, exact_trash)
        conn.commit()
        for p in exact_trash: logger.info(f"[AUTO_TRASH] {p}")
    elif exact_trash:
        print(f"  ✅ {len(exact_trash)} 枚の完全一致・低解像度画像が見つかりました（ドライラン）。")
    else:
        print("  ✅ 該当する画像は見つかりませんでした。")

    image_infos = filtered_infos

    # --- 類似ペア検索とグループ化 ---
    if len(image_infos) < 2:
        print("処理する類似画像がありません。")
        return

    image_infos.sort(key=get_sort_key, reverse=True)
    path_to_idx = {info['path']: i for i, info in enumerate(image_infos)}
    
    c.execute("SELECT path FROM images WHERE checked = 0")
    unchecked = set(r[0] for r in c.fetchall())
    unchecked_idx = [i for i, info in enumerate(image_infos) if info['path'] in unchecked]

    adj = defaultdict(set)
    c.execute("SELECT path1, path2 FROM similarity_edges")
    for p1, p2 in c.fetchall():
        if p1 in path_to_idx and p2 in path_to_idx:
            i, j = path_to_idx[p1], path_to_idx[p2]
            adj[i].add(j)
            adj[j].add(i)

    if unchecked_idx:
        print(f"\n🌲 【STEP 2】新しく追加された未検証の {len(unchecked_idx)} 枚を中心に類似判定を計算します...")
        tree = pybktree.BKTree(bktree_distance)
        h_map = defaultdict(list)
        for i, info in enumerate(image_infos):
            if not h_map[info['hash_str']]: tree.add(info['hash'])
            h_map[info['hash_str']].append(i)
            
        new_edges = []
        for count, i in enumerate(unchecked_idx):
            if count > 0 and count % 100 == 0:
                print(f"\r  🔍 {count} / {len(unchecked_idx)} 枚を比較中...   ", end="", flush=True)
            info = image_infos[i]
            for _, m_hash in tree.find(info['hash'], args.level - 1):
                for j in h_map[str(m_hash)]:
                    if i != j:
                        if hex_hamming_distance(info['color_hash_str'], image_infos[j]['color_hash_str']) <= args.color_level:
                            if abs(info['aspect_ratio'] - image_infos[j]['aspect_ratio']) <= config["ASPECT_TOLERANCE"]:
                                adj[i].add(j); adj[j].add(i)
                                p1, p2 = info['path'], image_infos[j]['path']
                                new_edges.append((min(p1, p2), max(p1, p2)))
        print()
        if new_edges:
            c.executemany("INSERT OR IGNORE INTO similarity_edges VALUES (?, ?)", list(set(new_edges)))
        c.executemany("UPDATE images SET checked = 1 WHERE path = ?", [(image_infos[i]['path'],) for i in unchecked_idx])
        conn.commit()
    else:
        print(f"\n🌲 【STEP 2】すべての画像の類似計算は完了済みです。キャッシュから瞬時にグループを読み込みます！⚡")

    visited = set()
    groups = []
    for i in range(len(image_infos)):
        if i not in visited and i in adj:
            g_idx = []
            q = [i]
            visited.add(i)
            while q:
                node = q.pop(0)
                g_idx.append(node)
                for n in adj[node]:
                    if n not in visited:
                        visited.add(n)
                        q.append(n)
            if len(g_idx) > 1:
                g_idx.sort(key=lambda idx: get_sort_key(image_infos[idx]), reverse=True)
                groups.append(g_idx)

    # --- 保護済みの画像「しかない」グループを最初から除外する ---
    valid_groups = []
    for g_idx in groups:
        if not all("_protect" in Path(image_infos[j]['path']).name for j in g_idx):
            valid_groups.append(g_idx)
            
    groups = valid_groups

    if not groups:
        print(f"\n合計 0 個の類似グループが見つかりました（処理が必要な画像はありません）。\n")
        return
        
    print(f"\n処理が必要な類似グループが合計 {len(groups)} 個見つかりました。\n")

    # --- GUI表示 ---
    app = SimilarImageApp(groups, image_infos, args.auto)
    app.mainloop()

    # --- 結果の反映 (リネーム・削除) ---
    rename_tasks = defaultdict(lambda: {"protect": False, "at": False})
    for paths in app.protect_actions.values():
        for p in paths: rename_tasks[p]["protect"] = True
    for paths in app.at_actions.values():
        for p in paths: rename_tasks[p]["at"] = True

    if rename_tasks:
        print("\n🏷️ 選択された画像の保護・マーク処理（リネーム）を行っています...")
        for p_str, flags in rename_tasks.items():
            old_path = Path(p_str)
            if old_path.exists():
                new_stem = old_path.stem
                if flags["protect"] and "_protect" not in new_stem: new_stem += "_protect"
                if flags["at"] and not new_stem.endswith("@"): new_stem += "@"
                
                if new_stem != old_path.stem:
                    new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
                    if args.dry_run:
                        print(f"  [Dry-Run] リネーム予定: {old_path.name} -> {new_path.name}")
                    else:
                        try:
                            old_path.rename(new_path)
                            delete_db_records(c, [p_str])
                            logger.info(f"[RENAME] {old_path.name} -> {new_path.name}")
                        except Exception as e:
                            logger.error(f"[RENAME_ERROR] {old_path.name} - {e}")
                            print(f"  ⚠️ リネーム失敗: {old_path.name} ({e})")
        conn.commit()

    all_trash = []
    for paths in app.trash_actions.values(): all_trash.extend(paths)
    
    if all_trash:
        print(f"\n🗑️ 最終処理: 選択された合計 {len(all_trash)} 枚の画像をゴミ箱に移動しています...")
        moved_count = 0
        for p in all_trash:
            if args.dry_run:
                print(f"  [Dry-Run] 削除予定: {Path(p).name}")
            else:
                try:
                    send2trash(p)
                    moved_count += 1
                    logger.info(f"[TRASH] {p}")
                except Exception as e:
                    logger.error(f"[TRASH_ERROR] {p} - {e}")
                    print(f"  ⚠️ エラー: {Path(p).name} をごみ箱に移動できませんでした ({e})")
        
        if not args.dry_run:
            delete_db_records(c, all_trash)
            conn.commit()
            print(f"✅ {moved_count} 枚の不要画像を処理しました。")

    conn.close()
    logger.info("=== スクリプト完了 ===")
    print("\n🎉 すべての処理が完了しました！")

if __name__ == "__main__":
    main()