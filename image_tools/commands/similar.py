"""類似画像の検出・整理（GUI / キャッシュ DB はプロジェクト直下）。"""

import os
import argparse
import multiprocessing
import warnings
import math
import datetime
import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

from image_tools.paths import PROJECT_ROOT, config_json, hash_cache_db
from image_tools.cache_db import (
    init_db,
    delete_db_records,
    SQL_INSERT_OR_REPLACE_IMAGE,
)

from PIL import Image, ImageOps, ImageStat, ImageFile, ImageTk
import tkinter as tk
from tkinter import ttk
import imagehash
from send2trash import send2trash
import pybktree
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

try:
    import pillow_avif
except ImportError:
    pass # AVIF非対応環境でも動くようにフォールバック

import sys
from pathlib import Path

# ==========================================
# 初期設定・準備
# ==========================================
ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.simplefilter('ignore', Image.DecompressionBombWarning)

CONFIG_FILE = config_json()
DB_FILENAME = hash_cache_db()

# ログ設定（プロジェクトルートに出力）
LOG_FILENAME = PROJECT_ROOT / f"similar_images_log_{datetime.date.today().strftime('%Y%m%d')}.txt"
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# デフォルト設定
DEFAULT_CONFIG = {
    "TARGET_DIRS": [
        r"E:\gaz 画像"
    ],
    "EXCLUDE_DIR_NAMES": ["お気に入り", "除外フォルダ", "絶対消さない"],
    "EXCLUDE_FILE_KEYWORDS": ["_keep", "保護"],
    "ASPECT_TOLERANCE": 0.1,
    "SOLID_TOLERANCE": 2.0
}

SUPPORTED_EXTS = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.avif'}

# 画像解析・DB バッチ（マジックナンバー集約）
MAX_IMAGE_FILE_BYTES = 30 * 1024 * 1024
MAX_IMAGE_PIXELS = 40_000_000
THUMB_SIZE = 256
DB_INSERT_BATCH_SIZE = 2000

# ==========================================
# ユーティリティ関数
# ==========================================
def contains_protect_marker(name: str) -> bool:
    """ファイル名または stem に _protect が含まれるか（表記ゆれ防止用）"""
    return "_protect" in name


@dataclass
class NormalizedSelectionCommand:
    is_protect: bool
    is_all_at: bool
    body: str


@dataclass
class SelectionResult:
    keep_indices: list
    at_indices: list
    last_action_msg: str
    log_line: str | None = None


def normalize_selection_command(ans: str) -> NormalizedSelectionCommand:
    """p 前置・a@/a+ の正規化のみ（q/s/b は呼び出し側）"""
    is_protect = False
    is_all_at = False
    s = ans
    if s.startswith("p"):
        is_protect = True
        s = s[1:].strip()
        if not s:
            s = "1"
    if s in ("a@", "a+"):
        s = "a"
        is_all_at = True
    return NormalizedSelectionCommand(is_protect=is_protect, is_all_at=is_all_at, body=s)


def compute_selection_indices(cmd: NormalizedSelectionCommand, file_names: list[str]) -> SelectionResult:
    """
    残すインデックス・＠対象・ステータス文言を決定（GUI 非依存の純粋処理）。
    file_names[i] は表示用ファイル名（ログ用）。
    """
    body = cmd.body
    n = len(file_names)
    action_prefix = "[🛡️保護] " if cmd.is_protect else ""
    keep_indices: list[int] = []
    at_indices: list[int] = []

    if body == "a":
        keep_indices = list(range(n))
        if cmd.is_all_at:
            at_indices = list(keep_indices)
            action_prefix += "[＠マーク] "
        return SelectionResult(
            keep_indices=keep_indices,
            at_indices=at_indices,
            last_action_msg=f"{action_prefix}すべてを残しました",
            log_line="  -> すべてを残しました。",
        )

    if body in ("0", "d"):
        return SelectionResult(
            keep_indices=[],
            at_indices=[],
            last_action_msg="すべて削除予定にしました",
            log_line="  -> すべて削除予定にしました。",
        )

    try:
        work = body
        if work in ("@", "+"):
            work = "1@"

        for part in work.split():
            has_at = ("@" in part) or ("+" in part)
            num_str = part.replace("@", "").replace("+", "")
            if not num_str:
                continue
            idx_val = int(num_str) - 1
            if 0 <= idx_val < n:
                if idx_val not in keep_indices:
                    keep_indices.append(idx_val)
                if has_at and idx_val not in at_indices:
                    at_indices.append(idx_val)

        if not keep_indices:
            return SelectionResult(
                keep_indices=[0],
                at_indices=[],
                last_action_msg=f"{action_prefix}一番良い画像を1枚残しました",
                log_line=None,
            )

        kept_names = [file_names[i] for i in keep_indices]
        at_text = "[＠マーク] " if at_indices else ""
        kept_names_str = ", ".join(kept_names)
        if len(kept_names_str) > 40:
            kept_names_str = kept_names_str[:37] + "..."
        return SelectionResult(
            keep_indices=keep_indices,
            at_indices=at_indices,
            last_action_msg=f"{action_prefix}{at_text}残しました -> {kept_names_str}",
            log_line=f"  -> 指定された画像を残しました: {', '.join(kept_names)}",
        )
    except ValueError:
        return SelectionResult(
            keep_indices=[0],
            at_indices=[],
            last_action_msg=f"{action_prefix}エラーのため一番良い画像を1枚残しました",
            log_line=None,
        )


def group_size_info_str(group_infos) -> str:
    total_size = sum(info["filesize"] for info in group_infos)
    max_size = max(info["filesize"] for info in group_infos)
    freeable_size = total_size - max_size
    return f"総容量: {format_size(total_size)} / 削減見込: {format_size(freeable_size)}"


def filter_similar_group_members(group_infos):
    """
    類似グループ内で低解像度を auto_trash に回す。
    戻り値: (filtered_infos, auto_trash_paths, max_pixels)
    """
    max_pixels = group_infos[0]["pixels"]
    filtered_infos = []
    auto_trash_paths = []
    for info in group_infos:
        if info["pixels"] <= max_pixels * 0.5 and info is not group_infos[0]:
            if contains_protect_marker(Path(info["path"]).name):
                filtered_infos.append(info)
            else:
                auto_trash_paths.append(info["path"])
                print(f"  🗑️ [自動削除予定: 類似判定による低解像度] {Path(info['path']).name}")
        else:
            filtered_infos.append(info)
    return filtered_infos, auto_trash_paths, max_pixels


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
    is_protected = 1 if contains_protect_marker(Path(info["path"]).name) else 0
    return (is_protected, info['pixels'], get_format_priority(info['path']), info['filesize'])

def hex_hamming_distance(hex_str1, hex_str2):
    try:
        if len(hex_str1) != len(hex_str2):
            return 999
        return (int(hex_str1, 16) ^ int(hex_str2, 16)).bit_count()
    except ValueError:
        return 999

_phash_obj_cache = {}

def phash_obj(hash_str):
    """同一 hash_str は1回だけ hex→ImageHash 変換（数十万枚で効く）"""
    o = _phash_obj_cache.get(hash_str)
    if o is None:
        o = imagehash.hex_to_hash(hash_str)
        _phash_obj_cache[hash_str] = o
    return o

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

def compute_image_info(args_tuple):
    path_str, solid_tol = args_tuple
    path = Path(path_str)
    try:
        filesize = os.path.getsize(path)
        if filesize > MAX_IMAGE_FILE_BYTES:
            return path_str, None
        with Image.open(path) as img:
            orig_w, orig_h = img.size
            if orig_w * orig_h > MAX_IMAGE_PIXELS:
                return path_str, None
            exif = img.getexif()
            if exif and exif.get(0x0112) in [5, 6, 7, 8]:
                orig_w, orig_h = orig_h, orig_w
            aspect_ratio = orig_w / orig_h if orig_h > 0 else 0
            
            img.draft("RGB", (THUMB_SIZE, THUMB_SIZE))
            # パレットモードで透過情報を持つ場合はRGBAに変換して警告を回避
            if img.mode == 'P' and 'transparency' in img.info:
                img = img.convert('RGBA')

            img = ImageOps.exif_transpose(img)
            img.thumbnail((THUMB_SIZE, THUMB_SIZE))
            stat = ImageStat.Stat(img)
            if max(stat.stddev) < solid_tol:
                return path_str, None
            h, c = imagehash.phash(img), imagehash.colorhash(img)
            return path_str, (
                path_str,
                str(h),
                str(c),
                orig_w * orig_h,
                filesize,
                aspect_ratio,
                os.path.getmtime(path),
            )
    except Exception:
        logger.debug("compute_image_info 失敗: %s", path_str, exc_info=True)
        return path_str, None

# ==========================================
# GUI アプリケーション (Tkinter)
# ==========================================
class SimilarImageApp(tk.Tk):
    def __init__(self, groups, image_infos, auto_mode=False, args=None, c=None, conn=None):
        super().__init__()
        self.groups = groups
        self.image_infos = image_infos
        self.auto_mode = auto_mode
        self.args = args
        self.c = c
        self.conn = conn
        self.current_idx = 0
        
        self.trash_actions = {}
        self.protect_actions = {}
        self.at_actions = {}
        self.history_stack = []
        self.current_filtered_infos = []
        self.current_auto_trash = []
        self.last_action_msg = ""
        self.thumbnail_executor = ThreadPoolExecutor(max_workers=4)
        
        self.title("類似画像チェッカー")
        try:
            self.state('zoomed')
        except:
            w, h = self.winfo_screenwidth(), self.winfo_screenheight()
            self.geometry(f"{w}x{h}+0+0")
            
        self._setup_ui()
        self.bind_all("<Escape>", lambda e: self.quit())
        
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

        # 案内テキストを変更
        self.guide_label = ttk.Label(self.bottom_frame, text="【入力例】o 1番変換 / oa 全変換 / p 保存 / a 全残す / d 全削除 / b 戻る / s 途中保存 / q 終了 / Enter (1番残す)", font=("Meiryo", 11))
        self.guide_label.pack(side=tk.TOP, pady=2)

        self.entry_var = tk.StringVar()
        # q d @ 0 + b は Enter なしで1文字目から即実行
        self._immediate_cmd_chars = frozenset("qd@0+b")
        self.cmd_entry = ttk.Entry(self.bottom_frame, textvariable=self.entry_var, font=("Meiryo", 16), width=30)
        self.cmd_entry.pack(side=tk.TOP, pady=10)
        self.entry_var.trace_add("write", self._on_entry_write)

        self.bind_all("<Return>", self.on_enter)
        self.bind_all("<Button-1>", lambda e: self.cmd_entry.focus_set())
        
        self.cmd_entry.focus_set()
        
        self.photo_refs = []

    def show_current_group(self):
        while self.current_idx < len(self.groups):
            for widget in self.image_frame.winfo_children():
                widget.destroy()
            self.photo_refs.clear()

            group_indices = self.groups[self.current_idx]
            group_infos = [self.image_infos[j] for j in group_indices]
            group_id = f"Group_{self.current_idx + 1}"
            size_info_str = group_size_info_str(group_infos)
            filtered_infos, auto_trash_paths, max_pixels = filter_similar_group_members(group_infos)

            if len(filtered_infos) == 1:
                print(f"--- 📁 {group_id}/{len(self.groups)} (Auto Skip | {size_info_str}) ---")
                self._record_action(self.current_idx, auto_trash_paths, [], [])
                self.last_action_msg = f"📁 {group_id}: 他は低解像度のため自動で1枚残しました"
                self.current_idx += 1
                continue

            if self.auto_mode:
                print(f"--- 📁 {group_id}/{len(self.groups)} (Auto | {size_info_str}) ---")
                current_trash = auto_trash_paths + [info["path"] for info in filtered_infos[1:]]
                self._record_action(self.current_idx, current_trash, [], [])
                self.last_action_msg = f"📁 {group_id}: Autoモードで1枚残しました"
                self.current_idx += 1
                continue

            print(f"--- 📁 {group_id}/{len(self.groups)} ({size_info_str}) ---")
            for i, info in enumerate(filtered_infos):
                p = Path(info["path"])
                print(f"  [{i+1}] {p.parent.name}/{p.name}")

            self.current_filtered_infos = filtered_infos
            self.current_auto_trash = auto_trash_paths
            self._populate_image_grid(filtered_infos, max_pixels, size_info_str)
            return

        self.quit()

    def _populate_image_grid(self, filtered_infos, max_pixels, size_info_str):
        """サムネイルグリッドとステータス行の描画"""
        total_imgs = len(filtered_infos)
        screen_w = self.winfo_screenwidth() - 50
        screen_h = self.winfo_screenheight() - 250

        max_cols = min(4, total_imgs)
        if max_cols == 0:
            max_cols = 1
        rows = math.ceil(total_imgs / max_cols)

        img_w = max(150, screen_w // max_cols)
        img_h = max(150, screen_h // rows)

        for c in range(10):
            self.image_frame.columnconfigure(c, weight=0)
        for r in range(10):
            self.image_frame.rowconfigure(r, weight=0)

        for c in range(max_cols):
            self.image_frame.columnconfigure(c, weight=1)
        for r in range(rows):
            self.image_frame.rowconfigure(r, weight=1)

        ext_colors = {
            ".avif": "#e8f5e9",
            ".webp": "#f3e5f5",
            ".png": "#e3f2fd",
            ".jpg": "#fff3e0",
            ".jpeg": "#fff3e0",
            ".gif": "#ffebee",
        }

        for i, info in enumerate(filtered_infos):
            ext_lower = Path(info["path"]).suffix.lower()
            bg_color = ext_colors.get(ext_lower, "#ffffff")

            frame = tk.Frame(self.image_frame, bd=2, relief=tk.GROOVE, bg=bg_color)
            row_idx = i // max_cols
            col_idx = i % max_cols
            frame.grid(row=row_idx, column=col_idx, sticky="nsew", padx=5, pady=5)
            ext = Path(info["path"]).suffix.upper()
            ratio = info["pixels"] / max_pixels

            if contains_protect_marker(Path(info["path"]).name):
                mark, color = "[保護済]", "blue"
            elif info["pixels"] == max_pixels:
                mark, color = "★最高画質", "red"
            elif ratio >= 0.8:
                mark, color = "○近い画質", "green"
            else:
                mark, color = "△低画質", "gray"

            txt = f"[{i+1}] {ext} {mark}\n{format_pixels(info['pixels'])}px / {format_size(info['filesize'])}"
            lbl = tk.Label(frame, text=txt, font=("Meiryo", 12, "bold"), fg=color, bg=bg_color)
            lbl.pack(side=tk.TOP, pady=5)

            # プレースホルダー表示
            img_lbl = tk.Label(frame, text="Loading...", bg=bg_color)
            img_lbl.pack(side=tk.TOP, expand=True)
            
            # 非同期で画像を読み込み
            self.thumbnail_executor.submit(self._load_thumbnail_async, info["path"], img_w, img_h, img_lbl, bg_color)

        status_text = f"Group {self.current_idx + 1} / {len(self.groups)}  [ {size_info_str} ]"

    def _load_thumbnail_async(self, path, w, h, label, bg_color):
        """別スレッドで画像を読み込み、GUIスレッドでラベルを更新する"""
        try:
            with Image.open(path) as img:
                img = ImageOps.exif_transpose(img)
                img.thumbnail((w, h))
                pimg = ImageTk.PhotoImage(img)
                # メインスレッドでラベルを更新
                self.after(0, self._update_image_label, label, pimg)
        except Exception as e:
            self.after(0, lambda: label.config(text=f"Error\n{e}", fg="red"))

    def _update_image_label(self, label, pimg):
        if label.winfo_exists():
            self.photo_refs.append(pimg) # 参照保持
            label.config(image=pimg, text="")

        if self.last_action_msg:
            status_text = f"【前回の操作】 {self.last_action_msg}  |  " + status_text

        self.status_label.config(text=status_text)
        self.entry_var.set("")
        self.cmd_entry.focus_set()

    def _record_action(self, idx, trash, protect, at):
        self.trash_actions[idx] = trash
        self.protect_actions[idx] = protect
        self.at_actions[idx] = at

    def _on_entry_write(self, *args):
        s = self.entry_var.get().strip().lower()
        if len(s) != 1 or s not in self._immediate_cmd_chars:
            return

        def _try_immediate():
            s2 = self.entry_var.get().strip().lower()
            if len(s2) != 1 or s2 not in self._immediate_cmd_chars:
                return
            self._apply_command(s2)

        self.after_idle(_try_immediate)

    def on_enter(self, event):
        ans = self.entry_var.get().strip().lower()
        self._apply_command(ans)

    def _apply_command(self, ans):
        if ans == 'q':
            print("🛑 処理を中断します。これまでの判定を反映して終了します。")
            self.quit()
            return
            
        if ans == 's':
            print("💾 これまでの判定を直ちにファイルに反映（途中保存）します...")
            self.apply_pending_actions()
            self.last_action_msg = "💾 これまでの変更を保存しました（ここから前には戻れません）"
            self.entry_var.set("")
            self.show_current_group()
            return
        
        if ans == 'b':
            if self.history_stack:
                self.current_idx = self.history_stack.pop()
                self.last_action_msg = f"[⏪ 戻る] Group {self.current_idx + 1} をやり直します"
                self.show_current_group()
            return

        # o コマンド (最適化) の処理
        if ans.startswith('o'):
            body = ans[1:].strip()
            indices = []
            
            if not body:
                # 'o' 単体なら 1番の画像（インデックス0）のみを対象
                if self.current_filtered_infos:
                    indices = [0]
            elif body == 'a':
                # 'oa' なら現在表示中のすべてを対象
                indices = list(range(len(self.current_filtered_infos)))
            else:
                try:
                    for part in body.split():
                        idx = int(part) - 1
                        if 0 <= idx < len(self.current_filtered_infos):
                            indices.append(idx)
                except ValueError:
                    self.last_action_msg = "⚠️ 'o' の後の指定が不正です (例: o, oa, o 1 2)"
                    self.entry_var.set("")
                    self.show_current_group()
                    return
            
            self.optimize_selected_images(indices)
            return

        cmd = normalize_selection_command(ans)
        file_names = [Path(self.current_filtered_infos[i]["path"]).name for i in range(len(self.current_filtered_infos))]
        result = compute_selection_indices(cmd, file_names)
        if result.log_line:
            print(result.log_line)
        self.last_action_msg = result.last_action_msg
        keep_indices = result.keep_indices
        at_indices = result.at_indices
        is_protect = cmd.is_protect

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

    def optimize_selected_images(self, indices):
        """選択された画像を optimizer.py で変換・最適化し、DBとUIを更新する"""
        if not indices:
            return

        print(f"\n🔄 選択された {len(indices)} 枚の画像を optimizer.py で最適化しています...")
        self.status_label.config(text="⏳ 画像を変換・最適化しています... しばらくお待ちください")
        self.update() # GUIを強制アップデートして待機表示を出す

        success_count = 0
        saved_total = 0
        # インポートをメソッド内で行い、子プロセス起動時のオーバーヘッドと競合を避ける
        import image_tools.commands.optimizer as optimizer
        config = load_config()

        for idx in indices:
            info = self.current_filtered_infos[idx]
            old_path_str = info["path"]
            old_path = Path(old_path_str)

            try:
                success, saved, new_path, orig_path = optimizer.process_single_image(old_path, as_grayscale=False)
                success, saved, new_path, orig_path = optimizer.process_single_image(old_path, as_grayscale=False, as_square=False)
                
                if success and new_path:
                    with Image.open(new_path) as test_img:
                        test_img.load()
                    
                    new_path_str = str(new_path.resolve())
                    orig_path_str = str(orig_path.resolve())

                    if new_path_str != orig_path_str:
                        send2trash(orig_path_str)
                        
                    _, res = compute_image_info((new_path_str, config["SOLID_TOLERANCE"]))
                    if res:
                        res_path, hash_str, color_hash_str, pixels, filesize, aspect_ratio, mtime = res
                        
                        # DB更新
                        if new_path_str != old_path_str:
                            self.c.execute("DELETE FROM images WHERE path = ?", (old_path_str,))
                            self.c.execute("UPDATE similarity_edges SET path1 = ? WHERE path1 = ?", (new_path_str, old_path_str))
                            self.c.execute("UPDATE similarity_edges SET path2 = ? WHERE path2 = ?", (new_path_str, old_path_str))
                            
                        self.c.execute(SQL_INSERT_OR_REPLACE_IMAGE, (*res, 1))
                        self.conn.commit()

                        # メモリ内の辞書を直接更新
                        info['path'] = new_path_str
                        info['hash_str'] = hash_str
                        info['color_hash_str'] = color_hash_str
                        info['pixels'] = pixels
                        info['filesize'] = filesize
                        info['aspect_ratio'] = aspect_ratio
                        
                        success_count += 1
                        saved_total += saved
                    else:
                        print(f"  ⚠️ {new_path.name} の再解析に失敗しました")

            except Exception as e:
                print(f"  ⚠️ {old_path.name} の最適化処理でエラー: {e}")

        if success_count > 0:
            self.last_action_msg = f"✨ {success_count}枚を最適化し、合計 {format_size(saved_total)} の容量を削減しました！"
        else:
            self.last_action_msg = "⚠️ 最適化しましたが、元の画像より容量が減らなかったか、エラーになりました。"

        self.entry_var.set("")
        self.show_current_group()

    def apply_pending_actions(self):
        if not self.c or not self.conn or not self.args:
            return

        rename_tasks = defaultdict(lambda: {"protect": False, "at": False})
        for paths in self.protect_actions.values():
            for p in paths: rename_tasks[p]["protect"] = True
        for paths in self.at_actions.values():
            for p in paths: rename_tasks[p]["at"] = True

        if rename_tasks:
            print("\n🏷️ 選択された画像の保護・マーク処理（リネーム）を行っています...")
            for p_str, flags in rename_tasks.items():
                old_path = Path(p_str)
                if old_path.exists():
                    new_stem = old_path.stem
                    if flags["protect"] and not contains_protect_marker(new_stem): new_stem += "_protect"
                    if flags["at"] and not new_stem.endswith("@"): new_stem += "@"
                    
                    if new_stem != old_path.stem:
                        new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
                        if not self.args.dry_run:
                            try:
                                old_path.rename(new_path)
                                new_p_str = str(new_path)
                                self.c.execute("UPDATE images SET path = ? WHERE path = ?", (new_p_str, p_str))
                                self.c.execute("UPDATE similarity_edges SET path1 = ? WHERE path1 = ?", (new_p_str, p_str))
                                self.c.execute("UPDATE similarity_edges SET path2 = ? WHERE path2 = ?", (new_p_str, p_str))
                            except Exception as e:
                                print(f"  ⚠️ リネーム失敗: {old_path.name} ({e})")
            self.conn.commit()

        all_trash = []
        for paths in self.trash_actions.values(): all_trash.extend(paths)
        
        if all_trash:
            print(f"\n🗑️ 途中保存: 選択された合計 {len(all_trash)} 枚の画像をゴミ箱に移動しています...")
            moved_count = 0
            for p in all_trash:
                if not self.args.dry_run:
                    try:
                        send2trash(p)
                        moved_count += 1
                    except Exception as e:
                        print(f"  ⚠️ エラー: {Path(p).name} をごみ箱に移動できません ({e})")
            
            if not self.args.dry_run:
                delete_db_records(self.c, all_trash)
                self.conn.commit()
                print(f"✅ {moved_count} 枚の不要画像を処理しました。")

        # 反映済みのタスクと履歴をリセット
        self.protect_actions.clear()
        self.at_actions.clear()
        self.trash_actions.clear()
        self.history_stack.clear()


# ==========================================
# フェーズ分割された関数群
# ==========================================
def scan_and_sync_files(args, config, conn, c, needs_scan, target_dirs_paths, today_str):
    image_infos = []
    
    if not needs_scan:
        print("⏭️ ファイルスキャンをスキップし、既存のデータベースから読み込みます（-f で再スキャン可能）。")
        c.execute('SELECT path, hash_str, color_hash_str, pixels, filesize, aspect_ratio FROM images')
        db_rows = c.fetchall()
        print(f"🗄️ データベースから {len(db_rows)} 件の画像情報を読み込み中...")
        for r in db_rows:
            image_infos.append({'path': r[0], 'hash_str': r[1], 'color_hash_str': r[2], 'pixels': r[3], 'filesize': r[4], 'aspect_ratio': r[5]})
        print("  ✅ 読み込み完了")
        return image_infos

    print(f"️ データベースと実際のファイル状況を同期しています...")
    c.execute('SELECT path, mtime, filesize, hash_str, color_hash_str, pixels, aspect_ratio FROM images')
    db_cache = {row[0]: row for row in c.fetchall()}
    db_dir_map = defaultdict(list)
    for p_str, r in db_cache.items():
        db_dir_map[str(Path(p_str).parent)].append(r)

    c.execute("SELECT path, mtime FROM folder_mtimes")
    db_folder_mtimes = dict(c.fetchall())
    
    to_compute = []
    valid_paths = set()
    new_folder_mtimes = []
    full_exclude_dirs = set(config["EXCLUDE_DIR_NAMES"])
    exclude_keywords = config["EXCLUDE_FILE_KEYWORDS"]
    
    print(f"🔍 画像ファイルを検索中...")
    for t_dir in target_dirs_paths:
        if not t_dir.is_dir(): continue
        for root, dirs, files in os.walk(t_dir):
            # 除外フォルダの設定を反映（dirs[:] を書き換えることで walk が中に入らなくなる）
            dirs[:] = [d for d in dirs if d not in full_exclude_dirs]
            
            root_str = str(Path(root))
            try:
                current_mtime = os.path.getmtime(root)
            except OSError: continue

            if db_folder_mtimes.get(root_str) == current_mtime and root_str in db_dir_map:
                # フォルダの更新時刻が変わっていない場合は、DBの内容をそのまま信頼して利用
                for r in db_dir_map[root_str]:
                    p_str = r[0]
                    if any(kw in os.path.basename(p_str) for kw in exclude_keywords): continue
                    valid_paths.add(p_str)
                    image_infos.append({'path': p_str, 'hash_str': r[3], 'color_hash_str': r[4], 'pixels': r[5], 'filesize': r[2], 'aspect_ratio': r[6]})
            else:
                # フォルダが更新されているか未登録の場合は、中のファイルを個別にチェック
                for f in files:
                    if Path(f).suffix.lower() not in SUPPORTED_EXTS: continue
                    if any(kw in f for kw in exclude_keywords): continue
                    
                    p = Path(root) / f
                    p_str = str(p)
                    try:
                        stat = p.stat()
                        mtime, fsize = stat.st_mtime, stat.st_size
                        if not os.access(p, os.W_OK): continue
                    except OSError: continue

                    valid_paths.add(p_str)
                    if p_str in db_cache and db_cache[p_str][1] == mtime and db_cache[p_str][2] == fsize:
                        r = db_cache[p_str]
                        image_infos.append({'path': p_str, 'hash_str': r[3], 'color_hash_str': r[4], 'pixels': r[5], 'filesize': fsize, 'aspect_ratio': r[6]})
                    else:
                        to_compute.append(p_str)
                
                new_folder_mtimes.append((root_str, current_mtime))

    if new_folder_mtimes:
        c.executemany("INSERT OR REPLACE INTO folder_mtimes VALUES (?, ?)", new_folder_mtimes)
        conn.commit()

    print(f"  🔍 合計 {len(valid_paths)} 件の対象ファイルを確認しました。")

    db_delete = [p_str for p_str in db_cache if p_str not in valid_paths]
    if db_delete: 
        delete_db_records(c, db_delete)
        conn.commit()

    if to_compute:
        print(f"🚀 {len(to_compute)} 個の新規/更新ファイルを解析して保存します...")
        batch = []
        args_list = [(p, config["SOLID_TOLERANCE"]) for p in to_compute]
        n_workers = os.cpu_count() or 3
        chunk = max(32, min(256, len(args_list) // (n_workers * 4) or 32))
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_workers) as executor:
            results = executor.map(compute_image_info, args_list, chunksize=chunk)
            for path_str, res in tqdm(results, total=len(args_list), desc="⏳ 画像解析"):
                if res:
                    batch.append((*res, 0))
                    image_infos.append({'path': path_str, 'hash_str': res[1], 'color_hash_str': res[2], 'pixels': res[3], 'filesize': res[4], 'aspect_ratio': res[5]})
                else:
                    delete_db_records(c, [path_str])
                    
                if len(batch) >= DB_INSERT_BATCH_SIZE:
                    c.executemany(SQL_INSERT_OR_REPLACE_IMAGE, batch)
                    conn.commit()
                    batch = []
            executor.shutdown(wait=True) # 明示的に終了を待機
        if batch:
            c.executemany(SQL_INSERT_OR_REPLACE_IMAGE, batch)
            conn.commit()
            
    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_scan_date', ?)", (today_str,))
    conn.commit()
    return image_infos

def process_exact_matches(image_infos, args, c, conn):
    print(f"\n✨ 【STEP 1】完全に同一（または特徴が完全一致する低解像度）の画像を自動で整理します...")
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
                if contains_protect_marker(Path(info["path"]).name):
                    filtered_infos.append(info)
                elif info['pixels'] <= g[0]['pixels'] * 0.5 or (info['pixels'] == g[0]['pixels'] and info['filesize'] == g[0]['filesize']):
                    exact_trash.append(info['path'])
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
    return filtered_infos

def find_similar_groups(image_infos, args, config, c, conn):
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
        print(f"\n🌲 【STEP 2】新しく追加された {len(unchecked_idx)} 枚を中心に類似判定を計算します...")
        tree = pybktree.BKTree(bktree_distance)
        h_map = defaultdict(list)
        for i, info in enumerate(image_infos):
            hs = info['hash_str']
            if not h_map[hs]:
                tree.add(phash_obj(hs))
            h_map[hs].append(i)
            
        new_edges = []
        for i in tqdm(unchecked_idx, desc="⏳ 類似判定"):
            info = image_infos[i]
            for _, m_hash in tree.find(phash_obj(info['hash_str']), args.level - 1):
                for j in h_map[str(m_hash)]:
                    if i != j:
                        if hex_hamming_distance(info['color_hash_str'], image_infos[j]['color_hash_str']) <= args.color_level:
                            if abs(info['aspect_ratio'] - image_infos[j]['aspect_ratio']) <= config["ASPECT_TOLERANCE"]:
                                adj[i].add(j); adj[j].add(i)
                                p1, p2 = info['path'], image_infos[j]['path']
                                new_edges.append((min(p1, p2), max(p1, p2)))
        if new_edges:
            c.executemany("INSERT OR IGNORE INTO similarity_edges VALUES (?, ?)", list(set(new_edges)))
        c.executemany("UPDATE images SET checked = 1 WHERE path = ?", [(image_infos[i]['path'],) for i in unchecked_idx])
        conn.commit()

    visited = set()
    groups = []
    for i in range(len(image_infos)):
        if i not in visited and i in adj:
            g_idx = []
            q = deque([i])
            visited.add(i)
            while q:
                node = q.popleft()
                g_idx.append(node)
                for n in adj[node]:
                    if n not in visited:
                        visited.add(n)
                        q.append(n)
            if len(g_idx) > 1:
                g_idx.sort(key=lambda idx: get_sort_key(image_infos[idx]), reverse=True)
                groups.append(g_idx)

    valid_groups = [
        g_idx for g_idx in groups
        if not all(contains_protect_marker(Path(image_infos[j]["path"]).name) for j in g_idx)
    ]
    
    if args.sort_size:
        valid_groups.sort(key=lambda g_idx: sum(image_infos[j]['filesize'] for j in g_idx), reverse=True)
        
    return valid_groups


# ==========================================
# メインプロセス
# ==========================================
def setup_path():
    """インポートパスの解決"""
    _current_dir = str(Path(__file__).resolve().parent)
    if _current_dir not in sys.path:
        sys.path.insert(0, _current_dir)

import multiprocessing
if os.name == 'nt':
    multiprocessing.set_start_method('spawn', force=True)

def main():
    setup_path()
    parser = argparse.ArgumentParser(description="類似画像整理スクリプト")
    parser.add_argument("-l", "--level", type=int, choices=range(1, 17), default=1)
    parser.add_argument("-c", "--color-level", type=int, default=10)
    parser.add_argument("-a", "--auto", action="store_true")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-f", "--force-update", action="store_true")
    parser.add_argument("-s", "--sort-size", action="store_true", help="容量が大きいグループ順に表示する")
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
    if not row:
        needs_scan = True

    c.execute(
        "SELECT key, value FROM metadata WHERE key IN ('last_level', 'last_color_level')"
    )
    meta_rows = dict(c.fetchall())
    last_l = int(meta_rows["last_level"]) if "last_level" in meta_rows else -1
    last_c = int(meta_rows["last_color_level"]) if "last_color_level" in meta_rows else -1

    if (last_l != -1 and last_l != args.level) or (last_c != -1 and last_c != args.color_level):
        print("⚠️ 判定レベル変更のためキャッシュをリセットします。")
        c.execute("DELETE FROM similarity_edges")
        c.execute("UPDATE images SET checked = 0")
        conn.commit()

    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_level', ?)", (str(args.level),))
    c.execute("INSERT OR REPLACE INTO metadata VALUES ('last_color_level', ?)", (str(args.color_level),))
    conn.commit()

    image_infos = scan_and_sync_files(args, config, conn, c, needs_scan, target_dirs_paths, today_str)
    image_infos = process_exact_matches(image_infos, args, c, conn)

    if len(image_infos) < 2:
        print("処理する類似画像がありません。")
        return
        
    groups = find_similar_groups(image_infos, args, config, c, conn)

    if not groups:
        print(f"\n合計 0 個の類似グループが見つかりました（処理が必要な画像はありません）。\n")
        return
        
    print(f"\n処理が必要な類似グループが合計 {len(groups)} 個見つかりました。\n")

    # アプリ起動
    app = SimilarImageApp(groups, image_infos, args.auto, args, c, conn)
    app.mainloop()

    # アプリ終了時に、まだ保存されていない残りの結果を反映
    app.apply_pending_actions()

    conn.close()
    logger.info("=== スクリプト完了 ===")
    print("\n🎉 すべての処理が完了しました！")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()