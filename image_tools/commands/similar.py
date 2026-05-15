"""類似画像の検出・整理（GUI / キャッシュ DB はプロジェクト直下）。"""

import os
import argparse
import multiprocessing
import warnings
import math
import datetime
import json
import logging
import psutil
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
import threading
import shutil
import subprocess

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


def parse_indices(text: str, n: int) -> list[tuple[int, bool]]:
    """
    コマンド文字列を解析して (インデックス, @フラグ) のリストを返す。
    n <= 9 の場合、'123' は 1, 2, 3 番目と解釈する。
    """
    results = []
    parts = text.split()
    for part in parts:
        has_at = "@" in part or "+" in part
        nums_str = part.replace("@", "").replace("+", "")
        
        if not nums_str:
            if has_at: # "@" 単体なら 1番目に適用
                results.append((0, True))
            continue

        if n <= 9:
            # 1桁モード: 各文字を個別のインデックスとして扱う
            for char in nums_str:
                if char.isdigit():
                    idx = int(char) - 1
                    if 0 <= idx < n:
                        results.append((idx, has_at))
        else:
            # 通常モード: スペース区切りの数字を扱う
            if nums_str.isdigit():
                idx = int(nums_str) - 1
                if 0 <= idx < n:
                    results.append((idx, has_at))
    return results


def is_excluded_path(p_str: str, exclude_names: set[str], exclude_abs_paths: list[str]) -> bool:
    """パス文字列が除外設定に含まれているか判定"""
    p_norm = p_str.replace('\\', '/')
    # 1. 絶対パスの接頭辞によるチェック
    for ex_p in exclude_abs_paths:
        ex_p_norm = ex_p.replace('\\', '/')
        if p_norm.startswith(ex_p_norm):
            if len(p_norm) == len(ex_p_norm) or p_norm[len(ex_p_norm)] == '/':
                return True
    # 2. フォルダ名によるチェック
    for part in p_norm.split('/'):
        if part in exclude_names:
            return True
    return False


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
            last_action_msg=f"{action_prefix}すべてを残しました (計{n}枚)",
            log_line=f"  -> すべてを残しました: {', '.join(file_names)}",
        )

    if body in ("0", "d"):
        return SelectionResult(
            keep_indices=[],
            at_indices=[],
            last_action_msg="すべて削除予定にしました",
            log_line="  -> すべて削除予定にしました。",
        )

    try:
        indices_info = parse_indices(body, n)
        for idx_val, has_at in indices_info:
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

        kept_info = ", ".join([str(i + 1) for i in keep_indices]) + "番"

        at_text = "[＠マーク] " if at_indices else ""

        return SelectionResult(
            keep_indices=keep_indices,
            at_indices=at_indices,
            last_action_msg=f"{action_prefix}{at_text}残しました -> {kept_info}",
            log_line=f"  -> 指定された画像を残しました: {', '.join([file_names[i] for i in keep_indices])}", # ログには詳細なファイル名を残す
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
            # 解析用の最小サイズにまず落とす (MemoryError 対策)
            img.thumbnail((THUMB_SIZE * 2, THUMB_SIZE * 2))

            # モード変換や回転は縮小後に行う
            if img.mode == 'P' and 'transparency' in img.info:
                img = img.convert('RGBA')
            img = ImageOps.exif_transpose(img)

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
class ImageManagerApp(tk.Tk): # Renamed from SimilarImageApp
    def __init__(self, groups, image_infos, auto_mode=False, args=None, c=None, conn=None, mode=None, num_per_page=None):
        super().__init__()
        self.groups = groups
        self.image_infos = image_infos
        self.auto_mode = auto_mode
        self.args = args
        self.c = c
        # モードの正規化 (s/similar -> similar, p/picker -> picker)
        m = mode if mode is not None else getattr(args, 'mode', 'similar')
        self.mode = "picker" if m.startswith("p") else "similar"
        self.num_per_page = num_per_page if num_per_page is not None else getattr(args, 'num', 4)
        self.conn = conn
        self.current_idx = 0
        
        self.trash_actions = {}
        self.final_kept_paths = set()
        self.protect_actions = {}
        self.at_actions = {}
        self.history_stack = []
        self.current_filtered_infos = []
        self.current_auto_trash = []
        self.last_action_msg = ""
        self.thumbnail_executor = ThreadPoolExecutor(max_workers=4)
        self.exit_requested = False
        self.thumbnail_cache = {}
        self.quit_entire_script = False # New flag to indicate if the entire script should terminate
        self.skip_apply = False # Flag to skip applying actions for the current folder
        self.cache_lock = threading.Lock()
        self.confirm_delete_all = False
        self.immediate_delete = True
        
        self.title("類似画像チェッカー")
        try:
            self.state('zoomed') # Maximize window on Windows
        except:
            # Fallback for other OS or if 'zoomed' state is not supported
            w, h = self.winfo_screenwidth(), self.winfo_screenheight()
            self.geometry(f"{w}x{h}+0+0")
            
        self._setup_ui()
        self.bind_all("<Escape>", lambda e: self.quit())
        self.protocol("WM_DELETE_WINDOW", self.quit)
        
        self.show_current_group()

    def quit(self):
        self.exit_requested = True # Mark that this GUI instance is closing
        self.thumbnail_executor.shutdown(wait=False)
        self.destroy()
        # Do NOT call super().quit() here; self.destroy() is enough to end mainloop

    def _setup_ui(self):
        self.main_frame = ttk.Frame(self)
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        self.image_frame = ttk.Frame(self.main_frame)
        self.image_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        self.bottom_frame = ttk.Frame(self.main_frame)
        self.bottom_frame.pack(fill=tk.X, side=tk.BOTTOM, padx=10, pady=10)

        self.status_label = ttk.Label(self.bottom_frame, text="", font=("Meiryo", 12, "bold"), foreground="blue")
        self.status_label.pack(side=tk.TOP, pady=2)

        # Unified guide text for both modes
        self.guide_label = ttk.Label(self.bottom_frame, text="【入力例】a 全残す / d 全削除 / s 保存 / v1 開く / o1 最適化 / r1 回転(90°) / 🖱️ダブルクリック", font=("Meiryo", 11))
        self.guide_label.pack(side=tk.TOP, pady=2)

        self.entry_var = tk.StringVar()
        # q w @ + b d 0 t は Enter なしで即実行
        self._immediate_cmd_chars = frozenset("qw@+bd0mt")
        self.cmd_entry = ttk.Entry(self.bottom_frame, textvariable=self.entry_var, font=("Meiryo", 16), width=30)
        self.cmd_entry.pack(side=tk.TOP, pady=10)
        self.entry_var.trace_add("write", self._on_entry_write)

        self.bind_all("<Return>", self.on_enter)
        self.bind_all("<Button-1>", lambda e: self.cmd_entry.focus_set())
        
        self.cmd_entry.focus_set()
        
        self.photo_refs = []

    def show_current_group(self):
        while self.current_idx < len(self.groups): # Loop until all groups are processed
            for widget in self.image_frame.winfo_children():
                widget.destroy()
            self.photo_refs.clear()

            group_indices = self.groups[self.current_idx]

            # キャッシュ管理: 現在と次の数グループ以外のキャッシュを破棄
            valid_paths = set()
            for i in range(max(0, self.current_idx - 1), min(len(self.groups), self.current_idx + 3)):
                for idx in self.groups[i]:
                    valid_paths.add(self.image_infos[idx]['path'])
            with self.cache_lock:
                self.thumbnail_cache = {p: img for p, img in self.thumbnail_cache.items() if p in valid_paths}

            current_infos_raw = [self.image_infos[j] for j in group_indices]
            
            if self.mode == "similar":
                group_id = f"Group_{self.current_idx + 1}"
                size_info_str = group_size_info_str(current_infos_raw)
                filtered_infos, auto_trash_paths, max_pixels = filter_similar_group_members(current_infos_raw)

                if len(filtered_infos) == 1:
                    print(f"--- 📁 {group_id}/{len(self.groups)} (Auto Skip | {size_info_str}) ---")
                    self._record_action(self.current_idx, auto_trash_paths, [], [])
                    for info in current_infos_raw: self.final_kept_paths.discard(info['path'])
                    self.final_kept_paths.add(filtered_infos[0]["path"])
                    self.last_action_msg = f"📁 {group_id}: 他は低解像度のため自動で1枚残しました"
                    self.current_idx += 1
                    continue

                if self.auto_mode:
                    print(f"--- 📁 {group_id}/{len(self.groups)} (Auto | {size_info_str}) ---")
                    current_trash = auto_trash_paths + [info["path"] for info in filtered_infos[1:]]
                    self._record_action(self.current_idx, current_trash, [], [])
                    for info in current_infos_raw: self.final_kept_paths.discard(info['path'])
                    self.final_kept_paths.add(filtered_infos[0]["path"])
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
                self._preload_next_groups()
                return

            elif self.mode == "picker":
                # Picker mode logic
                current_infos = current_infos_raw
                size_info_str = f"ページ容量: {format_size(sum(info['filesize'] for info in current_infos))}"
                max_pixels = max(info['pixels'] for info in current_infos) if current_infos else 0

                print(f"\n--- 📑 Page {self.current_idx + 1} / {len(self.groups)} ---")
                for i, info in enumerate(current_infos):
                    p = Path(info["path"])
                    print(f"  [{i+1}] {p.parent.name}/{p.name}")

                self.current_filtered_infos = current_infos # Use this for consistency with _handle_optimize
                self.current_auto_trash = [] # Not applicable in picker mode
                self._populate_image_grid(current_infos, max_pixels, size_info_str)
                return

        self.quit() # Exit if all groups are processed

    def _calc_thumb_dims(self, infos):
        """表示枚数とアスペクト比に基づいた最適なサムネイルサイズと列数を計算"""
        total_imgs = len(infos)
        if total_imgs <= 0: return 150, 150, 1

        avg_aspect = sum(info['aspect_ratio'] for info in infos) / total_imgs if total_imgs > 0 else 1.0

        # 横長画像(Landscape)が多い場合は、列数を2列に絞って大きく表示する
        if avg_aspect > 1.2 and total_imgs >= 2:
            max_cols = 2
        else:
            max_cols = min(self.num_per_page, total_imgs) if self.mode == "picker" else min(4, total_imgs)

        if max_cols < 1: max_cols = 1

        screen_w = self.winfo_screenwidth() - 50
        screen_h = self.winfo_screenheight() - 250
        rows = math.ceil(total_imgs / max_cols)

        return max(150, screen_w // max_cols), max(150, screen_h // rows), max_cols

    def _get_thumbnail_pil(self, path, w, h):
        """画像を読み込み、リサイズ・回転済みのPILオブジェクトを返す（キャッシュ対応）"""
        with self.cache_lock:
            if path in self.thumbnail_cache:
                return self.thumbnail_cache[path]

        try:
            with Image.open(path) as img:
                img.draft("RGB", (w, h))
                
                # draft が効かない形式でも強制的に縮小してから回転させる
                if img.size[0] > w * 2 or img.size[1] > h * 2:
                    img.thumbnail((w * 2, h * 2))

                img.thumbnail((w, h))
                img = ImageOps.exif_transpose(img)
                
                if img.mode == 'P' and 'transparency' in img.info:
                    img = img.convert('RGBA')
                
                with self.cache_lock:
                    self.thumbnail_cache[path] = img
                return img
        except Exception:
            return None

    def _preload_next_groups(self, count=3):
        """未来の数ページ分の画像をバックグラウンドで読み込み、デコードしてキャッシュしておく"""
        idx = self.current_idx + 1
        preloaded = 0
        while idx < len(self.groups) and preloaded < count:
            group_indices = self.groups[idx]
            group_infos = [self.image_infos[j] for j in group_indices]
            
            if self.mode == "similar":
                # 類似画像モードでは、低解像度を除外した後の画像のみを対象にする
                filtered, _, _ = filter_similar_group_members(group_infos)
                if len(filtered) <= 1: # 1枚以下なら自動スキップされるため読み飛ばす
                    idx += 1
                    continue
            else:
                filtered = group_infos

            if filtered:
                w, h, _ = self._calc_thumb_dims(filtered)
                for info in filtered:
                    path = info["path"]
                    with self.cache_lock:
                        if path in self.thumbnail_cache:
                            continue
                    self.thumbnail_executor.submit(self._get_thumbnail_pil, path, w, h)
                preloaded += 1
            idx += 1

    def _populate_image_grid(self, filtered_infos, max_pixels, size_info_str):
        """サムネイルグリッドとステータス行の描画"""
        total_imgs = len(filtered_infos)
        img_w, img_h, max_cols = self._calc_thumb_dims(filtered_infos)
        rows = math.ceil(total_imgs / max_cols) if total_imgs > 0 else 1

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
            p = Path(info["path"])
            ext_lower = p.suffix.lower()
            bg_color = ext_colors.get(ext_lower, "#ffffff")

            frame = tk.Frame(self.image_frame, bd=2, relief=tk.GROOVE, bg=bg_color)
            row_idx = i // max_cols
            col_idx = i % max_cols
            frame.grid(row=row_idx, column=col_idx, sticky="nsew", padx=5, pady=5)
            ext = p.suffix.upper()
            ratio = info["pixels"] / max_pixels
            
            mark = "" # Default mark
            color = "black" # Default color for picker mode (non-protected)
            
            if contains_protect_marker(p.name):
                mark = "[保護済]"
                color = "blue"
            elif self.mode == "similar": # Only apply resolution-based styling in similar mode
                if info["pixels"] == max_pixels:
                    mark = "★最高画質"
                    color = "red"
                elif ratio >= 0.8:
                    mark = "○近い画質"
                    color = "green"
                else:
                    mark = "△低画質"
                    color = "gray"

            folder_and_file = f"{p.parent.name}/{p.name}"
            display_path = (folder_and_file[:40] + '...') if len(folder_and_file) > 43 else folder_and_file

            txt = f"[{i+1}] {display_path}\n{ext} {mark} | {format_pixels(info['pixels'])}px | {format_size(info['filesize'])}"
            lbl = tk.Label(frame, text=txt, font=("Meiryo", 10, "bold"), fg=color, bg=bg_color)
            lbl.pack(side=tk.TOP, pady=5)

            # プレースホルダー表示
            img_lbl = tk.Label(frame, text="Loading...", bg=bg_color)
            img_lbl.pack(side=tk.TOP, expand=True)

            # ダブルクリックで開くイベントをバインド
            path_for_click = info["path"]
            frame.bind("<Double-Button-1>", lambda e, p=path_for_click: self._open_file(e, p))
            lbl.bind("<Double-Button-1>", lambda e, p=path_for_click: self._open_file(e, p))
            img_lbl.bind("<Double-Button-1>", lambda e, p=path_for_click: self._open_file(e, p))
            
            # 非同期で画像を読み込み
            self.thumbnail_executor.submit(self._load_thumbnail_async, info["path"], img_w, img_h, img_lbl, bg_color)

        move_status = "ON" if getattr(self.args, "move", False) else "OFF"
        del_status = "ON" if self.immediate_delete else "OFF"
        status_text = f"Group {self.current_idx + 1} / {len(self.groups)}  [ {size_info_str} ] | [終了時移動: {move_status}] [即時削除: {del_status}]"
        if self.last_action_msg:
            status_text = f"【前回の操作】 {self.last_action_msg}  |  " + status_text

        self.status_label.config(text=status_text)
        self.entry_var.set("")
        self.cmd_entry.focus_set()

    def _load_thumbnail_async(self, path, w, h, label, bg_color):
        """別スレッドで画像を読み込み、PIL画像をメインスレッドに渡す"""
        # メモリ使用率が極端に高い場合は、読み込みを少し遅延させる
        if psutil.virtual_memory().percent > 90:
            time.sleep(1)
            # さらに高い場合はキャッシュを半分捨てる
            if psutil.virtual_memory().percent > 95:
                with self.cache_lock:
                    self.thumbnail_cache.clear()

        img = self._get_thumbnail_pil(path, w, h)
        if img:
            self.after(0, self._update_image_label, label, img)
        else:
            self.after(0, lambda: label.config(text="Load Error", fg="red"))

    def _update_image_label(self, label, pil_img):
        """メインスレッドでPhotoImageを生成してラベルを更新する"""
        if label.winfo_exists():
            try:
                pimg = ImageTk.PhotoImage(pil_img)
                self.photo_refs.append(pimg) # 参照保持
                label.config(image=pimg, text="")
            except Exception:
                label.config(text="UI Error", fg="red")

    def _open_file(self, event, path):
        """画像を関連付けられたアプリケーションで開く"""
        try:
            os.startfile(path)
        except Exception as e:
            print(f"❌ ファイルを開けませんでした ({Path(path).name}): {e}")

    def _optimize_file(self, idx):
        """指定した画像を個別にAVIFへ変換（最適化）する"""
        try:
            if not (0 <= idx < len(self.current_filtered_infos)):
                return

            from image_tools.commands.optimizer import process_single_image, init_worker, CONFIG
            info = self.current_filtered_infos[idx]
            old_path_str = info["path"]
            p = Path(old_path_str)

            # ExifToolの準備（メタデータ保持のため）
            exif_path = CONFIG.get("EXIFTOOL_PATH") or ""
            init_worker(exif_path)

            # 最適化実行
            success, saved, new_path, _ = process_single_image(p)

            if success and new_path:
                new_path_str = str(new_path.resolve())
                
                # ファイル情報を物理ファイルから再取得 (容量表示が変わらない問題を修正)
                new_stat = new_path.stat()
                info["path"] = new_path_str
                info["filesize"] = new_stat.st_size
                info["mtime"] = new_stat.st_mtime
                
                # DBの情報を更新
                if self.c:
                    if new_path_str != old_path_str:
                        self.c.execute("DELETE FROM images WHERE path = ?", (new_path_str,))
                    self.c.execute(
                        "UPDATE images SET path = ?, filesize = ?, mtime = ? WHERE path = ?",
                        (new_path_str, info["filesize"], info["mtime"], old_path_str)
                    )
                    self.conn.commit()

                # サムネイルキャッシュを削除して再生成を促す
                with self.cache_lock:
                    self.thumbnail_cache.pop(old_path_str, None)

                self.last_action_msg = f"✨ 最適化完了: {p.name} ({format_size(saved)} 削減)"
            else:
                self.last_action_msg = f"ℹ️ 最適化不要: 容量が変わらないためスキップしました ({p.name})"

            self.show_current_group()
        except Exception as e:
            print(f"❌ 最適化エラー詳細: {e}")
            self.last_action_msg = f"❌ 最適化エラー: {e}"
            self.show_current_group()

    def _rotate_file(self, idx):
        """指定した画像の Orientation メタデータを更新して、画質を劣化させずに 90度回転させる"""
        try:
            if not (0 <= idx < len(self.current_filtered_infos)):
                return

            info = self.current_filtered_infos[idx]
            path_str = info["path"]
            p = Path(path_str)

            # ExifTool のパスを取得
            import image_tools.settings as app_settings
            settings = app_settings.load_settings()
            exiftool_path = settings.get("EXIFTOOL_PATH") or "exiftool"

            # 現在の Orientation を取得
            # 1: Normal, 6: 90 CW, 3: 180, 8: 270 CW
            res = subprocess.run(
                [exiftool_path, "-Orientation", "-n", "-s3", path_str],
                capture_output=True, text=True, check=False
            )
            curr_val = res.stdout.strip()
            curr = int(curr_val) if curr_val.isdigit() else 1
            
            # 90度ずつ右回転させるサイクル (1 -> 6 -> 3 -> 8 -> 1)
            next_orient = {1: 6, 6: 3, 3: 8, 8: 1}.get(curr, 6)

            # メタデータのみを書き換え (ロスレス回転)
            subprocess.run(
                [exiftool_path, f"-Orientation={next_orient}", "-n", "-overwrite_original", path_str],
                check=True, capture_output=True
            )

            # サムネイルキャッシュを削除して再描画を促す
            with self.cache_lock:
                self.thumbnail_cache.pop(path_str, None)
            
            # 縦横比情報を更新 (90度回転なので反転させる)
            if info["aspect_ratio"] > 0:
                info["aspect_ratio"] = 1.0 / info["aspect_ratio"]

            self.last_action_msg = f"🔄 ロスレス回転完了 (90°CW): {p.name}"
            self.show_current_group()
        except Exception as e:
            self.last_action_msg = f"❌ 回転エラー: {e}"
            self.show_current_group()

    def _record_action(self, idx, trash, protect, at):
        self.trash_actions[idx] = trash
        self.protect_actions[idx] = protect
        self.at_actions[idx] = at

    def _on_entry_write(self, *args):
        s = self.entry_var.get().strip().lower()
        if len(s) != 1 or s not in self._immediate_cmd_chars:
            return

        # d, 0 の場合は即時削除設定を確認
        if s in "d0" and not self.immediate_delete:
            return

        def _try_immediate():
            s2 = self.entry_var.get().strip().lower()
            if len(s2) != 1 or s2 not in self._immediate_cmd_chars:
                return
            self._apply_command(s2)

        self.after_idle(_try_immediate)

    def on_enter(self, event):
        # PickerApp のデフォルト動作を統合
        if not self.entry_var.get().strip() and self.mode == "picker":
            self.entry_var.set("a") # Default to 'a' (all keep) for picker if empty

        ans = self.entry_var.get().strip()
        self._apply_command(ans)

    def _apply_command(self, ans):
        ans_lower = ans.lower()

        # 閲覧コマンド (v1, v2...)
        if ans_lower.startswith('v'):
            try:
                val = int(ans_lower[1:]) - 1
                if 0 <= val < len(self.current_filtered_infos):
                    self._open_file(None, self.current_filtered_infos[val]["path"])
                    self.entry_var.set("") # 入力欄をクリア
                    return
            except ValueError:
                pass

        # 最適化コマンド (o1, o2...)
        if ans_lower.startswith('o'):
            try:
                # o1 2 3 のように複数指定可能にする
                parts = ans_lower[1:].split()
                if parts:
                    for part in parts:
                        self._optimize_file(int(part) - 1)
                self.entry_var.set("")
                return
            except ValueError:
                pass

        # 回転コマンド (r1, r2...)
        if ans_lower.startswith('r'):
            try:
                parts = ans_lower[1:].split()
                if not parts: parts = ["1"]
                for part in parts:
                    self._rotate_file(int(part) - 1)
                self.entry_var.set("")
                return
            except ValueError:
                pass

        if ans_lower == 'w':
            print("🛑 プログラム全体を終了します。これまでの判定を反映します。")
            self.quit_entire_script = True
            self.quit()
            return

        ans = ans.lower()
        if ans == 'q':
            print("🛑 現在のフォルダをスキップし、次のフォルダへ進みます（未保存の判定は破棄されます）。")
            self.skip_apply = True
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
                prev_idx = self.history_stack.pop()
                self.current_idx = prev_idx
                # 巻き戻したページの未確定アクションを削除
                self.trash_actions.pop(prev_idx, None)
                self.protect_actions.pop(prev_idx, None)
                self.at_actions.pop(prev_idx, None)
                self.last_action_msg = f"[⏪ 戻る] Group {self.current_idx + 1} をやり直します"
                self.show_current_group()
            return

        if ans == 'm':
            if self.args:
                self.args.move = not getattr(self.args, "move", False)
                status = "ON" if self.args.move else "OFF"
                self.last_action_msg = f"🚚 終了時のフォルダ移動を {status} に切り替えました"
            self.show_current_group()
            return

        if ans == 't':
            self.immediate_delete = not self.immediate_delete
            status = "ON" if self.immediate_delete else "OFF"
            self.last_action_msg = f"⚡ 削除(d/0)の即時実行を {status} に切り替えました"
            self.show_current_group()
            return

        cmd = normalize_selection_command(ans)
        file_names = [Path(self.current_filtered_infos[i]["path"]).name for i in range(len(self.current_filtered_infos))]
        result = compute_selection_indices(cmd, file_names)
        if result.log_line:
            print(result.log_line)

        self.confirm_delete_all = False
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

        # 維持したパスを追跡 (グループ内の全ファイルを一度除外し、残すものだけ入れる)
        for info in self.current_filtered_infos: self.final_kept_paths.discard(info['path'])
        for p in self.current_auto_trash: self.final_kept_paths.discard(p)
        for i in keep_indices: self.final_kept_paths.add(self.current_filtered_infos[i]['path'])

        self._record_action(self.current_idx, current_trash, current_protect, current_at)
        self.history_stack.append(self.current_idx)
        self.current_idx += 1
        self.show_current_group()

    def apply_pending_actions(self):
        if self.skip_apply:
            # スキップ時も、それまでに Enter で確定させた分は反映するように変更
            if self.trash_actions or self.protect_actions or self.at_actions:
                print("ℹ️ フォルダスキップが選択されました。確定済みの判定のみ反映します。")

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
                    if flags["protect"] and not contains_protect_marker(new_stem):
                        if new_stem.endswith("@"):
                            new_stem = new_stem[:-1] + "_protect@"
                        else:
                            new_stem += "_protect"
                    
                    # @付与: 末尾になく、かつ名前のどこにも@が含まれていない場合のみ追加
                    if flags["at"] and not new_stem.endswith("@") and "@" not in new_stem:
                        new_stem += "@"
                    
                    if new_stem != old_path.stem:
                        new_path = old_path.with_name(f"{new_stem}{old_path.suffix}")
                        if not self.args.dry_run:
                            try:
                                old_path.rename(new_path)
                                new_p_str = str(new_path)
                                if p_str in self.final_kept_paths:
                                    self.final_kept_paths.discard(p_str)
                                    self.final_kept_paths.add(new_p_str)
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

                # 空になったフォルダの整理
                trash_parents = {Path(p).parent for p in all_trash}
                for parent in trash_parents:
                    cleanup_empty_folders(parent, delete_root=True)

        # 反映済みのタスクと履歴をリセット
        self.protect_actions.clear()
        self.at_actions.clear()
        self.trash_actions.clear()
        self.history_stack.clear()


# ==========================================
# フェーズ分割された関数群
# ==========================================
def scan_and_sync_files(args, config, conn, c, needs_scan, target_dirs_paths, today_str):
    """DBとファイルシステムの同期を行い、有効な画像情報のリストを返す"""
    exclude_names = set(config.get("EXCLUDE_DIR_NAMES", []))
    exclude_abs_paths = []
    for d in config.get("EXCLUDE_DIR_NAMES", []):
        try:
            p = Path(d).resolve()
            exclude_abs_paths.append(str(p))
        except: pass

    def _is_ex(p):
        return is_excluded_path(p, exclude_names, exclude_abs_paths)

    image_infos = []

    if not needs_scan:
        print("⏭️ ファイルスキャンをスキップし、既存のデータベースから読み込みます（-f で再スキャン可能）。")
        c.execute('SELECT path, hash_str, color_hash_str, pixels, filesize, aspect_ratio FROM images')
        db_rows = c.fetchall()
        print(f"🗄️ データベースから {len(db_rows)} 件の画像情報を読み込み中...")
        for r in db_rows:
            p_str = r[0]
            if not _is_ex(p_str):
                image_infos.append({'path': p_str, 'hash_str': r[1], 'color_hash_str': r[2], 'pixels': r[3], 'filesize': r[4], 'aspect_ratio': r[5]})
        print("  ✅ 読み込み完了")
        return image_infos

    print(f"️ データベースと実際のファイル状況を同期しています...")
    c.execute('SELECT path, mtime, filesize, hash_str, color_hash_str, pixels, aspect_ratio FROM images')
    db_cache = {row[0]: row for row in c.fetchall()}
    db_dir_map = defaultdict(list)
    for p_str, r in db_cache.items():
        db_dir_map[os.path.dirname(p_str)].append(r)

    c.execute("SELECT path, mtime FROM folder_mtimes")
    db_folder_mtimes = dict(c.fetchall())
    
    to_compute = []
    valid_paths = set()
    new_folder_mtimes = []

    exclude_keywords = config["EXCLUDE_FILE_KEYWORDS"]
    
    print(f"🔍 画像ファイルを検索中...")
    for t_dir in target_dirs_paths:
        if not t_dir.is_dir(): continue
        for root, dirs, files in os.walk(str(t_dir)):
            # 除外設定にあるディレクトリは枝切り（スキップ）して高速化する
            dirs[:] = [d for d in dirs if d not in exclude_names and not _is_ex(os.path.join(root, d))]
            
            root_str = root
            try:
                current_mtime = os.path.getmtime(root)
            except OSError: continue

            if db_folder_mtimes.get(root_str) == current_mtime and root_str in db_dir_map:
                # フォルダの更新時刻が変わっていない場合は、DBの内容をそのまま信頼して利用
                # 枝切りにより走査対象の root は必ず非除外パスとなるため、個別の除外チェックは不要
                for r in db_dir_map[root_str]:
                    p_str = r[0]
                    file_name = os.path.basename(p_str)
                    if any(kw in file_name for kw in exclude_keywords): continue
                    valid_paths.add(p_str)
                    image_infos.append({'path': p_str, 'hash_str': r[3], 'color_hash_str': r[4], 'pixels': r[5], 'filesize': r[2], 'aspect_ratio': r[6]})
            else:
                # フォルダが更新されているか未登録の場合は、中のファイルを個別にチェック
                for f in files:
                    ext = os.path.splitext(f)[1].lower()
                    if ext not in SUPPORTED_EXTS: continue
                    if any(kw in f for kw in exclude_keywords): continue
                    
                    p_str = os.path.join(root, f)
                    try:
                        stat_res = os.stat(p_str)
                        mtime, fsize = stat_res.st_mtime, stat_res.st_size
                        if not os.access(p_str, os.W_OK): continue
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

    # 今回のスキャン対象フォルダの配下にあるパスのみ、実体がない場合にDBから削除する
    target_prefixes = []
    for d in target_dirs_paths:
        p = str(d.resolve())
        target_prefixes.append(p if p.endswith(os.sep) else p + os.sep)

    db_delete = [p_str for p_str in db_cache if p_str not in valid_paths and any(p_str.startswith(pre) for pre in target_prefixes)]
    if db_delete: 
        delete_db_records(c, db_delete)
        conn.commit()

    if to_compute:
        print(f"🚀 {len(to_compute)} 個の新規/更新ファイルを解析して保存します...")
        batch = []
        args_list = [(p, config["SOLID_TOLERANCE"]) for p in to_compute]
        
        # メモリ不足対策: ワーカー数を CPU コア数にかかわらず最大 8 に制限
        n_workers = min(os.cpu_count() or 3, 8)
        
        chunk = max(32, min(256, len(args_list) // (n_workers * 4) or 32))
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_workers) as executor:
            results = executor.map(compute_image_info, args_list, chunksize=chunk)
            for path_str, res in tqdm(results, total=len(args_list), desc="⏳ 画像解析"):
                # 解析ループ内でのメモリ監視
                if psutil.virtual_memory().percent > 90:
                    time.sleep(0.5)

                if res:
                    batch.append((*res, 0))
                    if not _is_ex(path_str):
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

        # 空になったフォルダの整理
        trash_parents = {Path(p).parent for p in exact_trash}
        for parent in trash_parents:
            cleanup_empty_folders(parent, delete_root=True)
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


def get_sub_targets(args, target_dirs_paths):
    """処理対象とするサブディレクトリのリストを取得する"""
    if not args.each:
        return target_dirs_paths
    
    sub_targets = []
    for d in target_dirs_paths:
        dirs = [Path(e.path) for e in os.scandir(d) if e.is_dir()]
        sub_targets.extend(sorted(dirs))
    return sub_targets


def run_image_manager_gui(groups, image_infos, args, c, conn, mode=None, num_per_page=None):
    """GUIを起動し、結果を適用する共通ラッパー"""
    app = ImageManagerApp(
        groups, image_infos, 
        auto_mode=args.auto, args=args, c=c, conn=conn, 
        mode=mode, num_per_page=num_per_page
    )
    app.mainloop()
    app.apply_pending_actions()
    return app


def execute_picker_workflow(current_all_infos, args, c, conn, target_prefix):
    """Pickerモードの一連の処理（選別・移動）を実行する"""
    # 保護済み画像 (_protect) を抽出
    protected_paths = [info['path'] for info in current_all_infos if contains_protect_marker(Path(info["path"]).name)]
    picker_infos = [info for info in current_all_infos if not contains_protect_marker(Path(info["path"]).name)]
    
    # 拡張子フィルタリング
    if args.ext:
        target_exts = {("." + e.lower().lstrip(".")) for e in args.ext}
        picker_infos = [info for info in picker_infos if Path(info["path"]).suffix.lower() in target_exts]
        protected_paths = [p for p in protected_paths if Path(p).suffix.lower() in target_exts]

    folder_kept_paths = set(protected_paths)

    if picker_infos:
        # 縦横比に基づいてグループ化（縦長・スクエア: <= 1.1, 横長: > 1.1）
        # ページ内で向きを揃えることで、表示サイズが小さくなるのを防ぐ
        portraits = [info for info in picker_infos if info.get('aspect_ratio', 1.0) <= 1.1]
        landscapes = [info for info in picker_infos if info.get('aspect_ratio', 1.0) > 1.1]

        if args.sort_size:
            portraits.sort(key=lambda x: x['filesize'], reverse=True)
            landscapes.sort(key=lambda x: x['filesize'], reverse=True)
        else:
            portraits.sort(key=lambda x: x['path'])
            landscapes.sort(key=lambda x: x['path'])

        # 向きを揃えた状態でリストを再構成
        picker_infos = portraits + landscapes
        
        groups = []
        # 縦長グループのページ分割
        p_len = len(portraits)
        for i in range(0, p_len, args.num):
            groups.append(list(range(i, min(i + args.num, p_len))))
            
        # 横長グループのページ分割（インデックスをオフセットして開始）
        l_len = len(landscapes)
        for i in range(0, l_len, args.num):
            groups.append(list(range(p_len + i, p_len + min(i + args.num, l_len))))

        print(f"🚀 選別開始: {len(picker_infos)} 枚 ({len(groups)} ページ)")

        app = run_image_manager_gui(groups, picker_infos, args, c, conn, mode="picker", num_per_page=args.num)
        
        # q を押してスキップした場合も、そこまでに「残す」と決めた分（完了ページ分）は反映させる
        folder_kept_paths.update(app.final_kept_paths)

        # 個別ファイルの移動処理 (中断(w)やスキップ(q)時も、そこまでの確定分は移動させる)
        if args.move:
            move_kept_files(folder_kept_paths, args, c, conn)

        if app.quit_entire_script:
            return True # スクリプト全体終了フラグ

    return False


def move_kept_files(folder_kept_paths, args, c, conn):
    """選別で残ったファイルを指定の移動先へ送る"""
    import image_tools.settings as app_settings
    s = app_settings.load_settings()
    dest = s.get("PICKER_MOVE_DEST")
    if not dest or not folder_kept_paths:
        return

    base_save = s.get("BASE_SAVE_DIR")
    dest_root = Path(dest)
    
    config = load_config()
    target_dirs_paths = [Path(d).resolve() for d in config["TARGET_DIRS"] if d.strip()]

    for p_str in list(folder_kept_paths):
        src_p = Path(p_str).resolve()
        if not src_p.exists():
            continue

        # src_p が TARGET_DIRS のいずれかの配下にあるかチェック
        found_root = None
        for d in target_dirs_paths:
            try:
                src_p.relative_to(d) # src_pがdの配下にあるかチェック
                found_root = d
                break
            except ValueError:
                continue

        if not found_root:
            print(f"  ⏭️ 移動スキップ(対象ルートフォルダ外): {src_p.name}")
            continue
        
        # 移動先のパスを構築: PICKER_MOVE_DEST / 直上のフォルダ名 / ファイル名
        target_p = dest_root / src_p.parent.name / src_p.name

        if target_p.exists():
            print(f"  ℹ️ 移動スキップ(既に存在): {src_p.name} -> {target_p}")
            continue

        target_p.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src_p), str(target_p))
            new_p_str = str(target_p.resolve())
            c.execute("UPDATE images SET path = ? WHERE path = ?", (new_p_str, p_str))
        except Exception as e:
            print(f"  ❌ 移動失敗: {src_p.name} -> {e}")

    conn.commit()


# ==========================================
# メインプロセス
# ==========================================
def cleanup_empty_folders(target_path: Path, delete_root: bool = False):
    """指定フォルダ内の空フォルダを再帰的にチェックして削除（ゴミ箱へ）"""
    if not target_path.exists() or not target_path.is_dir():
        return
    target_path_abs = target_path.resolve()
    for root, dirs, files in os.walk(str(target_path), topdown=False):
        curr_path = Path(root).resolve()
        if curr_path == target_path_abs and not delete_root:
            continue
        if not os.listdir(root):
            try:
                send2trash(root)
                print(f"  🗑️ 空フォルダを整理しました: {os.path.basename(root)}")
            except Exception:
                pass

def setup_path():
    """インポートパスの解決"""
    _current_dir = str(Path(__file__).resolve().parent)
    if _current_dir not in sys.path:
        sys.path.insert(0, _current_dir)

def main():
    setup_path()
    parser = argparse.ArgumentParser(description="画像整理スクリプト (類似画像検出 / 順次選別)")
    parser.add_argument("-m", "--mode", choices=["similar", "picker", "s", "p"], default="similar",
                        help="モード: similar(s) または picker(p)")
    parser.add_argument("-n", "--num", type=int, default=4,
                        help="Pickerモードで1ページに表示する枚数")
    parser.add_argument("-P", "--then-picker", action="store_true",
                        help="類似画像整理(similar)の完了後、続けて全画像の選別(picker)を実行する")
    parser.add_argument("-E", "--each", action="store_true",
                        help="指定フォルダのサブフォルダごとに一連の処理を実行する")
    parser.add_argument("root_dir", type=str, nargs="?", help="対象フォルダ (省略時は config.json の TARGET_DIRS)")
    parser.add_argument("-l", "--level", type=int, choices=range(1, 17), default=1)
    parser.add_argument("-c", "--color-level", type=int, default=10)
    parser.add_argument("-a", "--auto", action="store_true")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-f", "--force-update", action="store_true")
    parser.add_argument("-s", "--sort-size", action="store_true", help="容量が大きいグループ順に表示する")
    parser.add_argument("--ext", nargs="+",
                        help="Pickerモードで対象とする拡張子 (例: --ext jpg png)")
    parser.add_argument("--no-move", action="store_false", dest="move", default=True, help="選別終了後、対象フォルダの移動をスキップする")
    args = parser.parse_args()

    config = load_config()
    if args.root_dir:
        target_dirs_paths = [Path(args.root_dir).resolve()]
    else:
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

    # 指定された対象ディレクトリ配下のファイルのみに絞り込む (高速な文字列接頭辞比較)
    target_prefixes = []
    for d in target_dirs_paths:
        prefix = str(d.resolve())
        if not prefix.endswith(os.sep):
            prefix += os.sep
        target_prefixes.append(prefix)

    print(f"🧹 対象フォルダ配下の画像のみを抽出中...")
    image_infos = [info for info in image_infos if any(info["path"].startswith(p) for p in target_prefixes)]
    print(f"  ✅ {len(image_infos)} 枚を抽出しました。")

    # モードが similar の場合、まず全体で完全一致（STEP 1）判定を行う
    if args.mode.lower().startswith("s"):
        image_infos = process_exact_matches(image_infos, args, c, conn)

    if args.each and args.mode.lower().startswith("s"):
        print(f"\n🌲 【STEP 2-E】全体での類似画像検索を開始します...")
        global_groups = find_similar_groups(image_infos, args, config, c, conn)
        if global_groups:
            print(f"🔍 全体で {len(global_groups)} 個の類似グループが見つかりました。")
            app = run_image_manager_gui(global_groups, image_infos, args, c, conn)
            if app.quit_entire_script: # Check for full script exit
                conn.close()
                sys.exit(0)
            
            # DBから最新の状態を読み込み直す（削除やリネーム反映のため）
            c.execute('SELECT path, hash_str, color_hash_str, pixels, filesize, aspect_ratio FROM images')
            rows = c.fetchall()
            image_infos = [
                {'path': r[0], 'hash_str': r[1], 'color_hash_str': r[2], 'pixels': r[3], 'filesize': r[4], 'aspect_ratio': r[5]}
                for r in rows if any(r[0].startswith(p) for p in target_prefixes)
            ]
        else:
            print("ℹ️ 全体で類似グループは見つかりませんでした。")
        
        # 全体検索が終わったので、以降の各フォルダ処理は選別(picker)に切り替える
        args.mode = "picker"

    sub_targets = get_sub_targets(args, target_dirs_paths)
    if args.each: print(f"📁 {len(sub_targets)} 個のサブフォルダを順次処理します。")

    # --- フォルダごとのメインループ ---
    for current_target in sub_targets:
        app = None  # フォルダごとにリセットし、前フォルダの終了リクエストを引き継がないようにする
        target_prefix = str(current_target.resolve())
        if not target_prefix.endswith(os.sep): target_prefix += os.sep
        
        # このフォルダに属する画像を抽出
        current_all_infos = [info for info in image_infos if info['path'].startswith(target_prefix)]
        if not current_all_infos:
            continue

        print(f"\n📂 ==========================================")
        print(f"📂 フォルダ処理開始: {current_target.name} ({len(current_all_infos)}枚)")
        print(f"📂 ==========================================")

        current_mode = args.mode.lower()

        # --- フェーズ1: 類似画像検出 ---
        if current_mode.startswith("s"):
            groups = find_similar_groups(current_all_infos, args, config, c, conn) if len(current_all_infos) >= 2 else []

            if groups:
                print(f"🔍 類似グループ: {len(groups)} 個")
                app = run_image_manager_gui(groups, current_all_infos, args, c, conn)
                if app.quit_entire_script:
                    break
                if app.skip_apply: continue
            elif len(current_all_infos) < 2:
                print("ℹ️ 類似判定に必要な枚数がありません。")
            else:
                print("ℹ️ 類似グループは見つかりませんでした。")
            
            if args.then_picker:
                print("🔄 続けて選別(picker)モードに移行...")
                # 最新のDB状態から画像リストを再構築（リネームや削除を反映）
                c.execute('SELECT path, hash_str, color_hash_str, pixels, filesize, aspect_ratio FROM images')
                rows = c.fetchall()
                current_all_infos = [
                    {'path': r[0], 'hash_str': r[1], 'color_hash_str': r[2], 'pixels': r[3], 'filesize': r[4], 'aspect_ratio': r[5]}
                    for r in rows if r[0].startswith(target_prefix)
                ]
                current_mode = "picker"

        # --- フェーズ2: 順次選別 (Picker) ---
        if current_mode.startswith("p"):
            is_quit = execute_picker_workflow(current_all_infos, args, c, conn, target_prefix)
            if is_quit: break

        cleanup_empty_folders(current_target, delete_root=args.each)

    conn.close()

if __name__ == "__main__":
    import multiprocessing
    if os.name == 'nt':
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            # すでに設定済みの場合は無視
            pass
    multiprocessing.freeze_support() # Added for Windows compatibility
    main()