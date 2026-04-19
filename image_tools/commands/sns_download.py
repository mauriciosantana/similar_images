import subprocess
import sys
import time
import os
import shutil
import json
import glob
import concurrent.futures
import re
import threading
import queue
import argparse

try:
    import msvcrt
except ImportError:
    msvcrt = None

from image_tools.paths import PROJECT_ROOT
from image_tools import settings as app_settings
from image_tools.settings import require_setting_str

_S = app_settings.load_settings()
_BASE = _S.get("BASE_SAVE_DIR") or ""
BASE_SAVE_DIR = _BASE
IMAGE_SAVE_DIR = os.path.join(_BASE, "SNS画像") if _BASE else ""
VIDEO_SAVE_DIR = os.path.join(_BASE, "SNS動画") if _BASE else ""
NOJSON_SAVE_DIR = os.path.join(_BASE, "nojson") if _BASE else ""
METADATA_DIR_NAME = "metadata"
EXIFTOOL_PATH = _S.get("EXIFTOOL_PATH") or ""

ARCHIVE_FILE = str(PROJECT_ROOT / "download_history.sqlite3")
COOKIES_FILE = str(PROJECT_ROOT / "cookies.txt")
COMPLETED_FILE = str(PROJECT_ROOT / "completed_accounts.txt")
TARGETS_FILE = str(PROJECT_ROOT / "targets.txt")
NAME_CACHE_FILE = str(PROJECT_ROOT / "account_names.json")

FAKE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"

# ダウンロードするメディアの最小サイズ (0の場合は制限なし)
MIN_MEDIA_WIDTH = 300   
MIN_MEDIA_HEIGHT = 300  

# プラットフォームごとの設定定義
PLATFORM_CONFIG = {
    "instagram": {
        "url_template": "https://www.instagram.com/{}/",
        "prefix": "ig",
        "args": ["--max-downloads", "50", "--user-agent", FAKE_USER_AGENT]
    },
    "pixiv": {
        "url_template": "https://www.pixiv.net/users/{}",
        "prefix": "px",
        "args": ["-o", "pixiv:ugoira-conv=mp4", "-o", "pixiv:include=illust,manga,ugoira"]
    },
    "twitter": {
        "url_template": "https://x.com/{}/media",
        "prefix": "tw",
        "args": ["-o", "twitter:include-retweets=false", "--user-agent", FAKE_USER_AGENT]
    },
    "twitter_hashtag": {
        "url_template": "https://x.com/hashtag/{}",
        "prefix": "twtag",
        "args": ["-o", "twitter:include-retweets=false", "--user-agent", FAKE_USER_AGENT]
    }
}

SUCCESS_INJECTED = set()

# --------------------------------------------------------
# ★ 高速化用オブジェクト (正規表現, JSONキャッシュ, ExifTool常駐)
# --------------------------------------------------------
# 1. 正規表現の事前コンパイル
PATTERN_FILENAME_1 = re.compile(r"^(tw|twtag|ig|px)_(.+?)_\d{8}_\d{6}_")
PATTERN_FILENAME_2 = re.compile(r"^(tw|twtag|ig|px)_(.+)_[^_]+\.[a-zA-Z0-9]+$")
PATTERN_INVALID_CHARS = re.compile(r'[\\/:*?"<>|]')
PATTERN_TARGET_COMMENT = re.compile(r"^(twitter|instagram|pixiv|twtag)\s*:", re.IGNORECASE)

# 3. JSONパースのキャッシュ化
JSON_CACHE = {}
JSON_CACHE_LOCK = threading.Lock()

def load_cached_json(filepath):
    with JSON_CACHE_LOCK:
        if filepath in JSON_CACHE:
            return JSON_CACHE[filepath]
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            with JSON_CACHE_LOCK:
                JSON_CACHE[filepath] = data
            return data
    except Exception:
        return None

# 2. ExifToolの起動オーバーヘッド削減(常駐化クラス)
class FastExifTool:
    def __init__(self, executable):
        self.executable = executable
        self.process = None
        self.lock = threading.Lock()

    def start(self):
        if not os.path.exists(self.executable):
            return
        # Windows環境で裏コマンド実行時の黒窓ポップアップを防ぐ
        creationflags = 0x08000000 if os.name == 'nt' else 0
        self.process = subprocess.Popen(
            [self.executable, "-stay_open", "True", "-@", "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", bufsize=1, creationflags=creationflags
        )

    def execute(self, *args):
        with self.lock:
            if not self.process or self.process.poll() is not None:
                self.start()
            if not self.process:
                return False, "ExifToolが見つかりません"
            
            try:
                for arg in args:
                    self.process.stdin.write(arg + "\n")
                self.process.stdin.write("-execute\n")
                self.process.stdin.flush()
                
                output = ""
                while True:
                    line = self.process.stdout.readline()
                    if not line: break
                    if line.strip() == "{ready}": break
                    output += line
                
                is_success = "files updated" in output or "files created" in output or "image files read" in output
                return is_success, output
            except Exception as e:
                return False, str(e)

    def stop(self):
        if self.process:
            try:
                self.process.stdin.write("-stay_open\nFalse\n")
                self.process.stdin.flush()
                self.process.wait(timeout=3)
            except Exception:
                self.process.kill()
            self.process = None

FAST_EXIFTOOL = FastExifTool(EXIFTOOL_PATH)

# --------------------------------------------------------
# ★ ファイル・文字コード対応読み込み関数
# --------------------------------------------------------
def read_text_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            return f.readlines()
    except UnicodeDecodeError:
        with open(filepath, "r", encoding="cp932") as f:
            return f.readlines()

def load_name_cache():
    if os.path.exists(NAME_CACHE_FILE):
        meta = load_cached_json(NAME_CACHE_FILE)
        if meta: return meta
    return {}

def save_name_cache(cache):
    try:
        with open(NAME_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=4)
        with JSON_CACHE_LOCK:
            JSON_CACHE[NAME_CACHE_FILE] = cache
    except Exception:
        pass

def extract_name_from_meta(platform, meta):
    if platform == "twitter_hashtag":
        return None
        
    if isinstance(meta, list):
        if len(meta) >= 2 and isinstance(meta[1], dict):
            meta = meta[1]
        elif len(meta) >= 1 and isinstance(meta[0], dict):
            meta = meta[0]
        else:
            return None
            
    if not isinstance(meta, dict):
        return None

    name = None
    if platform == "twitter":
        # ハンドル名(nick)より表示名(name)を優先して取得
        user_info = meta.get("user", {})
        author_info = meta.get("author", {})
        name = author_info.get("name") or user_info.get("name") or author_info.get("nick") or user_info.get("nick")
    elif platform == "pixiv":
        # Pixiv: 多様なメタデータ構造から名前を抽出
        user = meta.get("user") or {}
        author = meta.get("author") or {}
        body = meta.get("body") or {}
        
        name = (meta.get("userName") or 
                (user.get("name") if isinstance(user, dict) else None) or 
                (body.get("userName") if isinstance(body, dict) else None) or 
                (author.get("name") if isinstance(author, dict) else author if isinstance(author, str) else None) or
                (author.get("nick") if isinstance(author, dict) else None))
    elif platform == "instagram":
        name = meta.get("owner", {}).get("full_name") or meta.get("user", {}).get("full_name") or meta.get("username")
    
    return name.strip() if name and isinstance(name, str) else name

def get_account_name(platform, prefix, account_id):
    if platform == "twitter_hashtag":
        return f"#{account_id}"
        
    cache = load_name_cache()
    # IDの大小文字による不一致を防ぐためキーを小文字化
    cache_key = f"{platform}:{account_id.lower()}"
    
    if cache_key in cache:
        return cache[cache_key]

    search_dirs = [os.path.join(BASE_SAVE_DIR, METADATA_DIR_NAME), BASE_SAVE_DIR]
    for directory in search_dirs:
        pattern = os.path.join(directory, f"{prefix}_{account_id}_*.json")
        for local_file in glob.glob(pattern):
            try:
                meta = load_cached_json(local_file)
                name = extract_name_from_meta(platform, meta)
                if name:
                    cache[cache_key] = name
                    save_name_cache(cache)
                    return name
            except Exception:
                continue
    return None

# --------------------------------------------------------
# ★ ファイル・設定読み込み関数
# --------------------------------------------------------
def load_targets():
    if not os.path.exists(TARGETS_FILE):
        with open(TARGETS_FILE, "w", encoding="utf-8") as f:
            f.write('twitter:"ek"\n\ninstagram:"z"\n\npixiv:"11"\n')
        print(f"⚠️ {TARGETS_FILE} が無かったため、ひな形を作成しました!")
        sys.exit(0)
        
    targets = {"twitter": [], "twitter_hashtag": [], "instagram": [], "pixiv": []}
    
    lines = read_text_file(TARGETS_FILE)
    for line in (l.strip() for l in lines):
        if not line or line.startswith("#"): 
            continue
        
        parts = line.split(":", 1)
        if len(parts) == 2:
            platform = parts[0].strip().lower()
            
            tokens = parts[1].strip().split()
            if not tokens: continue
            
            account_id = tokens[0]
            keywords = tokens[1:]
            
            if account_id.startswith('"') and account_id.endswith('"'):
                account_id = account_id[1:-1]
            
            if platform in targets and account_id:
                targets[platform].append({"id": account_id, "keywords": keywords})
                
    return targets

def load_completed_list():
    if not os.path.exists(COMPLETED_FILE): 
        return set()
    lines = read_text_file(COMPLETED_FILE)
    return set(line.strip() for line in lines if line.strip())

def mark_as_completed(identifier):
    with open(COMPLETED_FILE, "a", encoding="utf-8") as f:
        f.write(f"{identifier}\n")

# --------------------------------------------------------
# ★ フォルダ振り分けルールの生成・処理関数
# --------------------------------------------------------
def get_folder_mapping(targets_file):
    mapping = {}
    if not os.path.exists(targets_file):
        return mapping
        
    lines = read_text_file(targets_file)
        
    groups = []
    temp_group = []
    temp_comment = ""
    
    for line in lines:
        line = line.strip()
        if not line:
            if temp_group:
                groups.append((temp_comment, temp_group))
                temp_group = []
            temp_comment = ""
            continue
            
        if line.startswith("#"):
            if temp_group:
                groups.append((temp_comment, temp_group))
                temp_group = []
            c = line.lstrip("#").strip()
            if PATTERN_TARGET_COMMENT.match(c):
                continue
            if c:
                temp_comment = c
        else:
            parts = line.split(":", 1)
            if len(parts) == 2:
                platform = parts[0].strip().lower()
                
                tokens = parts[1].strip().split()
                if not tokens: continue
                account_id = tokens[0]
                
                if account_id.startswith('"') and account_id.endswith('"'):
                    account_id = account_id[1:-1]
                
                account_id = account_id.lower()
                
                prefix = ""
                if platform == "twitter": prefix = "tw"
                elif platform == "twitter_hashtag": prefix = "twtag"
                elif platform == "instagram": prefix = "ig"
                elif platform == "pixiv": prefix = "px"
                
                if prefix and account_id:
                    temp_group.append(f"{prefix}_{account_id}")
                    
    if temp_group:
        groups.append((temp_comment, temp_group))
        
    for comment, group in groups:
        if not comment:
            continue
            
        folder_name = comment.strip()
        folder_name = PATTERN_INVALID_CHARS.sub('_', folder_name)
        folder_name = folder_name.rstrip(" .") 
        
        for account_key in group:
            mapping[account_key] = folder_name
            
    return mapping

def get_account_keywords(all_targets):
    account_keywords = {}
    for pf, acc_list in all_targets.items():
        pf_prefix = ""
        if pf == "twitter": pf_prefix = "tw"
        elif pf == "twitter_hashtag": pf_prefix = "twtag"
        elif pf == "instagram": pf_prefix = "ig"
        elif pf == "pixiv": pf_prefix = "px"
        
        if pf_prefix:
            for acc in acc_list:
                if acc.get("keywords"):
                    acc_key = f"{pf_prefix}_{acc['id'].lower()}"
                    account_keywords[acc_key] = acc["keywords"]
    return account_keywords

def organize_single_file(media_path, target_json, folder_mapping, account_keywords):
    if not os.path.exists(media_path): return

    filename = os.path.basename(media_path)
    ext = os.path.splitext(filename)[1].lower()
    video_exts = {".mp4", ".mov", ".webm", ".avi", ".mkv", ".m4v"}
    base_dst_dir = VIDEO_SAVE_DIR if ext in video_exts else IMAGE_SAVE_DIR
    
    account_folder = "Unknown"
    match = PATTERN_FILENAME_1.search(filename)
    if not match:
        match = PATTERN_FILENAME_2.search(filename)
        
    if match:
        prefix = match.group(1).lower()
        raw_account_id = match.group(2)
        account_key = f"{prefix}_{raw_account_id.lower()}"
        
        hit_keyword = None
        if account_key in account_keywords and target_json and os.path.exists(target_json):
            try:
                meta = load_cached_json(target_json)
                if meta is not None:
                    m = meta
                    if isinstance(meta, list):
                        if len(meta) >= 2 and isinstance(meta[1], dict): m = meta[1]
                        elif len(meta) >= 1 and isinstance(meta[0], dict): m = meta[0]
                    
                    if isinstance(m, dict):
                        text_content = str(m.get("content", "")) + " " + str(m.get("description", ""))
                        for kw in account_keywords[account_key]:
                            if kw in text_content:
                                hit_keyword = kw
                                break
            except Exception: pass
        
        if hit_keyword:
            account_folder = hit_keyword
            account_folder = PATTERN_INVALID_CHARS.sub('_', account_folder)
        elif account_key in folder_mapping:
            account_folder = folder_mapping[account_key]
        else:
            account_folder = f"{prefix}_{raw_account_id}"
            account_folder = PATTERN_INVALID_CHARS.sub('_', account_folder)

    dst_dir = os.path.join(base_dst_dir, account_folder)
    os.makedirs(dst_dir, exist_ok=True)
    
    try:
        shutil.move(media_path, os.path.join(dst_dir, filename))
        if target_json and os.path.exists(target_json):
            os.remove(target_json)
            # キャッシュからも削除しておく
            with JSON_CACHE_LOCK:
                if target_json in JSON_CACHE:
                    del JSON_CACHE[target_json]
    except Exception:
        pass

# --------------------------------------------------------
# ★ メディア処理・メタデータ注入関数
# --------------------------------------------------------
def deep_clean_mp4(file_path):
    if not file_path.lower().endswith(".mp4"):
        return True
    
    temp_path = file_path + ".clean.mp4"
    conv_cmd = [
        "ffmpeg", "-i", file_path, "-c", "copy",
        "-map_metadata", "-1", "-fflags", "+bitexact",
        "-movflags", "+faststart", "-y", temp_path
    ]
    
    try:
        result = subprocess.run(conv_cmd, capture_output=True)
        if result.returncode == 0 and os.path.exists(temp_path):
            os.replace(temp_path, file_path)
            return True
            
        err_msg = result.stderr.decode('cp932', errors='replace')
        print(f"\n❌ [FFmpeg エラー] {os.path.basename(file_path)}: {err_msg.strip()}")
        if os.path.exists(temp_path): 
            os.remove(temp_path)
        return False
        
    except Exception as e:
        print(f"\n❌ [FFmpeg 実行例外] {e}")
        return False

def _background_inject(media_path, exiftool_path, folder_mapping, account_keywords, is_final_sweep=False):
    # 渡された文字が何であれ、ファイル名だけを抜き出して強制的に正しい保存先を指定する
    media_path = os.path.join(BASE_SAVE_DIR, os.path.basename(media_path))

    if media_path.lower().endswith(".zip"):
        SUCCESS_INJECTED.add(os.path.basename(media_path))
        if not is_final_sweep:
            organize_single_file(media_path, None, folder_mapping, account_keywords)
        return
        
    base_dir = os.path.dirname(media_path)
    base_name = os.path.splitext(os.path.basename(media_path))[0]
    prefix_match = base_name.rsplit("_", 1)[0] if "_" in base_name else base_name
    
    target_json = None
    loops = 1 if is_final_sweep else 15
    
    for _ in range(loops):
        exact_json = os.path.join(base_dir, base_name + ".json")
        if os.path.exists(exact_json) and os.path.getsize(exact_json) > 0:
            target_json = exact_json
            break
        
        candidates = [c for c in glob.glob(os.path.join(base_dir, prefix_match + "*.json")) if os.path.getsize(c) > 0]
        if candidates:
            target_json = candidates[0]
            break
            
        time.sleep(0.5)

    if not target_json or not os.path.exists(media_path):
        if not is_final_sweep:
            print(f"\n⚠️ [JSON未発見] {os.path.basename(media_path)} のJSONがタイムアウトしました。")
        return

    deep_clean_mp4(media_path)

    cmd_args = [
        "-overwrite_original", "-m", "-F",         
        f"-UserComment<={target_json}", f"-Description<={target_json}",
        f"-XMP:Description<={target_json}", f"-Keys:Description<={target_json}",
        media_path
    ]
    
    try:
        # 常駐化ExifToolでコマンドを流し込む(圧倒的高速化)
        is_success, output = FAST_EXIFTOOL.execute(*cmd_args)
        
        # 失敗判定になった場合のみ、従来通りの確実な方法で検証してリカバリを図る
        if not is_success:
            check_res = subprocess.run([exiftool_path, "-Description", "-s3", media_path], capture_output=True, text=True, errors="replace")
            if check_res.stdout.strip():
                is_success = True
                print(f"\n⚠️ [ExifTool 警告回復] {os.path.basename(media_path)} はパニックを起こしましたが、注入は成功していました。")
            else:
                err_msg = output.strip()
                print(f"\n❌ [ExifTool 注入エラー] {os.path.basename(media_path)}: {err_msg}")

        if is_success:
            SUCCESS_INJECTED.add(os.path.basename(media_path))
            if not is_final_sweep:
                organize_single_file(media_path, target_json, folder_mapping, account_keywords)

    except Exception as e:
        print(f"\n❌ [ExifTool 実行例外] {e}")

# --------------------------------------------------------
# ★ 整理機能 (最終バックアップスイープ)
# --------------------------------------------------------
def inject_and_organize_files():
    json_dir = os.path.join(BASE_SAVE_DIR, METADATA_DIR_NAME)
    for d in (json_dir, IMAGE_SAVE_DIR, VIDEO_SAVE_DIR, NOJSON_SAVE_DIR):
        os.makedirs(d, exist_ok=True)
        
    video_exts = {".mp4", ".mov", ".webm", ".avi", ".mkv", ".m4v"}
    image_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp"}
    
    targets_filepath = (
        TARGETS_FILE
        if os.path.exists(TARGETS_FILE)
        else os.path.join(BASE_SAVE_DIR, "targets.txt")
    )
    folder_mapping = get_folder_mapping(targets_filepath)
    all_targets = load_targets()
    account_keywords = get_account_keywords(all_targets)

    if os.path.exists(EXIFTOOL_PATH):
        for f in os.listdir(BASE_SAVE_DIR):
            if os.path.splitext(f)[1].lower() in (image_exts | video_exts) and f not in SUCCESS_INJECTED:
                _background_inject(os.path.join(BASE_SAVE_DIR, f), EXIFTOOL_PATH, folder_mapping, account_keywords, is_final_sweep=True)

    counts = {"img": 0, "vid": 0, "nojson": 0, "json_move": 0, "json_del": 0}
    failed_prefixes = set()
    
    name_cache = load_name_cache()
    
    for filename in os.listdir(BASE_SAVE_DIR):
        src_path = os.path.join(BASE_SAVE_DIR, filename)
        if not os.path.isfile(src_path): continue
        
        ext = os.path.splitext(filename)[1].lower()
        if ext not in (video_exts | image_exts): continue
        
        base_name = os.path.splitext(filename)[0]
        
        account_folder = "Unknown"
        match = PATTERN_FILENAME_1.search(filename)
        if not match:
            match = PATTERN_FILENAME_2.search(filename)
            
        if match:
            prefix = match.group(1).lower()
            raw_account_id = match.group(2)
            account_key = f"{prefix}_{raw_account_id.lower()}"
            
            hit_keyword = None
            if account_key in account_keywords:
                exact_json = os.path.join(BASE_SAVE_DIR, base_name + ".json")
                target_json = exact_json if os.path.exists(exact_json) else None
                
                if not target_json:
                    prefix_match = base_name.rsplit("_", 1)[0] if "_" in base_name else base_name
                    cands = glob.glob(os.path.join(BASE_SAVE_DIR, prefix_match + "*.json"))
                    if cands:
                        target_json = cands[0]
                        
                if target_json:
                    try:
                        meta = load_cached_json(target_json)
                        if meta is not None:
                            m = meta
                            if isinstance(meta, list):
                                if len(meta) >= 2 and isinstance(meta[1], dict): m = meta[1]
                                elif len(meta) >= 1 and isinstance(meta[0], dict): m = meta[0]
                            
                            if isinstance(m, dict):
                                text_content = str(m.get("content", "")) + " " + str(m.get("description", ""))
                                for kw in account_keywords[account_key]:
                                    if kw in text_content:
                                        hit_keyword = kw
                                        break
                    except Exception:
                        pass
            
            if hit_keyword:
                account_folder = hit_keyword
                account_folder = PATTERN_INVALID_CHARS.sub('_', account_folder)
            elif account_key in folder_mapping:
                account_folder = folder_mapping[account_key]
            else:
                account_folder = f"{prefix}_{raw_account_id}"
                account_folder = PATTERN_INVALID_CHARS.sub('_', account_folder)
        
        if filename in SUCCESS_INJECTED:
            base_dst_dir = VIDEO_SAVE_DIR if ext in video_exts else IMAGE_SAVE_DIR
            dst_dir = os.path.join(base_dst_dir, account_folder)
            os.makedirs(dst_dir, exist_ok=True)
            counts["vid" if ext in video_exts else "img"] += 1
        else:
            dst_dir = NOJSON_SAVE_DIR
            counts["nojson"] += 1
            failed_prefixes.update([base_name, base_name.rsplit("_", 1)[0] if "_" in base_name else base_name])
            
        try:
            shutil.move(src_path, os.path.join(dst_dir, filename))
        except Exception:
            pass

    for filename in os.listdir(BASE_SAVE_DIR):
        src_path = os.path.join(BASE_SAVE_DIR, filename)
        if not os.path.isfile(src_path) or not filename.lower().endswith(".json"): continue
        if filename in ("info.json", "account_names.json"): continue
        
        base_name = os.path.splitext(filename)[0]
        prefix_match = base_name.rsplit("_", 1)[0] if "_" in base_name else base_name
        
        match = PATTERN_FILENAME_1.search(filename)
        if not match:
            match = PATTERN_FILENAME_2.search(filename)
            
        if match:
            prefix = match.group(1)
            account_id = match.group(2)
            platform_map = {"tw": "twitter", "twtag": "twitter_hashtag", "ig": "instagram", "px": "pixiv"}
            pf = platform_map.get(prefix)
            if pf:
                try:
                    meta = load_cached_json(src_path)
                    name = extract_name_from_meta(pf, meta)
                    if name:
                        # キーを小文字化して照合
                        cache_key = f"{pf}:{account_id.lower()}"
                        if name_cache.get(cache_key) != name:
                            name_cache[cache_key] = name
                            save_name_cache(name_cache)
                except Exception:
                    pass
        
        if base_name in failed_prefixes or prefix_match in failed_prefixes:
            try:
                shutil.move(src_path, os.path.join(json_dir, filename))
                counts["json_move"] += 1
            except Exception:
                pass
        else:
            try:
                os.remove(src_path)
                with JSON_CACHE_LOCK:
                    if src_path in JSON_CACHE: del JSON_CACHE[src_path]
                counts["json_del"] += 1
            except Exception:
                pass

    print(f"🧹 ファイル整理(最終チェック):")
    print(f"   ✅ 成功 (移動済): 画像 {counts['img']}件 / 動画 {counts['vid']}件")
    if counts["json_del"] > 0:
        print(f"   🗑️ 不要JSON削除: {counts['json_del']}件")
    if counts["nojson"] > 0:
        print(f"   ❌ 失敗 (nojsonへ): メディア {counts['nojson']}件")
    if counts["json_move"] > 0:
        print(f"   📁 失敗用JSON退避 ({METADATA_DIR_NAME}へ): {counts['json_move']}件")

# --------------------------------------------------------
# ★ ダウンロード実行処理
# --------------------------------------------------------
def download_account(platform, account, config, completed_accounts, bg_executor, folder_mapping, account_keywords, keywords=None):
    identifier = f"{platform}:{account}"
    url = config["url_template"].format(account)
    prefix = config["prefix"]
    
    account_name = get_account_name(platform, prefix, account)
    display_name = f"{account_name} ({account})" if account_name and account_name != account else account
    
    print(f"{'='*80}")
    
    command = [
        "gallery-dl", "--cookies", COOKIES_FILE, "--download-archive", ARCHIVE_FILE,
        "--sleep", "4-6", "--sleep-request", "4-6", "-d", BASE_SAVE_DIR,       
        "-o", "directory=.", "-o", f"filename={prefix}_{account}_{{date:%Y%m%d_%H%M%S}}_{{id}}_{{num}}.{{extension}}",
        "--write-metadata", "--exec", "cmd /c echo GAL_DL_SUCCESS:::{}"
    ]
    command.extend(config["args"])
    
    filter_exprs = []
    
    if MIN_MEDIA_WIDTH > 0:
        filter_exprs.append(f"(not width or width >= {MIN_MEDIA_WIDTH})")
    if MIN_MEDIA_HEIGHT > 0:
        filter_exprs.append(f"(not height or height >= {MIN_MEDIA_HEIGHT})")

    if platform.startswith("twitter") and keywords:
        conditions = [f"('{kw.replace('\'', '\\\'')}' in str(content))" for kw in keywords]
        filter_exprs.append(f"(content and ({' or '.join(conditions)}))")

    if filter_exprs:
        combined_filter = " and ".join(filter_exprs)
        command.extend(["--filter", combined_filter])
        
        if platform.startswith("twitter") and keywords:
            print(f"🔍 [フィルタ有効] キーワード: {', '.join(keywords)} ")

    if identifier in completed_accounts:
        print(f"🚀 [{platform}] {display_name} (高速差分モード)")
        command.extend(["--abort", "3"])
    else:
        print(f"🐢 [{platform}] {display_name} (初回/未完了継続モード)")

    command.append(url)
    
    dl_count = err_count = 0
    is_critical_error = False
    
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
        )
        print(f"⏳ 処理を開始しました... (中断して完了扱いにするには 's' キー)", end="", flush=True)
        
        # 標準出力を非ブロッキングで読み込むためのキューとスレッド
        line_queue = queue.Queue()
        def enqueue_output(out, q):
            for l in iter(out.readline, ''):
                q.put(l)
            out.close()
        
        t = threading.Thread(target=enqueue_output, args=(process.stdout, line_queue))
        t.daemon = True
        t.start()

        while True:
            # キー入力の監視 ('s'キーで中断し、完了済みとしてマーク)
            if msvcrt and msvcrt.kbhit():
                if msvcrt.getch().decode('utf-8', errors='ignore').lower() == 's':
                    print(f"\n⏩ [手動完了] {display_name} の処理を中断し、完了済みとして記録します...")
                    process.terminate()
                    process.wait()
                    mark_as_completed(identifier)
                    completed_accounts.add(identifier)
                    return False

            try:
                line = line_queue.get_nowait()
            except queue.Empty:
                if process.poll() is not None:
                    break
                time.sleep(0.1) # CPU負荷軽減
                continue

            line = line.strip()
            if not line: continue
            lower_line = line.lower()
            
            if "[error]" in lower_line or "exception" in lower_line:
                err_count += 1
                print(f"\n❌ エラー詳細: {line}")
                if "challenge" in lower_line or "400 bad request" in lower_line:
                    is_critical_error = True
                    print(f"🚨 [警告] セキュリティロック(Challenge)または不正なリクエストを検知しました。")
                elif "401" in lower_line or "login" in lower_line or "unauthorized" in lower_line:
                    is_critical_error = True
                    print(f"🚨 [警告] ログイン切れ・認証エラーを検知しました。")
                elif "429" in lower_line or "too many requests" in lower_line or "rate limit" in lower_line:
                    is_critical_error = True
                    print(f"🚨 [警告] アクセス制限(Rate Limit/429)を検知しました。")
                    
            elif "GAL_DL_SUCCESS:::" in line:
                dl_count += 1
                parts = line.split(":::")
                if len(parts) >= 2:
                    media_path = os.path.normpath(parts[-1].strip(' "\'\r\n'))
                    
                    if not account_name:
                        base_name = os.path.splitext(os.path.basename(media_path))[0]
                        prefix_match = base_name.rsplit("_", 1)[0] if "_" in base_name else base_name
                        json_cands = glob.glob(os.path.join(os.path.dirname(media_path), prefix_match + "*.json"))
                        
                        if json_cands:
                            try:
                                meta = load_cached_json(json_cands[0])
                                extracted_name = extract_name_from_meta(platform, meta)
                                if extracted_name:
                                    account_name = extracted_name
                                    display_name = f"{account_name} ({account})"
                                    cache = load_name_cache()
                                    # キャッシュ保存時もキーを正規化
                                    cache[identifier.lower()] = account_name
                                    save_name_cache(cache)
                            except Exception:
                                pass

                    if os.path.exists(EXIFTOOL_PATH):
                        bg_executor.submit(_background_inject, media_path, EXIFTOOL_PATH, folder_mapping, account_keywords)
                    
            print(f"\r⏳ 処理中... [ 新規: {dl_count}件 | エラー: {err_count}件 ]", end="", flush=True)
            
        process.wait()
        print(f"\r{' '*80}\r✅ 完了!    [ 新規: {dl_count}件 / エラー: {err_count}件 ]")
        
        if process.returncode == 0 and err_count == 0:
            if identifier not in completed_accounts:
                mark_as_completed(identifier)
                completed_accounts.add(identifier)
        else:
            print(f"⚠️ このアカウントは一部エラーで中断されました(次回また続きからチェックします)。")

        return is_critical_error

    except KeyboardInterrupt:
        process.terminate()
        process.wait()
        print("\n\n🛑 手動で中断されました。すべての処理を停止します。")
        FAST_EXIFTOOL.stop() # 終了時に常駐プロセスも安全に切断
        sys.exit(0)

def run_downloader(do_organize=True, platforms=None):
    require_setting_str("BASE_SAVE_DIR")
    completed_accounts = load_completed_list()
    targets = load_targets()
    os.makedirs(BASE_SAVE_DIR, exist_ok=True)

    if platforms is None:
        platforms = ["pixiv", "twitter"]

    targets_filepath = (
        TARGETS_FILE
        if os.path.exists(TARGETS_FILE)
        else os.path.join(BASE_SAVE_DIR, "targets.txt")
    )
    folder_mapping = get_folder_mapping(targets_filepath)
    account_keywords = get_account_keywords(targets)

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as bg_executor:
            for platform in platforms:
                accounts = targets.get(platform, [])
                if not accounts: continue
                
                config = PLATFORM_CONFIG.get(platform)
                if not config: continue

                for target_info in accounts:
                    account = target_info["id"]
                    keywords = target_info["keywords"]
                    is_critical = download_account(platform, account, config, completed_accounts, bg_executor, folder_mapping, account_keywords, keywords)
                    if is_critical:
                        print(f"\n🚨 安全のため、{platform} の残りのアカウントをスキップします。\n")
                        break
                    time.sleep(10)

    finally:
        print(f"\n{'='*100}")
        if do_organize:
            print("🎉 最終処理を実行中...(未処理のJSON注入とファイルの振り分け)")
            inject_and_organize_files()
        else:
            print("⏭️ 引数指定により、最終処理(未処理のJSON注入とファイルの振り分け)をスキップしました。")
            
        # ★ 全処理終了時に常駐ExifToolを安全に閉じる
        FAST_EXIFTOOL.stop()

def main():
    parser = argparse.ArgumentParser(description="SNS Downloader with Gallery-dl")
    parser.add_argument("-t", "--twitter", action="store_true", help="Twitterを対象にする")
    parser.add_argument("-p", "--pixiv", action="store_true", help="Pixivを対象にする")
    parser.add_argument("-i", "--instagram", action="store_true", help="Instagramを対象にする")
    parser.add_argument("-tg", "--hashtag", action="store_true", help="Twitterハッシュタグを対象にする")
    parser.add_argument("--skip-organize", action="store_true", help="ダウンロード後のファイル整理をスキップする")
    
    args = parser.parse_args()
    
    selected_platforms = []
    if args.twitter: selected_platforms.append("twitter")
    if args.pixiv: selected_platforms.append("pixiv")
    if args.instagram: selected_platforms.append("instagram")
    if args.hashtag: selected_platforms.append("twitter_hashtag")
    
    # 引数指定がない場合は、従来の挙動（Pixiv）をデフォルトにする
    if not selected_platforms:
        selected_platforms = ["pixiv", "twitter"]
        
    run_downloader(do_organize=not args.skip_organize, platforms=selected_platforms)

if __name__ == "__main__":
    main()