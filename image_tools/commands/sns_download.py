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

import zipfile
import io
try:
    from PIL import Image
    # pillow_avif がないとアニメーションAVIF変換ができないため、インポートを試みる
    # noqa: F401 は不要
    import pillow_avif
except ImportError:
    pass

from image_tools.paths import PROJECT_ROOT
from image_tools import settings as app_settings
from image_tools.settings import require_setting_str
from image_tools.utils.media_utils import deep_clean_mp4, PATTERN_FILENAME_1, PATTERN_FILENAME_2, PATTERN_INVALID_CHARS, PATTERN_TARGET_COMMENT, MEDIA_EXTS

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
ACCOUNT_STATS_FILE = str(PROJECT_ROOT / "account_stats.json")

FAKE_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0"

# ダウンロードするメディアの最小サイズ (0の場合は制限なし)
MIN_MEDIA_WIDTH = 300   
MIN_MEDIA_HEIGHT = 300  
MIN_TWITTER_LIKES = 0   # Twitterの最小「いいね」数 (0は無制限)

# プラットフォームごとの設定定義
PLATFORM_CONFIG = {
    "instagram": {
        "url_template": "https://www.instagram.com/{}/",
        "prefix": "ig",
        "args": ["--max-downloads", "30", "--user-agent", FAKE_USER_AGENT],
        "sleep": "15-30"
    },
    "pixiv": {
        "url_template": "https://www.pixiv.net/users/{}",
        "prefix": "px",
        "args": ["-o", "pixiv:ugoira-conv=zip", "-o", "pixiv:include=illust,manga,ugoira"]
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

from image_tools.utils.exiftool_wrapper import FastExifTool

FAST_EXIFTOOL = FastExifTool(EXIFTOOL_PATH)

# --------------------------------------------------------
# ★ ファイル・文字コード対応読み込み関数
# --------------------------------------------------------
def read_text_file(filepath):
    """テキストファイルをUTF-8-SIGまたはCP932で読み込む"""
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            return f.readlines()
    except UnicodeDecodeError:
        with open(filepath, "r", encoding="cp932") as f:
            return f.readlines()
    except FileNotFoundError:
        return []

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

def load_account_stats():
    if os.path.exists(ACCOUNT_STATS_FILE):
        return load_cached_json(ACCOUNT_STATS_FILE) or {}
    return {}

def save_account_stats(stats):
    try:
        with open(ACCOUNT_STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=4)
        with JSON_CACHE_LOCK:
            JSON_CACHE[ACCOUNT_STATS_FILE] = stats
    except Exception:
        pass

def should_skip_account(platform, account, stats):
    identifier = f"{platform}:{account}"
    if identifier not in stats:
        return False, 0
    
    entry = stats[identifier]
    last_check = entry.get("last_check", 0)
    last_new = entry.get("last_new", 0)
    now = time.time()
    
    # 最後に新しい画像が見つかってからの経過日数
    days_since_new = (now - last_new) / 86400
    
    # 更新頻度が低いほど間隔を広げる (1日未更新なら2日おき、2日なら3日おき... 最大7日)
    interval_days = min(7, int(days_since_new) + 1)
    
    # 最後の確認からインターバルが経過していなければスキップ
    if (now - last_check) < (interval_days * 86400):
        remaining_days = interval_days - (now - last_check) / 86400
        return True, remaining_days
    return False, 0

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
        
        name = (meta.get("user_name") or 
                meta.get("userName") or 
                (user.get("name") if isinstance(user, dict) else user if isinstance(user, str) else None) or 
                (user.get("userName") if isinstance(user, dict) else None) or
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
            c = line.lstrip("#").strip() # コメント行から # を除去
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
        folder_name = folder_name.rstrip(" .") # 末尾のスペースやドットを除去
        
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
    ext = os.path.splitext(filename)[1].lower() # 拡張子
    base_dst_dir = VIDEO_SAVE_DIR if ext in video_exts else IMAGE_SAVE_DIR
    
    account_folder = "Unknown"
    match = PATTERN_FILENAME_1.search(filename)
    if not match:
        match = PATTERN_FILENAME_2.search(filename)
        
    if match:
        prefix = match.group(1).lower()
        raw_account_id = match.group(2)
        
        source_tag = None
        if "#" in raw_account_id:
            raw_account_id, source_tag = raw_account_id.split("#", 1)
            filename = filename.replace(f"#{source_tag}", "")

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
        elif source_tag and f"twtag_{source_tag.lower()}" in folder_mapping:
            account_folder = folder_mapping[f"twtag_{source_tag.lower()}"]
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


def convert_ugoira_zip_to_avif(zip_path, json_path=None):
    """PixivのうごイラZIPをアニメーションAVIFに変換する"""
    try:
        from PIL import Image
    except ImportError:
        return None

    durations = []
    frames = []
    ugoira_meta = None

    # メタデータから各フレームの遅延時間を取得
    if json_path and os.path.exists(json_path):
        meta = load_cached_json(json_path)
        if isinstance(meta, list) and len(meta) > 1:
            ugoira_meta = meta[1].get("ugoira_data")
        elif isinstance(meta, dict):
            ugoira_meta = meta.get("ugoira_data")

    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            img_files = [n for n in z.namelist() if n.lower().endswith(('.jpg', '.jpeg', '.png'))]
            if not img_files: return None
                
            if ugoira_meta and "frames" in ugoira_meta:
                for f_info in ugoira_meta["frames"]:
                    fname = f_info["file"]
                    delay = f_info["delay"]
                    if fname in z.namelist():
                        with z.open(fname) as f:
                            frames.append(Image.open(io.BytesIO(f.read())).convert("RGB"))
                            durations.append(delay)
            else:
                # メタデータがない場合は連番ソートして10fpsで作成
                for name in sorted(img_files):
                    with z.open(name) as f:
                        frames.append(Image.open(io.BytesIO(f.read())).convert("RGB"))
                        durations.append(100)
        
        if not frames: return None
        
        avif_path = os.path.splitext(zip_path)[0] + ".avif"
        # アニメーションAVIFとして保存 (qualityは60、ループ設定)
        frames[0].save(
            avif_path, save_all=True, append_images=frames[1:],
            duration=durations, loop=0, quality=60, subsampling="4:4:4"
        )
        return avif_path
    except Exception as e:
        print(f"\n❌ [うごイラ変換エラー] {os.path.basename(zip_path)}: {e}")
        return None

def _background_inject(media_path, exiftool_path, folder_mapping, account_keywords, is_final_sweep=False):
    # 渡された文字が何であれ、ファイル名だけを抜き出して強制的に正しい保存先を指定する
    media_path = os.path.join(BASE_SAVE_DIR, os.path.basename(media_path)) # BASE_SAVE_DIR直下にあると仮定

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

    # ZIPファイル（Pixivうごイラ）の場合はAVIFへの変換を試みる
    if media_path.lower().endswith(".zip"):
        new_avif = convert_ugoira_zip_to_avif(media_path, target_json)
        if new_avif:
            try:
                os.remove(media_path)
                media_path = new_avif
                base_name = os.path.splitext(os.path.basename(media_path))[0]
            except: pass
        else:
            # 変換対象外または失敗した場合はそのまま整理へ
            SUCCESS_INJECTED.add(os.path.basename(media_path))
            if not is_final_sweep:
                organize_single_file(media_path, target_json, folder_mapping, account_keywords)
            return

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
            
            # ハッシュタグ経由のファイルからタグ情報を分離
            source_tag = None
            if "#" in raw_account_id:
                raw_account_id, source_tag = raw_account_id.split("#", 1)

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
            elif source_tag and f"twtag_{source_tag.lower()}" in folder_mapping:
                # ユーザーが未登録なら、元のハッシュタグのグループフォルダを使用
                account_folder = folder_mapping[f"twtag_{source_tag.lower()}"]
            else:
                account_folder = f"{prefix}_{raw_account_id}"
                account_folder = PATTERN_INVALID_CHARS.sub('_', account_folder)
            
            # ファイル名から一時的なタグ識別子を削除
            if source_tag:
                filename = filename.replace(f"#{source_tag}", "")
        
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
            
            if "#" in account_id:
                account_id = account_id.split("#", 1)[0]

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
def download_account(platform, account, config, completed_accounts, bg_executor, folder_mapping, account_keywords, keywords=None, min_likes=0, ignore_history=False):
    identifier = f"{platform}:{account}"
    url = config["url_template"].format(account)
    prefix = config["prefix"]
    
    account_name = get_account_name(platform, prefix, account)
    display_name = f"{account_name} ({account})" if account_name and account_name != account else account
    
    print(f"{'='*80}")
    
    # ファイル名テンプレートの構築
    # ハッシュタグの場合は 'tw_{投稿者ID}#{タグ名}' にすることで整理時に追跡可能にする
    fn_prefix = prefix
    fn_account = account
    if platform == "twitter_hashtag":
        fn_prefix = "tw"
        fn_account = "{author[nick]}#" + account

    command = [
        "gallery-dl", "--cookies", COOKIES_FILE,
        "--sleep", "4-6", "--sleep-request", "4-6", "-d", BASE_SAVE_DIR,       
        "-o", "directory=.", "-o", f"filename={fn_prefix}_{fn_account}_{{date:%Y%m%d_%H%M%S}}_{{id}}_{{num}}.{{extension}}",
        "--write-metadata", "--exec", "cmd /c echo GAL_DL_SUCCESS:::{}"
    ]
    if not ignore_history:
        command.extend(["--download-archive", ARCHIVE_FILE])

    command.extend(config["args"])
    
    filter_exprs = []
    
    if MIN_MEDIA_WIDTH > 0:
        filter_exprs.append(f"(not width or width >= {MIN_MEDIA_WIDTH})")
    if MIN_MEDIA_HEIGHT > 0:
        filter_exprs.append(f"(not height or height >= {MIN_MEDIA_HEIGHT})")

    # Twitterの「いいね」数フィルタを追加
    if platform.startswith("twitter") and min_likes > 0:
        filter_exprs.append(f"(favorite_count >= {min_likes})")

    if platform.startswith("twitter") and keywords:
        conditions = [f"('{kw.replace('\'', '\\\'')}' in str(content))" for kw in keywords]
        filter_exprs.append(f"(content and ({' or '.join(conditions)}))")

    if filter_exprs:
        combined_filter = " and ".join(filter_exprs)
        command.extend(["--filter", combined_filter])
        
        if platform.startswith("twitter"):
            if keywords:
                print(f"🔍 [フィルタ有効] キーワード: {', '.join(keywords)} ")
            if min_likes > 0:
                print(f"🔍 [フィルタ有効] 最小いいね数: {min_likes} ")

    if identifier in completed_accounts and not ignore_history:
        print(f"🚀 [{platform}] {display_name} (高速差分モード)")
        command.extend(["--abort", "3"])
    else:
        mode_text = "履歴無視モード" if ignore_history else "初回/未完了継続モード"
        print(f"🐢 [{platform}] {display_name} ({mode_text})")

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
        print(f"⏳ 処理を開始しました... (中断して完了扱いにするには 's' キー)")
        
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
                    msg = "中断し、完了済みとして記録します..."
                    print(f"\r{' ' * 80}\r⏩ [手動完了] {display_name} の処理を{msg}")
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
                print(f"\r{' ' * 80}\r❌ エラー詳細: {line}")
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

        # 統計情報の更新 (致命的なエラーでない場合)
        if not is_critical_error:
            stats = load_account_stats()
            ident = f"{platform}:{account}"
            now = time.time()
            if ident not in stats:
                stats[ident] = {}
            
            stats[ident]["last_check"] = now
            if dl_count > 0:
                stats[ident]["last_new"] = now
            elif "last_new" not in stats[ident]:
                # 初回チェックで何も見つからなかった場合、現在時刻を起算点とする
                stats[ident]["last_new"] = now
            save_account_stats(stats)
        
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

def run_downloader(do_organize=True, platforms=None, min_likes=0, specific_ids=None, force_check=False):
    require_setting_str("BASE_SAVE_DIR")
    completed_accounts = load_completed_list()
    account_stats = load_account_stats()
    os.makedirs(BASE_SAVE_DIR, exist_ok=True)

    if platforms is None:
        platforms = ["pixiv", "twitter"]

    ignore_history = False
    if specific_ids:
        targets = {pf: [{"id": sid, "keywords": []} for sid in specific_ids] for pf in platforms}
        ignore_history = True
        print(f"✨ 特定ID指定ダウンロード: {', '.join(specific_ids)} (履歴を無視して取得します)")
    else:
        targets = load_targets()

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

                    # 更新頻度に基づくスキップ判定 (特定ID指定や強制実行時は無視)
                    if not specific_ids and not force_check:
                        skip, remaining = should_skip_account(platform, account, account_stats)
                        if skip:
                            account_name = get_account_name(platform, config["prefix"], account)
                            display_name = f"{account_name} ({account})" if account_name and account_name != account else account
                            print(f"⏭️  [{platform}] {display_name}: 投稿頻度により確認を延期 (残り約 {remaining:.1f}日)")
                            continue

                    keywords = target_info["keywords"]
                    is_critical = download_account(platform, account, config, completed_accounts, bg_executor, folder_mapping, account_keywords, keywords, min_likes=min_likes, ignore_history=ignore_history)
                    if is_critical:
                        print(f"\n🚨 安全のため、{platform} の残りのアカウントをスキップします。\n")
                        break
                    
                    # アカウント間の待機時間をプラットフォームごとに調整
                    acc_sleep = 60 if platform == "instagram" else 10
                    time.sleep(acc_sleep)

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
    parser.add_argument("--min-likes", type=int, default=MIN_TWITTER_LIKES, help="Twitterの最小「いいね」数")
    parser.add_argument("-id", "--ids", nargs="+", help="特定のIDを指定してダウンロード (履歴無視)")
    parser.add_argument("-f", "--force", action="store_true", help="更新頻度によるスキップを無視して強制チェックする")
    
    args = parser.parse_args()
    
    selected_platforms = []
    if args.twitter: selected_platforms.append("twitter")
    if args.pixiv: selected_platforms.append("pixiv")
    if args.instagram: selected_platforms.append("instagram")
    if args.hashtag: selected_platforms.append("twitter_hashtag")
    
    # 引数指定がない場合は、従来の挙動（Pixiv）をデフォルトにする
    if not selected_platforms:
        selected_platforms = ["pixiv", "twitter"]
        
    run_downloader(do_organize=not args.skip_organize, platforms=selected_platforms, min_likes=args.min_likes, specific_ids=args.ids, force_check=args.force)

if __name__ == "__main__":
    main()