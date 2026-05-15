import os
import re
import subprocess
from pathlib import Path
import send2trash

# ＝＝＝ 処理対象の拡張子 ＝＝＝
MEDIA_EXTS = {
    # 動画
    ".mp4", ".mov", ".webm", ".avi", ".mkv", ".m4v",
    # 画像
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp"
}
image_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp"}
video_exts = {".mp4", ".mov", ".webm", ".avi", ".mkv", ".m4v"}

# --------------------------------------------------------
# ★ 正規表現の事前コンパイル
# --------------------------------------------------------
PATTERN_FILENAME_1 = re.compile(r"^(tw|twtag|ig|px)_(.+?)_\d{8}_\d{6}_")
PATTERN_FILENAME_2 = re.compile(r"^(tw|twtag|ig|px)_(.+)_[^_]+\.[a-zA-Z0-9]+$")
PATTERN_INVALID_CHARS = re.compile(r'[\\/:*?"<>|]')
PATTERN_TARGET_COMMENT = re.compile(r"^(twitter|instagram|pixiv|twtag)\s*:", re.IGNORECASE)

def deep_clean_mp4(file_path):
    """
    FFmpegを使用して、Terminator警告の原因となる壊れたメタデータ領域を
    物理的に一度削除し、クリーンなコンテナを作成する
    """
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

def safe_delete(path: Path, use_trash: bool = True):
    """
    ファイルを安全に削除する。use_trash=True の場合、send2trash を使用。
    失敗した場合は警告を表示。
    """
    try:
        if not path.exists():
            return
        
        if use_trash:
            send2trash.send2trash(str(path))
        else:
            # send2trash が失敗した場合のフォールバック、または明示的にゴミ箱をスキップする場合
            if path.is_file():
                os.remove(path)
            elif path.is_dir():
                # ディレクトリの場合は shutil.rmtree を使うが、send2trash が優先されるべき
                # ここではファイルのみを想定しているため、ディレクトリ削除は send2trash に任せる
                pass 
        
    except Exception as e:
        print(f"Warning: Could not delete {path}: {e}")