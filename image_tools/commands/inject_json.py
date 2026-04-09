import os
import glob
import subprocess

from image_tools import settings as app_settings
from image_tools.settings import require_setting_str

_S = app_settings.load_settings()
_BASE = _S.get("BASE_SAVE_DIR") or ""
BASE_SAVE_DIR = _BASE
METADATA_DIR = os.path.join(_BASE, "metadata") if _BASE else ""
EXIFTOOL_PATH = _S.get("EXIFTOOL_PATH") or ""
WAITING_DIR = BASE_SAVE_DIR
TARGET_INJECT_DIR = _S.get("INJECT_NOJSON_DIR") or ""

def is_match(base_name, file_name):
    name_idx = file_name.find(base_name)
    if name_idx == -1: return False
    next_char_idx = name_idx + len(base_name)
    if next_char_idx < len(file_name):
        if file_name[next_char_idx].isalnum(): return False 
    return True

def deep_clean_mp4(file_path):
    """
    FFmpegを使用して、Terminator警告の原因となる壊れたメタデータ領域を
    物理的に一度削除し、クリーンなコンテナを作成する
    """
    if not file_path.lower().endswith(".mp4"):
        return True
    
    temp_path = file_path + ".clean.mp4"
    
    # -map_metadata -1: 既存の全メタデータを破棄
    # -fflags +bitexact: 余計な識別子を入れない
    # -movflags +faststart: 構造を最適化
    conv_cmd = [
        "ffmpeg", "-i", file_path,
        "-c", "copy",
        "-map_metadata", "-1",
        "-fflags", "+bitexact",
        "-movflags", "+faststart",
        "-y", temp_path
    ]
    
    try:
        result = subprocess.run(conv_cmd, capture_output=True, text=True)
        if result.returncode == 0 and os.path.exists(temp_path):
            os.replace(temp_path, file_path)
            return True
        else:
            print(f"\n[FFmpeg Clean Fail] {os.path.basename(file_path)}")
            if os.path.exists(temp_path): os.remove(temp_path)
            return False
    except Exception as e:
        print(f"\n[FFmpeg Exception] {e}")
        return False

def inject_and_cleanup():
    print(f"💉 強制クリーンアップ注入モードを開始します...\n")

    require_setting_str("BASE_SAVE_DIR")
    require_setting_str("INJECT_NOJSON_DIR")
    s = app_settings.load_settings()
    base_save_dir = str(s["BASE_SAVE_DIR"])
    metadata_dir = os.path.join(base_save_dir, "metadata")
    waiting_dir = base_save_dir
    target_inject_dir = str(s["INJECT_NOJSON_DIR"])
    exiftool_path = s.get("EXIFTOOL_PATH") or ""

    if not os.path.exists(exiftool_path):
        print(f"❌ エラー: exiftool.exeが見つかりません。")
        return

    json_files = glob.glob(os.path.join(metadata_dir, "*.json"))
    total = len(json_files)
    if total == 0:
        print("✅ 処理するJSONがありません。")
        return

    # ファイル一覧をキャッシュ
    target_files = []
    if os.path.exists(target_inject_dir):
        for root, _, files in os.walk(target_inject_dir):
            for f in files:
                if not f.endswith(".json"):
                    target_files.append((f, os.path.join(root, f)))

    waiting_files = [f for f in os.listdir(waiting_dir) if os.path.isfile(os.path.join(waiting_dir, f)) and not f.endswith(".json")]

    injected_count = 0
    skipped_count = 0
    not_found_count = 0

    for idx, json_path in enumerate(json_files, 1):
        base_name = os.path.splitext(os.path.basename(json_path))[0]
        targets = [path for name, path in target_files if is_match(base_name, name)]
        in_waiting = any(is_match(base_name, f) for f in waiting_files)

        print(f"\r⏳ 進行中: {idx}/{total} (成功:{injected_count})", end="", flush=True)

        if not targets and not in_waiting:
            try: os.remove(json_path)
            except: pass
            not_found_count += 1
            continue
        if not targets:
            skipped_count += 1
            continue

        success_any = False
        for media_path in targets:
            # --- 手順1: FFmpegで壊れたメタデータ領域を削ぎ落とす ---
            if media_path.lower().endswith(".mp4"):
                deep_clean_mp4(media_path)

            # --- 手順2: ExifToolで新しいメタデータを書き込む ---
            # ここでさらに強力なオプションを追加
            cmd = [
                exiftool_path,
                "-overwrite_original",
                "-m",         # マイナーエラーを無視
                "-F",         # 壊れたファイルの修復を試みる(Fix)
                f"-UserComment<={json_path}",
                f"-Description<={json_path}",
                f"-XMP:Description<={json_path}",
                f"-Keys:Description<={json_path}",
                media_path
            ]
            
            res = subprocess.run(cmd, capture_output=True)
            if res.returncode == 0:
                success_any = True
            else:
                try: err = res.stderr.decode('cp932')
                except: err = str(res.stderr)
                print(f"\n❌ 失敗 ({os.path.basename(media_path)}): {err.strip()}")

        if success_any:
            try:
                os.remove(json_path)
                injected_count += 1
            except: pass

    print(f"\n\n✨ 完了レポート：成功 {injected_count} / 待機 {skipped_count} / 消失 {not_found_count}")

if __name__ == "__main__":
    inject_and_cleanup()