import os
import glob
import subprocess

from image_tools.utils.exiftool_wrapper import FastExifTool
from image_tools.utils.media_utils import deep_clean_mp4
from image_tools import settings as app_settings
from image_tools.settings import require_setting_str

_S = app_settings.load_settings()
_BASE = _S.get("BASE_SAVE_DIR") or ""
BASE_SAVE_DIR = _BASE
METADATA_DIR = os.path.join(_BASE, "metadata") if _BASE else ""
EXIFTOOL_PATH = _S.get("EXIFTOOL_PATH") or ""
WAITING_DIR = BASE_SAVE_DIR
TARGET_INJECT_DIR = _S.get("INJECT_NOJSON_DIR") or ""

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
            return False, "ExifTool not found"
        
        for arg in args:
            self.process.stdin.write(arg + "\n")
        self.process.stdin.write("-execute\n")
        self.process.stdin.flush()
        
        output = ""
        while True:
            line = self.process.stdout.readline()
            if not line or line.strip() == "{ready}": break
            output += line
        return "files updated" in output or "image files read" in output, output

    def stop(self):
        if self.process:
            try:
                self.process.stdin.write("-stay_open\nFalse\n")
                self.process.stdin.flush()
                self.process.wait(timeout=2)
            except:
                self.process.kill()

def is_match(base_name, file_name):
    name_idx = file_name.find(base_name)
    if name_idx == -1: return False
    next_char_idx = name_idx + len(base_name)
    if next_char_idx < len(file_name):
        if file_name[next_char_idx].isalnum(): return False 
    return True

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

    fast_exiftool = FastExifTool(exiftool_path)

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
            args = [
                "-overwrite_original",
                "-m",         # マイナーエラーを無視
                "-F",         # 壊れたファイルの修復を試みる(Fix)
                f"-UserComment<={json_path}",
                f"-Description<={json_path}",
                f"-XMP:Description<={json_path}",
                f"-Keys:Description<={json_path}",
                media_path
            ]
            
            success, output = fast_exiftool.execute(*args)
            if success:
                success_any = True
            else:
                print(f"\n❌ 失敗 ({os.path.basename(media_path)}): {output.strip()}")

        if success_any:
            try:
                os.remove(json_path)
                injected_count += 1
            except: pass

    fast_exiftool.stop()
    print(f"\n\n✨ 完了レポート：成功 {injected_count} / 待機 {skipped_count} / 消失 {not_found_count}")

if __name__ == "__main__":
    inject_and_cleanup()