import os
import shutil
import re

from image_tools import settings as app_settings
from image_tools.settings import require_setting_str

_S = app_settings.load_settings()
TARGET_DIR = _S.get("TEKETOU_TARGET_DIR") or ""
TARGETS_FILE = _S.get("TEKETOU_TARGETS_FILE") or ""

# ＝＝＝ 処理対象の拡張子 ＝＝＝
MEDIA_EXTS = {
    # 動画
    ".mp4", ".mov", ".webm", ".avi", ".mkv", ".m4v",
    # 画像
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp"
}

def get_folder_mapping(targets_file):
    """targets.txtのコメントと連続したアカウント群からフォルダ名を生成する"""
    mapping = {}
    if not os.path.exists(targets_file):
        return mapping
        
    with open(targets_file, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()
        
    groups = []
    temp_group = []
    temp_comment = ""
    
    for line in lines:
        line = line.strip()
        if not line:
            # 空行でグループとコメントをリセット
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
            # 「#twitter:xxx」のような無効化されたアカウント指定は名前として拾わない
            if re.match(r"^(twitter|instagram|pixiv|twtag)\s*:", c, re.IGNORECASE):
                continue
            # 飾りだけのコメントを除外
            if c:
                temp_comment = c
        else:
            parts = line.split(":", 1)
            if len(parts) == 2:
                platform = parts[0].strip().lower()
                account_id = parts[1].strip()
                if account_id.startswith('"') and account_id.endswith('"'):
                    account_id = account_id[1:-1]
                
                # ★比較を確実にするため、アカウントIDを小文字に統一
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
            # コメントが無い場合はマッピングを作成しない
            continue
            
        # アカウントIDは入れず、コメント名だけのフォルダ名にする
        folder_name = comment
        folder_name = re.sub(r'[\\/:*?"<>|]', '_', folder_name)
        
        for account_key in group:
            mapping[account_key] = folder_name
            
    return mapping

def organize_media_files(target_dir):
    require_setting_str("TEKETOU_TARGET_DIR")
    require_setting_str("TEKETOU_TARGETS_FILE")
    if not os.path.exists(target_dir):
        print(f"❌ 指定されたフォルダが存在しません: {target_dir}")
        return

    folder_mapping = get_folder_mapping(TARGETS_FILE)
    
    move_count = 0
    skip_count = 0
    unknown_count = 0

    print(f"📁 整理（再帰探索）を開始します: {target_dir}\n")

    # 【1】ファイルの移動処理（サブフォルダもすべて探索）
    for root, dirs, files in os.walk(target_dir):
        for item in files:
            item_path = os.path.join(root, item)
            
            # 拡張子が対象のメディアファイルかチェック
            ext = os.path.splitext(item)[1].lower()
            if ext not in MEDIA_EXTS:
                continue
                
            account_folder = None
            
            # 命名規則からプラットフォームとアカウントIDを抽出
            match = re.search(r"^(tw|twtag|ig|px)_(.+?)_\d{8}_\d{6}_", item)
            if not match:
                # 日付時刻が含まれない形式 (アンダーバーを含むIDに対応)
                match = re.search(r"^(tw|twtag|ig|px)_(.+)_[^_]+\.[a-zA-Z0-9]+$", item)
                
            if match:
                prefix = match.group(1).lower()
                raw_account_id = match.group(2)
                
                # ★辞書検索用に小文字化して照合
                account_key = f"{prefix}_{raw_account_id.lower()}"
                
                if account_key in folder_mapping:
                    account_folder = folder_mapping[account_key]
                else:
                    # 該当しない場合は元の大文字小文字を維持したままフォルダ名にする
                    account_folder = f"{prefix}_{raw_account_id}"
                    account_folder = re.sub(r'[\\/:*?"<>|]', '_', account_folder)
            else:
                account_folder = "Unknown"
                unknown_count += 1
                
            # 移動先のディレクトリパスとファイルパスを作成
            dst_dir = os.path.join(target_dir, account_folder)
            dst_path = os.path.join(dst_dir, item)
            
            # すでに正しいフォルダにある場合は移動しない
            if os.path.normpath(item_path) == os.path.normpath(dst_path):
                skip_count += 1
                continue
                
            os.makedirs(dst_dir, exist_ok=True)
            
            # ファイルを移動
            try:
                shutil.move(item_path, dst_path)
                move_count += 1
            except Exception as e:
                print(f"❌ 移動エラー ({item}): {e}")

    # 【2】空になった旧フォルダの削除処理
    delete_count = 0
    # topdown=False にすることで、深い階層（サブフォルダ）から順番にチェックして削除可能
    for root, dirs, files in os.walk(target_dir, topdown=False):
        if root == target_dir:
            continue # 大元のターゲットフォルダ自体は削除しない
            
        # フォルダ内が空っぽなら削除する
        if not os.listdir(root):
            try:
                os.rmdir(root)
                delete_count += 1
            except Exception:
                pass

    print(f"🎉 整理が完了しました！")
    print(f"   ✅ 新たに移動したファイル: {move_count}件")
    print(f"   ⏭️ 既に正しいフォルダにあるためスキップ: {skip_count}件")
    if unknown_count > 0:
        print(f"   ⚠️ 命名規則外（Unknownへ移動）: {unknown_count}件")
    if delete_count > 0:
        print(f"   🗑️ 空になった旧フォルダを削除しました: {delete_count}件")

if __name__ == "__main__":
    organize_media_files(TARGET_DIR)