"""`.image_hash_cache.db` を使った高速集計（パス接頭辞は引数または設定で指定）。"""

import argparse
import os
import sqlite3

from image_tools.paths import hash_cache_db
from image_tools.commands.similar import load_config
from image_tools import settings as app_settings
from image_tools.settings import missing_settings_message


def db_count_large_files(db_path: str, path_prefix: str, min_size_mb: float, extensions: list[str]) -> None:
    min_size_bytes = int(min_size_mb * 1024 * 1024)

    target_exts = []
    for ext in extensions:
        ext = ext.lower()
        if not ext.startswith("."):
            ext = "." + ext
        target_exts.append(ext)

    print("🗄️ データベースにアクセスして集計中...")

    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        ext_conditions = " OR ".join([f"path LIKE '%{ext}'" for ext in target_exts])
        query = f"""
            SELECT COUNT(path), SUM(filesize)
            FROM images
            WHERE filesize >= ?
              AND ({ext_conditions})
              AND path LIKE ?
        """
        prefix_like = path_prefix.rstrip("\\/") + "%"
        if os.sep == "\\":
            prefix_like = prefix_like.replace("/", "\\")
        c.execute(query, (min_size_bytes, prefix_like))
        result = c.fetchone()
        conn.close()

    except sqlite3.OperationalError:
        print(f"❌ エラー: データベースが見つからないか開けません。\nパス: {db_path}")
        return
    except Exception as e:
        print(f"❌ 予期せぬエラーが発生しました: {e}")
        return

    count = result[0] if result[0] is not None else 0
    total_size_bytes = result[1] if result[1] is not None else 0

    total_size_mb = total_size_bytes / (1024 * 1024)
    total_size_gb = total_size_bytes / (1024 * 1024 * 1024)

    print("\n=== 超高速集計結果 ===")
    print(f"対象領域: {path_prefix} (データベース経由)")
    print(f"条件: {min_size_mb} MB 以上の {', '.join(target_exts)} ファイル")
    print(f"該当件数: {count} 件")

    if total_size_mb >= 1000:
        print(f"合計容量: 約 {total_size_gb:.2f} GB ({total_size_mb:.2f} MB)")
    else:
        print(f"合計容量: 約 {total_size_mb:.2f} MB")


def main() -> None:
    s = app_settings.load_settings()
    config = load_config()
    
    # 集計の優先順位: 1. config.jsonのTARGET_DIRS, 2. project_settings.jsonのBASE_SAVE_DIR
    target_dirs = config.get("TARGET_DIRS", [])
    base_save_dir = s.get("BASE_SAVE_DIR")
    default_prefix = target_dirs[0] if target_dirs else base_save_dir

    parser = argparse.ArgumentParser(
        description="DBを利用して指定した容量・拡張子のファイル数と合計容量を高速集計します。"
    )
    parser.add_argument("size", type=float, nargs="?", default=0.0, help="最小サイズ（MB）（デフォルト: 0.0）")
    parser.add_argument(
        "--ext",
        nargs="+",
        default=["jpg", "jpeg", "png"],
        help="対象拡張子（例: --ext png webp）",
    )
    parser.add_argument(
        "--db",
        default=str(hash_cache_db()),
        help="SQLite のパス（省略時はプロジェクトの類似画像キャッシュ）",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="集計対象のパス接頭辞 (例: E:\\gaz 画像\\SNS画像)",
    )
    args = parser.parse_args()

    prefix = args.prefix or default_prefix

    if not prefix:
        raise SystemExit(
            "❌ エラー: 集計対象のパス（prefix）が特定できません。\n"
            "config.json の TARGET_DIRS を設定するか、--prefix でパスを指定してください。"
        )

    db_count_large_files(args.db, prefix, args.size, args.ext)


if __name__ == "__main__":
    main()