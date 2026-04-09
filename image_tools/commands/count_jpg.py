"""`.image_hash_cache.db` を使った高速集計（パス接頭辞は引数または設定で指定）。"""

import argparse
import os
import sqlite3

from image_tools.paths import hash_cache_db
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
    base = s.get("BASE_SAVE_DIR")
    default_prefix = os.path.join(base, "SNS画像") if base else None

    parser = argparse.ArgumentParser(
        description="DBを利用して指定した容量・拡張子のファイル数と合計容量を高速集計します。"
    )
    parser.add_argument("size", type=float, help="最小サイズ（MB）")
    parser.add_argument(
        "--ext",
        nargs="+",
        default=["jpg", "jpeg"],
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
        help="path の接頭辞（省略時は BASE_SAVE_DIR\\SNS画像。BASE_SAVE_DIR 未設定時は必須）",
    )
    args = parser.parse_args()
    prefix = args.prefix
    if prefix is None:
        if not base:
            raise SystemExit(
                missing_settings_message("BASE_SAVE_DIR")
                + "\nまたは --prefix で接頭辞を直接指定してください。"
            )
        prefix = default_prefix
    db_count_large_files(args.db, prefix, args.size, args.ext)


if __name__ == "__main__":
    main()
