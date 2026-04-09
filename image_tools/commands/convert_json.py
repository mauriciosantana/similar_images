"""旧 `.image_hash_cache.json` を SQLite に取り込む（スキーマは `similar_images` と共通）。"""

import os
import json
from pathlib import Path

from image_tools.paths import PROJECT_ROOT
from image_tools.cache_db import init_db, SQL_INSERT_OR_REPLACE_IMAGE

JSON_FILENAME = ".image_hash_cache.json"


def main() -> None:
    json_path = PROJECT_ROOT / JSON_FILENAME
    db_path = PROJECT_ROOT / ".image_hash_cache.db"

    if not json_path.exists():
        print(f"❌ エラー: 変換元の {JSON_FILENAME} が見つかりません。")
        return

    print(f"📂 {JSON_FILENAME} を読み込んでいます... (少々お待ちください)")
    try:
        with open(json_path, encoding="utf-8") as f:
            cache = json.load(f)
    except Exception as e:
        print(f"❌ JSONの読み込みに失敗しました: {e}")
        return

    print(f"🗄️ データベース {db_path.name} を準備中...")
    conn, c = init_db(db_path)

    batch_data = []
    added_count = 0
    skipped_count = 0

    print("🔄 変換処理を開始します...")

    for path_str, info in cache.items():
        if not os.path.exists(path_str):
            skipped_count += 1
            continue

        try:
            hash_str = info.get("hash_str", "")
            color_hash_str = info.get("color_hash_str", "")
            pixels = info.get("pixels", 0)
            filesize = info.get("filesize", 0)
            aspect_ratio = info.get("aspect_ratio", 0.0)
            mtime = info.get("mtime", 0.0)
            batch_data.append(
                (
                    path_str,
                    hash_str,
                    color_hash_str,
                    pixels,
                    filesize,
                    aspect_ratio,
                    mtime,
                    0,
                )
            )
            added_count += 1
        except Exception:
            skipped_count += 1
            continue

        if len(batch_data) >= 10000:
            c.executemany(SQL_INSERT_OR_REPLACE_IMAGE, batch_data)
            conn.commit()
            batch_data = []

    if batch_data:
        c.executemany(SQL_INSERT_OR_REPLACE_IMAGE, batch_data)
        conn.commit()

    conn.close()

    print("\n🎉 変換が完了しました！")
    print(f"   ✅ データベースに登録した画像: {added_count} 枚")
    if skipped_count > 0:
        print(f"   ⚠️ 存在しないためスキップした画像: {skipped_count} 枚")


if __name__ == "__main__":
    main()
