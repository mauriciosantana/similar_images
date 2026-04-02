import os
import json
import sqlite3
from pathlib import Path

# ファイル名の設定
JSON_FILENAME = ".image_hash_cache.json"
DB_FILENAME = ".image_hash_cache.db"

def main():
    script_dir = Path(__file__).resolve().parent
    json_path = script_dir / JSON_FILENAME
    db_path = script_dir / DB_FILENAME

    if not json_path.exists():
        print(f"❌ エラー: 変換元の {JSON_FILENAME} が見つかりません。")
        return

    print(f"📂 {JSON_FILENAME} を読み込んでいます... (少々お待ちください)")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)
    except Exception as e:
        print(f"❌ JSONの読み込みに失敗しました: {e}")
        return

    print(f"🗄️ データベース {DB_FILENAME} を準備中...")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # データベースのテーブル作成（メインスクリプトと同じ構造）
    c.execute('''
        CREATE TABLE IF NOT EXISTS images (
            path TEXT PRIMARY KEY,
            hash_str TEXT,
            color_hash_str TEXT,
            pixels INTEGER,
            filesize INTEGER,
            aspect_ratio REAL,
            mtime REAL
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_exact ON images(hash_str, color_hash_str, pixels, filesize)')
    conn.commit()

    batch_data = []
    added_count = 0
    skipped_count = 0

    print("🔄 変換処理を開始します...")
    
    for path_str, info in cache.items():
        # 【重要】ファイルが実際に見つからない場合はスキップしてエラーを防ぐ
        if not os.path.exists(path_str):
            skipped_count += 1
            continue
            
        try:
            # JSONから必要なデータを抽出
            hash_str = info.get('hash_str', '')
            color_hash_str = info.get('color_hash_str', '')
            pixels = info.get('pixels', 0)
            filesize = info.get('filesize', 0)
            aspect_ratio = info.get('aspect_ratio', 0.0)
            mtime = info.get('mtime', 0.0)
            
            # データベースに渡す形式にまとめる
            batch_data.append((path_str, hash_str, color_hash_str, pixels, filesize, aspect_ratio, mtime))
            added_count += 1
        except Exception:
            skipped_count += 1
            continue

        # メモリ節約と書き込み速度向上のため、1万件ごとにデータベースへ保存
        if len(batch_data) >= 10000:
            c.executemany('INSERT OR REPLACE INTO images VALUES (?,?,?,?,?,?,?)', batch_data)
            conn.commit()
            batch_data = []

    # 残りのデータを最後に書き込み
    if batch_data:
        c.executemany('INSERT OR REPLACE INTO images VALUES (?,?,?,?,?,?,?)', batch_data)
        conn.commit()

    conn.close()

    print("\n🎉 変換が完了しました！")
    print(f"   ✅ データベースに登録した画像: {added_count} 枚")
    if skipped_count > 0:
        print(f"   ⚠️ 存在しないためスキップした画像: {skipped_count} 枚")
    print("\n※ これで前回お渡しした最新版のメインスクリプトを、ハッシュ計算をスキップして即座に動かせるようになりました。")

if __name__ == "__main__":
    main()