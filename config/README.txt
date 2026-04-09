================================================================================
設定・データファイルの置き場所（プロジェクト直下 = ima image_process）
================================================================================

【このフォルダ config/】
  project_settings.example.json … サンプル。コピーして「一つ上の階層」に
                                   project_settings.json として保存し、パスを記入。
                                   コード内の既定値は未設定のため、sns / inject / teketou /
                                   youtube / pdf2avif 等では事実上必須です。

  similar_config.example.json   … 類似画像ツール用 config.json の例。
                                   実際に使うときは親フォルダに config.json として置きます
                                   （無い場合は初回実行時に自動生成されます）。

【プロジェクト直下（実行時に読み書きされるファイル）】
  config.json              … 類似画像（run.py similar）の対象フォルダなど
  project_settings.json    … パス系コマンド用（上記サンプルをコピーして編集）
  .image_hash_cache.db     … 類似画像のハッシュキャッシュ（SQLite）
  targets.txt              … SNS ダウンロード対象アカウント
  cookies.txt              … gallery-dl 用
  download_history.sqlite3 … gallery-dl アーカイブ
  completed_accounts.txt   … SNS 処理済みマーカー
  account_names.json       … アカウント表示名キャッシュ
  youtube_history.txt      … yt-dlp アーカイブ

【起動方法】
  python run.py --help    … コマンド一覧と簡単な説明
  python run.py similar
  python run.py sns
  python run.py           … コマンド省略時も一覧表示（終了コード 2）

【optimizer を使ったあと】
  optimizer は .image_hash_cache.db を更新しません。
  類似画像キャッシュを実ファイルと一致させるには、変換後に
    python run.py similar
  を実行してスキャン同期してください。

================================================================================
