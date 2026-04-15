# image_process（image_tools）

画像の類似検出、SNS ダウンロード補助、AVIF 最適化などをまとめた個人用ツール集です。

## 必要な環境

- Python 3.10+（目安）
- 依存パッケージ: `pip install -r requirements.txt`
- **gallery-dl**: `sns` コマンドは PATH 上の `gallery-dl` を subprocess で呼び出します（別途インストール）。
- **ExifTool**: `optimizer` のメタデータコピー用。`project_settings.json` の `EXIFTOOL_PATH` で指定。

## 起動方法

プロジェクト直下（この README があるフォルダ）で:

```text
python run.py --help
python run.py <command> [そのコマンドのオプション...]
python -m image_tools --help
python -m image_tools <command> ...
```

各サブコマンドの詳細は `python run.py similar --help` のように個別に `-h` / `--help` を付けて確認できます。

## 設定

- `config/README.txt` … ファイルの置き場所とコマンド一覧の補足
- **`project_settings.json`（必須・パス系コマンド）**  
  リポジトリ内の既定パスは **未設定（None）** です。`sns` / `inject` / `teketou` / `youtube` / `pdf2avif` などは、プロジェクト直下に `project_settings.json` を置き、サンプル `config/project_settings.example.json` を参考に各パスを書いてください。  
  `optimizer` はルートディレクトリを引数で渡すため必須ではありません。ExifTool を使う場合のみ `EXIFTOOL_PATH` を設定します。  
  `count` は `--prefix` を付ければ `BASE_SAVE_DIR` なしでも動きます。
- 類似画像用は `config.json`（初回実行で自動生成される場合あり）

## optimizer と類似画像キャッシュ（DB）

`optimizer` はディスク上の画像を AVIF などに置き換えるだけで、`.image_hash_cache.db` には書き込みません。大量に変換したあとは、キャッシュを実ファイルと揃えるため **`python run.py similar` を実行してスキャン同期**することを推奨します。

## バッチ実行

`optimizer` は処理後にコンソール待ちする場合があります。待たないときは `--no-pause` を付けてください。

```text
python run.py optimizer "D:\photos" --no-pause
```
