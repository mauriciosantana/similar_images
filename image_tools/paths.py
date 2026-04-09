"""プロジェクトルート（`ima image_process`）を基準にしたパス定義。

実行時の設定・データは多くが PROJECT_ROOT 直下。
サンプル JSON は config/ を参照（説明は config/README.txt）。
"""

from pathlib import Path

# image_tools/ の親 = リポジトリ／作業ディレクトリのルート
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
CONFIG_EXAMPLES_DIR: Path = PROJECT_ROOT / "config"


def hash_cache_db() -> Path:
    return PROJECT_ROOT / ".image_hash_cache.db"


def config_json() -> Path:
    return PROJECT_ROOT / "config.json"
