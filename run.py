"""
単一エントリポイント。プロジェクト直下で:

  python run.py --help
  python run.py <command> [オプション...]

コマンド一覧は `python run.py` または `python run.py --help` で表示されます。
`python -m image_tools ...` でも同じ動作です。
"""

from __future__ import annotations

import multiprocessing
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))


def main() -> None:
    if len(sys.argv) < 2:
        from image_tools.__main__ import _usage

        _usage()
        sys.exit(2)

    if sys.argv[1] in ("-h", "--help"):
        from image_tools.__main__ import _usage

        _usage()
        sys.exit(0)

    if sys.argv[1] in ("similar", "picker"):
        multiprocessing.freeze_support()

    cmd = sys.argv[1]
    sys.argv = [sys.argv[0], cmd] + sys.argv[2:]

    from image_tools.__main__ import main as cli_main

    cli_main()


if __name__ == "__main__":
    main()
