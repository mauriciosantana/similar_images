"""CLI エントリ: `python -m image_tools <command>` または `python run.py <command>`。"""

from __future__ import annotations

import sys
from typing import Callable


def usage_text() -> str:
    return (
        "使い方:\n"
        "  python run.py --help\n"
        "  python run.py <command> [オプション...]\n"
        "  python -m image_tools <command> [オプション...]\n"
        "（プロジェクトフォルダで実行）\n\n"
        "コマンド一覧:\n"
        "  similar          類似画像チェッカー (-m p で選別, -P で継続選別, -E でサブフォルダごと)\n"
        "  picker           画像選別システム (similar -m p の短縮コマンド)\n"
        "  sns              SNS メディア一括ダウンロード (gallery-dl)\n"
        "  optimizer        画像・圧縮ファイル最適化\n"
        "  teketou          SNS画像フォルダの手動整理\n"
        "  inject           nojson 向けメタデータ注入\n"
        "  youtube          YouTube ダウンロード (yt-dlp)\n"
        "  convert-json     旧 JSON キャッシュ → SQLite\n"
        "  count            キャッシュDBから容量集計\n"
        "  pdf2avif         PDF → AVIF\n\n"
        "各コマンドの詳細: python run.py <command> --help\n"
        "設定ファイルの説明は config\\README.txt を参照。\n"
    )


def _usage() -> None:
    print(usage_text())


def _run_similar() -> None:
    from image_tools.commands.similar import main as run

    run()


def _run_picker() -> None:
    from image_tools.commands.similar import main as run
    import sys
    # picker コマンドとして呼ばれた場合、内部的に similar の picker モードとして引数を偽装する
    sys.argv.insert(1, "-m")
    sys.argv.insert(2, "p")
    run()


def _run_optimizer() -> None:
    from image_tools.commands.optimizer import main as run

    run()


def _run_teketou() -> None:
    from image_tools.commands.teketou import TARGET_DIR, organize_media_files

    organize_media_files(TARGET_DIR)


def _run_inject() -> None:
    from image_tools.commands.inject_json import inject_and_cleanup

    inject_and_cleanup()


def _run_youtube() -> None:
    from image_tools.commands.youtube import main as run

    run()


def _run_convert_json() -> None:
    from image_tools.commands.convert_json import main as run

    run()


def _run_count() -> None:
    from image_tools.commands.count import main as run

    run()


def _run_pdf2avif() -> None:
    from image_tools.commands.pdf_to_avif import main as run

    run()


def _run_sns() -> None:
    from image_tools.commands.sns_download import main as run
    run()


_COMMAND_HANDLERS: dict[str, Callable[[], None]] = {
    "similar": _run_similar,
    "picker": _run_picker,
    "optimizer": _run_optimizer,
    "teketou": _run_teketou,
    "inject": _run_inject,
    "youtube": _run_youtube,
    "convert-json": _run_convert_json,
    "count": _run_count,
    "pdf2avif": _run_pdf2avif,
}


def main() -> None:
    if len(sys.argv) < 2:
        _usage()
        sys.exit(2)

    first = sys.argv[1]
    if first in ("-h", "--help"):
        _usage()
        sys.exit(0)

    cmd = first
    rest = sys.argv[2:]
    sys.argv = [sys.argv[0]] + rest

    if cmd == "sns":
        _run_sns()
        return

    handler = _COMMAND_HANDLERS.get(cmd)
    if handler is None:
        print(f"不明なコマンド: {cmd}\n")
        _usage()
        sys.exit(2)

    handler()


if __name__ == "__main__":
    main()
