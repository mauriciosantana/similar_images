"""任意の `project_settings.json` で上書き可能な共有設定。

パス系の既定値は None（環境に依存しない）です。実際のパスは
`project_settings.json` で必ず設定してください。

作成手順: config/project_settings.example.json をコピーし、
プロジェクト直下に project_settings.json として保存して編集。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from image_tools.paths import PROJECT_ROOT

SETTINGS_FILE = PROJECT_ROOT / "project_settings.json"

# パスは None。clone した環境でも他人のドライブ・ユーザー名を参照しない。
DEFAULTS: dict[str, Any] = {
    "BASE_SAVE_DIR": None,
    "EXIFTOOL_PATH": None,
    "TEKETOU_TARGET_DIR": None,
    "TEKETOU_TARGETS_FILE": None,
    "INJECT_NOJSON_DIR": None,
    "YOUTUBE_SAVE_DIR": None,
    "PDF2AVIF_INPUT_DIR": None,
    "PDF2AVIF_OUTPUT_DIR": None,
    "PDF2AVIF_DPI": 200,
    "PDF2AVIF_QUALITY": 60,
}

_cache: dict[str, Any] | None = None


def load_settings() -> dict[str, Any]:
    global _cache
    if _cache is not None:
        return _cache
    data = DEFAULTS.copy()
    if SETTINGS_FILE.exists():
        with open(SETTINGS_FILE, encoding="utf-8") as f:
            user = json.load(f)
        for k, v in user.items():
            if v is not None:
                data[k] = v
    _cache = data
    return data


def reload_settings() -> dict[str, Any]:
    global _cache
    _cache = None
    return load_settings()


def missing_settings_message(key: str) -> str:
    return (
        f"設定エラー: 「{key}」が未設定です。\n"
        f"{SETTINGS_FILE} に値を設定するか、"
        f"config/project_settings.example.json をコピーして編集してください。"
    )


def require_setting_str(key: str) -> str:
    v = load_settings().get(key)
    if v is None or (isinstance(v, str) and not str(v).strip()):
        raise SystemExit(missing_settings_message(key))
    return str(v)
