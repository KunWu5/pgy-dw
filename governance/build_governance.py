"""一键生成全部数据治理产物。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from governance import dictionary, lineage, quality_catalog  # noqa: E402


def build() -> dict:
    return {
        "dictionary": dictionary.build(),
        "lineage": lineage.build(),
        "quality": quality_catalog.build(),
    }


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2))
