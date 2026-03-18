from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def payload_checksum(payload: Any) -> str:
    content = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or "unknown"
