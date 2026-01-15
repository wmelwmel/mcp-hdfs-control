from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from src.config import mcp_settings
from src.mcp_hdfs.constants import AUDIT_TRIM_CHARS
from src.mcp_hdfs.models import PermDiff, PermSnapshot
import os
from pathlib import Path


def init_audit_log() -> None:
    path = Path(mcp_settings.mcp_audit_log)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


@dataclass
class AuditRecord:
    ts: str
    tool: str
    risk: Optional[str] = None
    args: Dict[str, Any] = None
    docker_cmd: List[str] = None
    ok: bool = False
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    user: Optional[str] = "unknown"
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    diff: Optional[Dict[str, Any]] = None


def write_audit(rec: AuditRecord) -> None:
    rec.stdout = (rec.stdout or "")[-AUDIT_TRIM_CHARS:]
    rec.stderr = (rec.stderr or "")[-AUDIT_TRIM_CHARS:]
    with open(mcp_settings.mcp_audit_log, "a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")


def compute_perm_diff(before: PermSnapshot, after: PermSnapshot) -> PermDiff:
    changes: Dict[str, List[str]] = {}
    if before.perm != after.perm:
        changes["perm"] = [before.perm, after.perm]
    if before.owner != after.owner:
        changes["owner"] = [before.owner, after.owner]
    if before.group != after.group:
        changes["group"] = [before.group, after.group]
    return PermDiff(changed=bool(changes), changes=changes)
