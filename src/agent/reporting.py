from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ActionLog:
    tool: str
    args: Dict[str, Any]
    ok: bool
    error: Optional[str] = None


def render_actions_table(actions: List[ActionLog]) -> str:
    if not actions:
        return "Actions: (none)"

    lines = ["Actions:"]
    for i, a in enumerate(actions, start=1):
        status = "OK" if a.ok else "ERROR"
        line = f"{i}) {a.tool}({a.args})  {status}"
        if a.error:
            line += f" | {a.error}"
        lines.append(line)
    return "\n".join(lines)
