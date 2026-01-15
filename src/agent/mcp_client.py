from __future__ import annotations

import json
from typing import Any, Dict


def _tool_to_dict(tool: Any) -> Dict[str, Any]:
    if isinstance(tool, dict):
        return tool
    if hasattr(tool, "model_dump"):
        return tool.model_dump()
    if hasattr(tool, "__dict__"):
        return dict(tool.__dict__)
    return {"name": str(tool), "description": "", "inputSchema": {"type": "object", "properties": {}}}


def mcp_tool_to_openai(tool: Any) -> Dict[str, Any]:
    t = _tool_to_dict(tool)

    name = t.get("name") or t.get("tool_name") or t.get("id")
    description = t.get("description") or ""

    input_schema = t.get("inputSchema") or t.get("input_schema") or t.get("parameters")
    if not input_schema:
        input_schema = {"type": "object", "properties": {}, "additionalProperties": True}

    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": input_schema,
        },
    }


def mcp_result_to_text(result: Any) -> str:
    if hasattr(result, "content"):
        parts = []
        for block in result.content:
            if hasattr(block, "text") and block.text is not None:
                parts.append(block.text)
            else:
                parts.append(str(block))
        return "\n".join(parts)

    try:
        return json.dumps(result, ensure_ascii=False)
    except TypeError:
        return str(result)


def mcp_server_entrypoint() -> str:
    return "mcp_server_bootstrap.py"
