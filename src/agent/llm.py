from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI
from src.config import agent_settings


def make_client() -> OpenAI:
    return OpenAI(api_key=agent_settings.openrouter_api_key,
                  base_url="https://openrouter.ai/api/v1")


def chat_completion(
    client: OpenAI,
    model: str,
    messages: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
) -> Any:
    return client.chat.completions.create(
        model=model,
        messages=messages,
        tools=tools,
        tool_choice="auto",
        temperature=0.2,
        extra_headers={
            "HTTP-Referer": "http://localhost",
            "X-Title": "hdfs-mcp-agent",
        },
    )
