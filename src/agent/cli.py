from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List

from fastmcp import Client
from src.config import agent_settings

from src.agent.llm import make_client, chat_completion
from src.agent.mcp_client import mcp_result_to_text, mcp_server_entrypoint, mcp_tool_to_openai
from src.agent.prompts import SYSTEM_PROMPT
from src.agent.reporting import ActionLog, render_actions_table
from src.mcp_hdfs.constants import RISKY_TOOLS


# def needs_user_confirmation(tool_name: str, args: dict) -> bool:
#     if tool_name in RISKY_TOOLS:
#         return True

#     if tool_name in {"put", "get"} and isinstance(args, dict) and args.get("overwrite") is True:
#         return True

#     return False

# def sanitize_tool_args(tool_name: str, args: dict) -> dict:
#     if not isinstance(args, dict):
#         return {}

#     # Never trust confirm from the model
#     if "confirm" in args:
#         args["confirm"] = False

#     # Never trust overwrite from the model; default to False.
#     if tool_name in {"get", "put"}:
#         if "overwrite" in args:
#             args["overwrite"] = False

#     return args


async def main() -> None:
    print("HDFS control & assist (MCP) - CLI")
    print("Type 'exit' to quit.\n")

    llm_client = make_client()
    server_path = mcp_server_entrypoint()

    async with Client(server_path) as mcp:
        mcp_tools = await mcp.list_tools()
        tools = [mcp_tool_to_openai(t) for t in mcp_tools]

        messages: List[Dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]

        while True:
            user_text = input("you> ").strip()
            if not user_text:
                continue
            if user_text.lower() in {"exit", "quit"}:
                break

            actions: List[ActionLog] = []
            messages.append({"role": "user", "content": user_text})

            for _ in range(10):
                resp = chat_completion(
                    client=llm_client,
                    model=agent_settings.openrouter_model,
                    messages=messages,
                    tools=tools,
                )

                msg = resp.choices[0].message
                tool_calls = msg.tool_calls or []

                if not tool_calls:
                    content = msg.content or ""
                    print(f"\nagent-hdfs> {content}\n")
                    print(render_actions_table(actions))
                    print()
                    messages.append({"role": "assistant", "content": content})
                    break

                messages.append({
                    "role": "assistant",
                    "content": msg.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                        }
                        for tc in tool_calls
                    ],
                })

                for tc in tool_calls:
                    fn = tc.function.name
                    raw_args = tc.function.arguments or "{}"

                    try:
                        args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                    except json.JSONDecodeError:
                        args = {}
                    
                    t0 = time.perf_counter()

                    try:
                        # # args = sanitize_tool_args(fn, args)
                        # if needs_user_confirmation(fn, args):
                        #     print("\n!!!  Risky action requested:")
                        #     print(f"   tool: {fn}")
                        #     print(f"   args: {args}")
                        #     ans = input("Approve? (y/n) > ").strip().lower()

                        #     if ans.lower() not in {"y", "yes"}:
                        #         tool_text = json.dumps({"ok": False, "error": "User denied confirmation"}, ensure_ascii=False)
                        #         actions.append(ActionLog(tool=fn, args=args, ok=False, error="User denied confirmation"))
                        #         print(f"[tool] {fn}({args}) -> denied by user")

                        #         messages.append({
                        #             "role": "tool",
                        #             "tool_call_id": tc.id,
                        #             "name": fn,
                        #             "content": tool_text,
                        #         })
                        #         continue

                        #     if isinstance(args, dict):
                        #         args["confirm"] = True

                        result = await mcp.call_tool(fn, args)
                        tool_text = mcp_result_to_text(result)
                        actions.append(ActionLog(tool=fn, args=args, ok=True))
                        ok = True
                    except Exception as e:
                        tool_text = json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
                        actions.append(ActionLog(tool=fn, args=args, ok=False, error=str(e)))
                        ok = False

                    dt = (time.perf_counter() - t0) * 1000
                    print(f"[tool] {fn}({args}) -> {'ok' if ok else 'error'} in {dt:.1f} ms")

                    messages.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "name": fn,
                        "content": tool_text,
                    })
            else:
                print("\nagent-hdfs> Too many tool steps; stopping.\n")
                print(render_actions_table(actions))
                print()


if __name__ == "__main__":
    asyncio.run(main())
