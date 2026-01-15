from __future__ import annotations

import subprocess
import time
from typing import List, Optional, Tuple

from src.config import mcp_settings
from src.mcp_hdfs.constants import ALLOWED_HDFS_DFS


def build_hdfs_dfs_cmd(subcommand: str, args: List[str]) -> List[str]:
    """
    Build a safe `hdfs dfs` command using allow-list of subcommands.
    Prevents accidental execution of arbitrary HDFS commands.
    """
    if subcommand not in ALLOWED_HDFS_DFS:
        raise ValueError(f"Forbidden hdfs dfs subcommand: {subcommand}")
    return ["hdfs", "dfs", f"-{subcommand}", *args]


def run_docker_exec(cmd: List[str]) -> Tuple[int, str, str, List[str]]:
    docker_cmd = ["docker", "exec", mcp_settings.hdfs_namenode_container] + cmd

    last_exc: Optional[Exception] = None
    for attempt in range(mcp_settings.mcp_retries + 1):
        try:
            p = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                timeout=mcp_settings.mcp_timeout_sec,
            )
            return p.returncode, p.stdout, p.stderr, docker_cmd
        except (subprocess.TimeoutExpired, OSError) as e:
            last_exc = e
            if attempt < mcp_settings.mcp_retries:
                time.sleep(0.5 * (2 ** attempt))
            else:
                raise RuntimeError(
                    f"Command failed after retries: {docker_cmd}. Last error: {e}"
                ) from e

    raise RuntimeError(f"Unexpected failure: {last_exc}")
