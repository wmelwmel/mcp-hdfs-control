from __future__ import annotations

from typing import Dict, List


def parse_hdfs_ls(stdout: str) -> List[Dict]:
    lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    items: List[Dict] = []

    for ln in lines:
        if ln.startswith("Found "):
            continue
        parts = ln.split()
        if len(parts) < 8:
            continue

        perm = parts[0]
        repl = parts[1]
        owner = parts[2]
        group = parts[3]
        size = parts[4]
        date = parts[5]
        t = parts[6]
        path = " ".join(parts[7:])

        items.append({
            "perm": perm,
            "replication": repl,
            "owner": owner,
            "group": group,
            "size": int(size) if size.isdigit() else 0,
            "date": date,
            "time": t,
            "path": path,
            "type": "dir" if perm.startswith("d") else "file",
        })

    return items


def parse_hdfs_stat(raw: str) -> Dict:
    parts = raw.strip().split("|")
    if len(parts) != 8:
        return {"raw": raw.strip()}

    name, size, block_size, repl, owner, group, modified, ftype = parts
    return {
        "name": name,
        "size": int(size) if size.isdigit() else 0,
        "block_size": block_size,
        "replication": repl,
        "owner": owner,
        "group": group,
        "modified": modified,
        "type": ftype,
        "raw": raw.strip(),
    }
