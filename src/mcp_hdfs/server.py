from __future__ import annotations

from fastmcp import FastMCP
from src.config import mcp_settings

from src.mcp_hdfs.audit import AuditRecord, compute_perm_diff, now_iso, write_audit, init_audit_log
from src.mcp_hdfs.hdfs_exec import run_docker_exec, build_hdfs_dfs_cmd
from src.mcp_hdfs.constants import MAX_LIST_LIMIT, SAFE_TOOLS, RISKY_TOOLS
from src.mcp_hdfs.models import (
    ChmodRequest, ChownRequest,
    GetRequest, ListRequest, ListResponseData, LsItem,
    MkdirRequest,
    PermSnapshot,
    PutRequest,
    StatRequest, StatResponseData,
    ToolError, ToolOk,
)
from src.mcp_hdfs.parsers import parse_hdfs_ls, parse_hdfs_stat


mcp = FastMCP("mcp-hdfs")


def tool_risk(tool_name: str) -> str:
    if tool_name in SAFE_TOOLS:
        return "safe"
    if tool_name in RISKY_TOOLS:
        return "risky"
    return "unknown"


def _perm_snapshot(path: str) -> PermSnapshot | None:
    """
    Snapshot permissions/owner/group for EXACT path (file or directory),
    using `hdfs dfs -stat` (NOT `-ls`), so directories work correctly.
    """
    fmt = "%A|%u|%g|%F"  # perm | owner | group | type
    try:
        args = build_hdfs_dfs_cmd("stat", [fmt, path])
        code, out, err, docker_cmd = run_docker_exec(args)
        if code != 0:
            return None

        raw = out.strip()
        parts = raw.split("|")
        if len(parts) != 4:
            return None

        perm, owner, group, ftype = parts
        return PermSnapshot(
            path=path,
            perm=perm,
            owner=owner,
            group=group,
            type=ftype,
        )
    except Exception:
        return None


@mcp.tool()
def list(path: str = "/", recursive: bool = False, limit: int = 200, offset: int = 0) -> ToolOk:
    """
    List directory contents in HDFS with paging.

    Args:
        path: HDFS directory path to list.
        recursive: If True, list recursively.
        limit: Max number of items to return in this page (paging).
        offset: Start index for paging.

    Safety: SAFE (read-only).
    Idempotency: Yes (repeating does not change state).

    Returns:
        ToolOk with items[] and next_offset (or null if last page).
    """
    # Покажи содержимое /data/raw
    # Покажи первый 1 файл в /data/raw
    # Покажи следующие файлы (offset=1)
    req = ListRequest(path=path, recursive=recursive, limit=min(limit, MAX_LIST_LIMIT), offset=offset)

    ls_args = (["-R"] if req.recursive else []) + [req.path]
    args = build_hdfs_dfs_cmd("ls", ls_args)

    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="list",
        risk=tool_risk("list"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -ls failed")).model_dump()

    parsed = parse_hdfs_ls(out)
    page = parsed[req.offset:req.offset + req.limit]
    next_offset = req.offset + req.limit if (req.offset + req.limit) < len(parsed) else None

    data = ListResponseData(
        items=[LsItem(**x) for x in page],
        next_offset=next_offset,
        total_in_page=len(page),
    )
    return ToolOk(data=data.model_dump()).model_dump()


@mcp.tool()
def stat(path: str) -> ToolOk:
    """
    Get metadata for a single HDFS path (file or directory).

    Args:
      path: HDFS path.

    Safety: SAFE (read-only).
    Idempotency: Yes.

    Returns:
      ToolOk with size, owner, group, permissions, type, mtime (best-effort).
    """
    # Какой размер у /data/raw/sample.csv?
    # Кто владелец /data/raw?
    req = StatRequest(path=path)
    fmt = "%n|%b|%o|%r|%u|%g|%y|%F"

    args = build_hdfs_dfs_cmd("stat", [fmt, req.path])
    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="stat",
        risk=tool_risk("stat"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -stat failed")).model_dump()

    parsed = parse_hdfs_stat(out)
    if "raw" in parsed and len(parsed) == 1:
        return ToolOk(data=parsed).model_dump()

    data = StatResponseData(**parsed)
    return ToolOk(data=data.model_dump()).model_dump()


@mcp.tool()
def mkdir(path: str, parents: bool = True, confirm: bool = False) -> ToolOk | ToolError:
    """
    Create a directory in HDFS.

    Args:
      path: Directory path to create.
      parents: If True, create parent directories (like mkdir -p).
      confirm: Must be True if strict_confirm policy is enabled.

    Safety: RISKY-ish (writes to filesystem). May require confirmation by policy.
    Idempotency: Generally yes with parents=True; repeated calls should not break state.

    Returns:
      ToolOk with created path, or ToolError on failure.
    """
    # Создай директорию /data/by_llm
    # Создай /data/a/b/c
    req = MkdirRequest(path=path, parents=parents, confirm=confirm)

    if mcp_settings.strict_confirm and not req.confirm:
        return ToolError(
            error="mkdir requires confirm=true by policy",
            hint="Call mkdir with confirm=true"
        ).model_dump()

    mkdir_args = (["-p"] if req.parents else []) + [req.path]
    args = build_hdfs_dfs_cmd("mkdir", mkdir_args)

    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="mkdir",
        risk=tool_risk("mkdir"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -mkdir failed")).model_dump()

    return ToolOk(data={"path": req.path}).model_dump()


@mcp.tool()
def put(local_path: str, hdfs_path: str, overwrite: bool = False, confirm: bool = False) -> ToolOk | ToolError:
    """
    Upload a local file into HDFS.

    Args:
      local_path: Path inside the namenode container (e.g. /tmp/a.txt).
      hdfs_path: Destination path in HDFS.
      overwrite: If True, overwrite destination if exists (requires confirm=True).
      confirm: Explicit confirmation required when overwrite=True.

    Safety: RISKY (write). Overwrite is destructive.
    Idempotency: No for overwrite; for overwrite=False it is safe if file does not exist.

    Returns:
      ToolOk with destination path, or ToolError on failure.
    """
    # Загрузи файл /tmp/a.txt в /data/by_llm/a2.txt
    # Попробуй перезаписать /data/by_llm/a2.txt
    req = PutRequest(local_path=local_path, hdfs_path=hdfs_path, overwrite=overwrite, confirm=confirm)

    if req.overwrite and not req.confirm:
        return ToolError(
            error="overwrite=true requires confirm=true",
            hint="Set confirm=true to allow overwrite"
        ).model_dump()

    put_args = (["-f"] if req.overwrite else []) + [req.local_path, req.hdfs_path]
    args = build_hdfs_dfs_cmd("put", put_args)

    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="put",
        risk=tool_risk("put"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -put failed")).model_dump()

    return ToolOk(data={"hdfs_path": req.hdfs_path}).model_dump()


@mcp.tool()
def get(hdfs_path: str, local_path: str, overwrite: bool = False, confirm: bool = False) -> ToolOk | ToolError:
    """
    Download a file from HDFS into the namenode container local filesystem.

    Args:
      hdfs_path: Source path in HDFS.
      local_path: Destination path inside namenode container (e.g. /tmp/file.csv).
      overwrite: If True, overwrite local destination (requires confirm=True).
      confirm: Explicit confirmation required when overwrite=True.

    Safety: SAFE for HDFS (read-only), but can be destructive locally when overwrite=True.
    Idempotency: Yes when overwrite=False and file exists -> fails, state unchanged.

    Returns:
      ToolOk with local_path, or ToolError on failure.
    """
    # Скачай /data/raw/a.txt в /tmp/a_dl.txt
    # Скачай ещё раз в тот же путь
    req = GetRequest(hdfs_path=hdfs_path, local_path=local_path, overwrite=overwrite, confirm=confirm)

    if req.overwrite and not req.confirm:
        return ToolError(
            error="overwrite=true requires confirm=true",
            hint="Set confirm=true to allow overwrite"
        ).model_dump()

    get_args = (["-f"] if req.overwrite else []) + [req.hdfs_path, req.local_path]
    args = build_hdfs_dfs_cmd("get", get_args)

    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="get",
        risk=tool_risk("get"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -get failed")).model_dump()

    return ToolOk(data={"local_path": req.local_path}).model_dump()


@mcp.tool()
def chmod(path: str, mode: str, recursive: bool = False, confirm: bool = False) -> ToolOk | ToolError:
    """
    Change permissions for a path in HDFS.

    Args:
      path: HDFS path.
      mode: Permission mode (e.g. 755, 777).
      recursive: Apply recursively (-R).
      confirm: Must be True (risky operation).

    Safety: RISKY (permission change).
    Idempotency: Repeating the same chmod results in no further changes.

    Audit:
      Logs before/after permission snapshot and a diff.

    Returns:
      ToolOk with diff, or ToolError on failure.
    """
    # Поставь 755 на /data/raw
    # Поставь 777 на /data/test_perm
    req = ChmodRequest(path=path, mode=mode, recursive=recursive, confirm=confirm)

    if not req.confirm:
        return ToolError(
            error="chmod is risky and requires confirm=true",
            hint="Call chmod with confirm=true"
        ).model_dump()

    before = _perm_snapshot(req.path)

    chmod_args = (["-R"] if req.recursive else []) + [req.mode, req.path]
    args = build_hdfs_dfs_cmd("chmod", chmod_args)

    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    after = _perm_snapshot(req.path)
    diff = None
    if before and after:
        diff = compute_perm_diff(before, after).model_dump()

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="chmod",
        risk=tool_risk("chmod"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
        before=(before.model_dump() if before else None),
        after=(after.model_dump() if after else None),
        diff=diff,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -chmod failed")).model_dump()

    return ToolOk(data={"path": req.path, "diff": diff}).model_dump()


@mcp.tool()
def chown(path: str, 
          owner: str, 
          group: str | None = None, 
          recursive: bool = False, 
          confirm: bool = False) -> ToolOk | ToolError:
    """
    Change owner/group for a path in HDFS.

    Args:
      path: HDFS path.
      owner: New owner.
      group: Optional new group.
      recursive: Apply recursively (-R).
      confirm: Must be True (risky operation).

    Safety: RISKY (ownership change).
    Idempotency: Repeating the same chown results in no further changes.

    Audit:
      Logs before/after snapshot and a diff.

    Returns:
      ToolOk with diff, or ToolError on failure.
    """
    # Поменяй владельца /data/test_perm на root
    # Поменяй владельца и группу /data/test_perm на root:supergroup
    req = ChownRequest(path=path, owner=owner, group=group, recursive=recursive, confirm=confirm)

    if not req.confirm:
        return ToolError(
            error="chown is risky and requires confirm=true",
            hint="Call chown with confirm=true"
        ).model_dump()

    target = req.owner if req.group is None else f"{req.owner}:{req.group}"
    before = _perm_snapshot(req.path)

    chown_args = (["-R"] if req.recursive else []) + [target, req.path]
    args = build_hdfs_dfs_cmd("chown", chown_args)

    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    after = _perm_snapshot(req.path)
    diff = None
    if before and after:
        diff = compute_perm_diff(before, after).model_dump()

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="chown",
        risk=tool_risk("chown"),
        args=req.model_dump(),
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
        before=(before.model_dump() if before else None),
        after=(after.model_dump() if after else None),
        diff=diff,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -chown failed")).model_dump()

    return ToolOk(data={"path": req.path, "diff": diff}).model_dump()


@mcp.tool()
def getquota(path: str) -> ToolOk | ToolError:
    """
    Get quota and usage information for an HDFS path.

    Args:
      path: HDFS path.

    Safety: SAFE (read-only).
    Idempotency: Yes.

    Returns:
      ToolOk with quota and usage data.
    """
    # Покажи квоты и использование для /data
    # Какая квота на /data/raw?
    args = ["hdfs", "dfs", "-count", "-q", path]
    code, out, err, docker_cmd = run_docker_exec(args)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="getquota",
        risk=tool_risk("getquota"),
        args={"path": path},
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "hdfs dfs -count -q failed")).model_dump()

    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    return ToolOk(data={"path": path, "raw": out, "line": (lines[-1] if lines else "")}).model_dump()


@mcp.tool()
def setquota(path: str, 
             namespace_quota: int | None = None, 
             space_quota: str | None = None, 
             confirm: bool = False) -> ToolOk | ToolError:
    """
    Set quota limits for an HDFS path.

    Args:
      path: HDFS path.
      namespace_quota: Max number of files/directories (optional).
      space_quota: Space quota value (optional).
      confirm: Must be True to apply changes.

    Safety: RISKY (can block writes). Requires explicit confirmation.
    Idempotency: Reapplying the same quota is idempotent.

    Returns:
      ToolOk with applied quota values.
    """
    # Поставь квоту 1000 файлов на /data/raw
    # Поставь квоту по месту 1g на /data/raw
    if not confirm:
        return ToolError(error="setquota requires confirm=true", hint="Approve and call again with confirm=true").model_dump()

    cmds = []
    if namespace_quota is not None:
        cmds.append(["hdfs", "dfsadmin", "-setQuota", str(namespace_quota), path])
    if space_quota is not None:
        cmds.append(["hdfs", "dfsadmin", "-setSpaceQuota", str(space_quota), path])

    if not cmds:
        return ToolError(error="No quota parameters provided").model_dump()

    all_out, all_err = "", ""
    ok = True
    last_cmd = []
    last_code = 0

    for c in cmds:
        code, out, err, docker_cmd = run_docker_exec(c)
        last_cmd = docker_cmd
        last_code = code
        all_out += out
        all_err += err
        if code != 0:
            ok = False
            break

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="setquota",
        risk=tool_risk("setquota"),
        args={"path": path, "namespace_quota": namespace_quota, "space_quota": space_quota, "confirm": confirm},
        docker_cmd=last_cmd,
        ok=ok,
        exit_code=last_code,
        stdout=all_out,
        stderr=all_err,
    ))

    if not ok:
        return ToolError(error=(all_err.strip() or "setquota failed")).model_dump()

    return ToolOk(data={"path": path, "namespace_quota": namespace_quota, "space_quota": space_quota}).model_dump()


@mcp.tool()
def snapshot_create(path: str, name: str | None = None, confirm: bool = False) -> ToolOk | ToolError:
    """
    Create a snapshot for an HDFS directory.

    Args:
      path: HDFS directory path.
      name: Snapshot name (optional).
      confirm: Must be True to create snapshot.

    Safety: RISKY (creates snapshot metadata). Requires confirmation.
    Idempotency: Creating the same snapshot twice will fail.

    Returns:
      ToolOk with snapshot information.
    """
    # Сделай snapshot для /data/raw с именем s1
    if not confirm:
        return ToolError(error="snapshot_create requires confirm=true").model_dump()

    cmd = ["hdfs", "dfs", "-createSnapshot", path] + ([name] if name else [])
    code, out, err, docker_cmd = run_docker_exec(cmd)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="snapshot_create",
        risk=tool_risk("snapshot_create"),
        args={"path": path, "name": name, "confirm": confirm},
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "createSnapshot failed"),
                         hint="You may need: hdfs dfsadmin -allowSnapshot <path>").model_dump()

    return ToolOk(data={"path": path, "name": name, "raw": out.strip()}).model_dump()


@mcp.tool()
def snapshot_delete(path: str, name: str, confirm: bool = False) -> ToolOk | ToolError:
    """
    Delete an existing snapshot for an HDFS directory.

    Args:
      path: HDFS directory path.
      name: Snapshot name.
      confirm: Must be True to delete snapshot.

    Safety: DESTRUCTIVE. Requires explicit confirmation.
    Idempotency: No (deletes state).

    Returns:
      ToolOk on success.
    """
    # Удалить snapshot s1 у /data/raw
    if not confirm:
        return ToolError(error="snapshot_delete requires confirm=true").model_dump()

    cmd = ["hdfs", "dfs", "-deleteSnapshot", path, name]
    code, out, err, docker_cmd = run_docker_exec(cmd)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="snapshot_delete",
        risk=tool_risk("snapshot_delete"),
        args={"path": path, "name": name, "confirm": confirm},
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "deleteSnapshot failed")).model_dump()

    return ToolOk(data={"path": path, "name": name}).model_dump()


@mcp.tool()
def balancer_trigger(confirm: bool = False) -> ToolOk | ToolError:
    """
    Trigger the HDFS balancer process.

    Args:
      confirm: Must be True to start balancing.

    Safety: RISKY / HEAVY operation. Requires confirmation.
    Idempotency: Re-running may have no effect if balancer is already active.

    Returns:
      ToolOk with balancer output.
    """
    # Запусти балансировщик
    if not confirm:
        return ToolError(error="balancer_trigger requires confirm=true").model_dump()

    cmd = ["hdfs", "balancer"]
    code, out, err, docker_cmd = run_docker_exec(cmd)
    ok = (code == 0)

    write_audit(AuditRecord(
        ts=now_iso(),
        tool="balancer_trigger",
        risk=tool_risk("balancer_trigger"),
        args={"confirm": confirm},
        docker_cmd=docker_cmd,
        ok=ok,
        exit_code=code,
        stdout=out,
        stderr=err,
    ))

    if not ok:
        return ToolError(error=(err.strip() or "balancer trigger failed"),
                         hint="Balancer may be unavailable/irrelevant on single-node demo").model_dump()

    return ToolOk(data={"raw": out.strip()}).model_dump()



def run() -> None:
    init_audit_log()
    mcp.run()


if __name__ == "__main__":
    run()
