SAFE_TOOLS = {"list", "stat", "get", "getquota"}

RISKY_TOOLS = {
    "mkdir",
    # "put",
    # "get",
    "chmod",
    "chown",
    "setquota",
    "getquota", 
    "snapshot_create",
    "snapshot_delete",
    "balancer_trigger",
}

ALLOWED_HDFS_DFS = {"ls", "stat", "mkdir", "put", "get", "chmod", "chown"}

AUDIT_TRIM_CHARS = 5000
MAX_LIST_LIMIT = 5000
