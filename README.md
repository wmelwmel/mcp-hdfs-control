# MCP Server and Agent for HDFS ("HDFS Control & Assist")

**MCP Server + LLM Agent for HDFS management**  

This project demonstrates how a LLM can safely interact with a distributed storage system (HDFS) exclusively through MCP tools, with security controls, auditing, and fault tolerance.


## Overview

The system consists of:
- MCP server (`mcp-hdfs`) exposing a controlled set of HDFS management tools
- LLM-powered agent (`agent-hdfs`) that:
  - accepts natural-language requests
  - plans actions (React / CodeAct style)
  - asks for confirmations when required
  - executes operations via MCP
  - explains results and failures

All HDFS operations are:
- allow-listed
- validated
- logged
- and executed with retries and timeouts


## Features

### MCP Server (mcp-hdfs)

Supported tools:
- list, stat
- mkdir
- chmod, chown
- put, get
- getquota, setquota
- snapshot_create, snapshot_delete
- balancer_trigger

Key properties:
- allow-list of HDFS commands
- risky operations require explicit confirmation (confirm=true)
- idempotent read operations
- structured audit log (JSONL)
- retry + timeout handling
- permission diff tracking for chmod/chown

### LLM Agent (agent-hdfs)

- CLI-based chat interface
- natural-language understanding
- action planning and tool selection
- clarification of missing or risky parameters
- error explanations
- operates only through MCP tools (no direct shell access)

## Architecture

```
         User
     
           ↓
     
     Agent CLI (LLM)
     
           ↓  MCP
     
     MCP HDFS Server
     
           ↓
       
HDFS (NameNode + DataNodes)
```


## Project Structure

```
docker/
  docker-compose.yml        # HDFS cluster (NameNode + DataNodes)

scripts/
  seed_hdfs.ps1             # initial test data
  bench_many_files.ps1      # many-files + paging benchmark

src/
  agent/                    # LLM agent (CLI, planning, reporting)
  config/                   # Pydantic-based settings (env validation)
  mcp_hdfs/                 # MCP server implementation
    audit.py                # audit logging
    constants.py            # allow-list and risk classification
    hdfs_exec.py            # docker exec + retries
    models.py               # Pydantic models
    parsers.py              # HDFS output parsers
    server.py               # MCP server entrypoint

.env.example
audit.log.jsonl             # audit log
README.md
```


## Requirements

- Docker and Docker Compose
- Python 3.10.11
- uv package manager
- OpenRouter API key (for LLM access)


## Quick Start

### 1. Start HDFS cluster

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verify cluster state:
```bash
docker exec -it namenode hdfs dfsadmin -report
```

### 2. Seed initial data (optional)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/seed_hdfs.ps1
```

### 3. Upload a large file (optional multi-block demo)

```bash
docker exec -it namenode bash -lc "dd if=/dev/urandom of=/tmp/big.bin bs=1M count=300"
docker exec -it namenode hdfs dfs -put -f /tmp/big.bin /data/raw/big.bin
```

If NameNode is in safemode:
```bash
docker exec -it namenode hdfs dfsadmin -safemode leave
```

Verify block distribution:
```bash
docker exec -it namenode hdfs fsck /data/raw/big.bin -files -blocks -locations
```

### 4. Generate many-files benchmark (optional)
```powershell
powershell -ExecutionPolicy Bypass -File scripts/bench_many_files.ps1 -N 20000
```

### 5. Enable snapshots (optional once)
```bash
docker exec -it namenode hdfs dfsadmin -allowSnapshot /data/raw
```

### 6. Run the LLM agent
```
uv run python -m src.agent.cli
```

## Example natural-language queries per yool

The following examples demonstrate how the LLM agent maps natural-language requests to MCP tools.

---

### list

Show contents of /data/raw  
Show the first file in /data/raw  
Show the next files in /data/raw (expected offset = 1)

---

### stat

What is the size of /data/raw/sample.csv?  
Who is the owner of /data/raw?

---

### mkdir

Create directory /data/by_llm  
Create nested directory /data/a/b/c

---

### put

Upload file /tmp/a.txt to /data/by_llm/a2.txt  
Try to overwrite /data/by_llm/a2.txt

---

### get

Download /data/raw/a.txt to /tmp/a_dl.txt  
Download the same file again to the same local path

---

### chmod

Set permissions 755 on /data/raw  
Set permissions 777 on /data/test_perm

---

### chown

Change owner of /data/test_perm to root  
Change owner and group of /data/test_perm to root:supergroup

---

### getquota

Show quotas and usage for /data  
What is the quota on /data/raw?

---

### setquota

Set namespace quota to 1000 files on /data/raw  
Set space quota to 1g on /data/raw

---

### snapshot_create

Create snapshot s1 for /data/raw

---

### snapshot_delete

Delete snapshot s1 from /data/raw

---

### balancer_trigger

Run the HDFS balancer

---

## Pagination example (many files)

The following queries can be used to test paging on large directories:

Show the first 50 files in /bench/many  
Show the next 50 files  
Show the next 50 files after that

---

## Invalid operation example

Remove directory /data/raw

Expected behavior:
- the agent reports that no such tool exists
- no action is performed
- no changes are made in HDFS
