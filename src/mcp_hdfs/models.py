from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field


class ToolError(BaseModel):
    ok: Literal[False] = False
    error: str
    hint: Optional[str] = None


class ToolOk(BaseModel):
    ok: Literal[True] = True
    data: Any


ToolResult = Union[ToolOk, ToolError]


class ListRequest(BaseModel):
    path: str = Field(default="/", description="HDFS path like /data/raw")
    recursive: bool = False
    limit: int = Field(default=200, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)


class LsItem(BaseModel):
    path: str
    type: Literal["file", "dir"]
    perm: str
    owner: str
    group: str
    size: int
    date: str
    time: str
    replication: str


class ListResponseData(BaseModel):
    items: List[LsItem]
    next_offset: Optional[int] = None
    total_in_page: int


class StatRequest(BaseModel):
    path: str


class StatResponseData(BaseModel):
    name: str
    size: int
    block_size: str
    replication: str
    owner: str
    group: str
    modified: str
    type: str
    raw: str


class MkdirRequest(BaseModel):
    path: str
    parents: bool = True
    confirm: bool = False


class PutRequest(BaseModel):
    local_path: str = Field(description="Path INSIDE namenode container (MVP)")
    hdfs_path: str
    overwrite: bool = False
    confirm: bool = False


class GetRequest(BaseModel):
    hdfs_path: str
    local_path: str = Field(description="Path INSIDE namenode container (MVP)")
    overwrite: bool = False
    confirm: bool = False


class ChmodRequest(BaseModel):
    path: str
    mode: str = Field(description="e.g. 755 or u+rwx,g+rx,o+rx")
    recursive: bool = False
    confirm: bool = False


class ChownRequest(BaseModel):
    path: str
    owner: str
    group: Optional[str] = None
    recursive: bool = False
    confirm: bool = False


class PermSnapshot(BaseModel):
    path: str
    perm: str
    owner: str
    group: str
    type: Optional[str] = None


class PermDiff(BaseModel):
    changed: bool
    changes: Dict[str, List[str]]
