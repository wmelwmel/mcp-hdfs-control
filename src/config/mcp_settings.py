from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MCPSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Docker / HDFS
    hdfs_namenode_container: str = Field(default="namenode", alias="HDFS_NAMENODE_CONTAINER")

    # Audit
    mcp_audit_log: str = Field(default="audit.log.jsonl", alias="MCP_AUDIT_LOG")

    # Execution controls
    mcp_timeout_sec: int = Field(default=20, ge=1, le=600, alias="MCP_TIMEOUT_SEC")
    mcp_retries: int = Field(default=2, ge=0, le=10, alias="MCP_RETRIES")

    # Security knobs
    strict_confirm: bool = Field(default=True, alias="MCP_STRICT_CONFIRM")


mcp_settings = MCPSettings()
