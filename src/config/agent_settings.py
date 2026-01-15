from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AgentSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    openrouter_api_key: str = Field(alias="OPENROUTER_API_KEY")
    openrouter_model: str = Field(alias="OPENROUTER_MODEL")

    @field_validator("openrouter_api_key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        if not v or not v.startswith("sk-"):
            raise ValueError("OPENROUTER_API_KEY must start with 'sk-'")
        return v

    @field_validator("openrouter_model")
    @classmethod
    def validate_model(cls, v: str) -> str:
        if "/" not in v:
            raise ValueError("OPENROUTER_MODEL must look like 'provider/model'")
        return v


agent_settings = AgentSettings()
