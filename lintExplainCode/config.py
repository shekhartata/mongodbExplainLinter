import os
from typing import Optional
from pydantic import BaseSettings, Field


class MongoConfig(BaseSettings):
    """MongoDB connection configuration."""
    connection_string: str = Field(
        default="mongodb://localhost:27017",
        env="MONGO_CONNECTION_STRING"
    )
    database: str = Field(
        default="test",
        env="MONGO_DATABASE"
    )
    username: Optional[str] = Field(default=None, env="MONGO_USERNAME")
    password: Optional[str] = Field(default=None, env="MONGO_PASSWORD")
    auth_source: str = Field(default="admin", env="MONGO_AUTH_SOURCE")
    
    class Config:
        env_file = ".env"


class LintConfig(BaseSettings):
    """Linting configuration."""
    max_collection_scan_threshold: int = Field(
        default=1000,
        description="Maximum documents to scan before flagging as performance issue"
    )
    max_execution_time_ms: int = Field(
        default=100,
        description="Maximum execution time in milliseconds before flagging"
    )
    include_system_collections: bool = Field(
        default=False,
        description="Whether to include system collections in analysis"
    )
    sample_size: int = Field(
        default=1000,
        description="Number of documents to sample for analysis"
    )
    
    class Config:
        env_file = ".env"


class Config:
    """Main configuration class."""
    mongo: MongoConfig = MongoConfig()
    lint: LintConfig = LintConfig()
    
    # CI/CD specific settings
    ci_mode: bool = Field(
        default=False,
        env="CI_MODE"
    )
    pr_number: Optional[str] = Field(default=None, env="PR_NUMBER")
    github_token: Optional[str] = Field(default=None, env="GITHUB_TOKEN")
    github_repo: Optional[str] = Field(default=None, env="GITHUB_REPO")


# Global config instance
config = Config()
