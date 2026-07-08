"""Pydantic models for Kafka message definitions."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

class JobResponse(BaseModel):
    """Generic job response."""

    job_id: str
    status: Literal["success", "failed", "processing"] = "success"
    result: dict[str, Any] = Field(default_factory=dict)
    error: str | None = None
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
