"""
Request models for MCP Server HTTP API
"""

from typing import Any, Dict
from pydantic import BaseModel


class ToolCallRequest(BaseModel):
    """Request model for HTTP tool calls"""
    name: str
    arguments: Dict[str, Any]