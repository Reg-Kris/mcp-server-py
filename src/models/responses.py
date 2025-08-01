"""
Response models for MCP Server HTTP API
"""

from typing import List, Optional
from pydantic import BaseModel
from mcp.types import Tool, TextContent


class ToolCallResponse(BaseModel):
    """Response model for HTTP tool calls"""
    result: List[TextContent]
    success: bool
    error: Optional[str] = None


class ToolListResponse(BaseModel):
    """Response model for listing available tools"""
    tools: List[Tool]