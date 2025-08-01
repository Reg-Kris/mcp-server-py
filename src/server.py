#!/usr/bin/env python3
"""
MCP Server for Airtable Integration (Refactored)
Exposes Airtable operations as MCP tools for LLM integration

Now supports both stdio (legacy) and HTTP modes for better performance
Refactored for modularity and maintainability
"""

import asyncio
import logging
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Import configuration and handlers
from .config import (
    MCP_SERVER_NAME, MCP_SERVER_VERSION, MCP_SERVER_MODE, MCP_SERVER_PORT,
    AIRTABLE_GATEWAY_URL, CORS_ORIGINS, SECURE_CONFIG_AVAILABLE,
    gateway, cleanup_config
)
from .models import ToolCallRequest, ToolCallResponse, ToolListResponse
from .handlers import (
    handle_list_tables, handle_get_records, handle_get_field_info,
    handle_create_record, handle_update_record, handle_delete_record,
    handle_batch_create_records, handle_batch_update_records,
    handle_analyze_table_data, handle_find_duplicates,
    handle_search_records, handle_create_metadata_table,
    handle_export_table_csv, handle_sync_tables
)

if SECURE_CONFIG_AVAILABLE:
    from pyairtable_common.middleware import setup_security_middleware

logger = logging.getLogger(__name__)

# Initialize MCP server (for stdio mode)
server = Server(MCP_SERVER_NAME)

# Initialize FastAPI app for HTTP mode
http_app = FastAPI(
    title="MCP Server HTTP API",
    description="HTTP API for MCP tools (replaces stdio for better performance)",
    version=MCP_SERVER_VERSION
)

# Add CORS middleware for HTTP mode with security hardening
http_app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key", "X-Trace-ID"],
)

# Custom middleware for distributed tracing
import uuid
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response

class DistributedTracingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        # Extract trace ID from incoming request or generate new one
        trace_id = request.headers.get("X-Trace-ID") or str(uuid.uuid4())
        
        # Add trace ID to request state for use in handlers
        request.state.trace_id = trace_id
        
        # Log request start with trace ID
        logger.info(f"[TRACE:{trace_id}] MCP Server request: {request.method} {request.url.path}")
        
        # Process request
        response = await call_next(request)
        
        # Add trace ID to response headers
        response.headers["X-Trace-ID"] = trace_id
        
        # Log request completion
        logger.info(f"[TRACE:{trace_id}] MCP Server response: {response.status_code}")
        
        return response

# Add distributed tracing middleware
http_app.add_middleware(DistributedTracingMiddleware)

# Add security middleware for HTTP mode
if SECURE_CONFIG_AVAILABLE:
    setup_security_middleware(http_app, rate_limit_calls=100, rate_limit_period=60)


@server.list_tools()
async def list_tools() -> List[Tool]:
    """List all available MCP tools"""
    return [
        Tool(
            name="list_tables",
            description="List all tables in an Airtable base",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {"type": "string", "description": "Airtable base ID (e.g., appXXXXXXXXXXXXXX)"}
                },
                "required": ["base_id"]
            }
        ),
        Tool(
            name="get_records",
            description="Retrieve records from an Airtable table",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {"type": "string", "description": "Airtable base ID"},
                    "table_id": {"type": "string", "description": "Table ID or name"},
                    "max_records": {"type": "integer", "description": "Maximum number of records to return (default: 100)", "default": 100},
                    "view": {"type": "string", "description": "View name or ID to filter by"},
                    "filter_by_formula": {"type": "string", "description": "Airtable formula to filter records"}
                },
                "required": ["base_id", "table_id"]
            }
        ),
        Tool(name="create_record", description="Create a new record in an Airtable table", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "fields": {"type": "object", "description": "Field values for the new record"}}, "required": ["base_id", "table_id", "fields"]}),
        Tool(name="update_record", description="Update an existing record in an Airtable table", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "record_id": {"type": "string", "description": "Record ID to update"}, "fields": {"type": "object", "description": "Field values to update"}}, "required": ["base_id", "table_id", "record_id", "fields"]}),
        Tool(name="delete_record", description="Delete a record from an Airtable table", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "record_id": {"type": "string", "description": "Record ID to delete"}}, "required": ["base_id", "table_id", "record_id"]}),
        Tool(name="search_records", description="Search records in an Airtable table with advanced filtering", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "query": {"type": "string", "description": "Search query text"}, "fields": {"type": "array", "items": {"type": "string"}, "description": "Specific fields to search in"}, "max_records": {"type": "integer", "description": "Maximum number of records to return", "default": 50}}, "required": ["base_id", "table_id", "query"]}),
        Tool(name="create_metadata_table", description="Create a table containing metadata about all tables in a base", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID to analyze"}, "table_name": {"type": "string", "description": "Name for the metadata table", "default": "Table Metadata"}}, "required": ["base_id"]}),
        Tool(name="batch_create_records", description="Create multiple records in a single operation (efficient for bulk data)", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "records": {"type": "array", "items": {"type": "object", "description": "Record fields object"}, "description": "Array of record objects to create"}}, "required": ["base_id", "table_id", "records"]}),
        Tool(name="batch_update_records", description="Update multiple records in a single operation", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "records": {"type": "array", "items": {"type": "object", "properties": {"id": {"type": "string"}, "fields": {"type": "object"}}, "required": ["id", "fields"]}, "description": "Array of records with IDs and fields to update"}}, "required": ["base_id", "table_id", "records"]}),
        Tool(name="get_field_info", description="Get detailed information about fields in a table", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}}, "required": ["base_id", "table_id"]}),
        Tool(name="analyze_table_data", description="Analyze table data to show statistics, patterns, and data quality insights", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "sample_size": {"type": "integer", "description": "Number of records to analyze (default: 100)", "default": 100}}, "required": ["base_id", "table_id"]}),
        Tool(name="find_duplicates", description="Find duplicate records in a table based on specified fields", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "fields": {"type": "array", "items": {"type": "string"}, "description": "Field names to check for duplicates"}, "ignore_empty": {"type": "boolean", "description": "Whether to ignore empty values when checking duplicates", "default": True}}, "required": ["base_id", "table_id", "fields"]}),
        Tool(name="export_table_csv", description="Export table data to CSV format (useful for data analysis)", inputSchema={"type": "object", "properties": {"base_id": {"type": "string", "description": "Airtable base ID"}, "table_id": {"type": "string", "description": "Table ID or name"}, "fields": {"type": "array", "items": {"type": "string"}, "description": "Specific fields to export (optional - all fields if not specified)"}, "view": {"type": "string", "description": "View name or ID to export"}, "max_records": {"type": "integer", "description": "Maximum number of records to export", "default": 1000}}, "required": ["base_id", "table_id"]}),
        Tool(name="sync_tables", description="Compare and sync data between two tables (useful for data migration/backup)", inputSchema={"type": "object", "properties": {"source_base_id": {"type": "string", "description": "Source base ID"}, "source_table_id": {"type": "string", "description": "Source table ID"}, "target_base_id": {"type": "string", "description": "Target base ID"}, "target_table_id": {"type": "string", "description": "Target table ID"}, "key_field": {"type": "string", "description": "Field name to use as unique identifier for syncing"}, "dry_run": {"type": "boolean", "description": "If true, only show what would be synced without making changes", "default": True}}, "required": ["source_base_id", "source_table_id", "target_base_id", "target_table_id", "key_field"]})
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool execution - delegates to appropriate handler"""
    logger.info(f"Executing tool: {name} with arguments: {arguments}")
    
    try:
        # Route to appropriate handler
        handler_map = {
            "list_tables": handle_list_tables,
            "get_records": handle_get_records,
            "get_field_info": handle_get_field_info,
            "create_record": handle_create_record,
            "update_record": handle_update_record,
            "delete_record": handle_delete_record,
            "batch_create_records": handle_batch_create_records,
            "batch_update_records": handle_batch_update_records,
            "analyze_table_data": handle_analyze_table_data,
            "find_duplicates": handle_find_duplicates,
            "search_records": handle_search_records,
            "create_metadata_table": handle_create_metadata_table,
            "export_table_csv": handle_export_table_csv,
            "sync_tables": handle_sync_tables
        }
        
        handler = handler_map.get(name)
        if handler:
            return await handler(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def call_tool_with_trace(name: str, arguments: Dict[str, Any], trace_id: str = None) -> List[TextContent]:
    """Handle tool execution with trace ID support"""
    if trace_id:
        logger.info(f"[TRACE:{trace_id}] Executing tool: {name} with arguments: {arguments}")
    else:
        logger.info(f"Executing tool: {name} with arguments: {arguments}")
    
    try:
        # Route to appropriate handler
        handler_map = {
            "list_tables": handle_list_tables,
            "get_records": handle_get_records,
            "get_field_info": handle_get_field_info,
            "create_record": handle_create_record,
            "update_record": handle_update_record,
            "delete_record": handle_delete_record,
            "batch_create_records": handle_batch_create_records,
            "batch_update_records": handle_batch_update_records,
            "analyze_table_data": handle_analyze_table_data,
            "find_duplicates": handle_find_duplicates,
            "search_records": handle_search_records,
            "create_metadata_table": handle_create_metadata_table,
            "export_table_csv": handle_export_table_csv,
            "sync_tables": handle_sync_tables
        }
        
        handler = handler_map.get(name)
        if handler:
            # Pass trace_id to handlers that support it
            import inspect
            if 'trace_id' in inspect.signature(handler).parameters:
                return await handler(arguments, trace_id=trace_id)
            else:
                return await handler(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except Exception as e:
        if trace_id:
            logger.error(f"[TRACE:{trace_id}] Error executing tool {name}: {e}")
        else:
            logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


# HTTP Endpoints for performance optimization
@http_app.get("/health")
async def http_health_check():
    """Health check for HTTP mode"""
    return {"status": "healthy", "service": "mcp-server-http", "version": MCP_SERVER_VERSION}


@http_app.get("/tools", response_model=ToolListResponse)
async def http_list_tools():
    """HTTP endpoint to list available tools"""
    try:
        tools = await list_tools()
        return ToolListResponse(tools=tools)
    except Exception as e:
        logger.error(f"Error listing tools via HTTP: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@http_app.post("/tools/call", response_model=ToolCallResponse)
async def http_call_tool(request: ToolCallRequest, http_request: Request):
    """HTTP endpoint to call a tool (replaces subprocess stdio)"""
    try:
        # Get trace ID from request state
        trace_id = getattr(http_request.state, 'trace_id', None)
        
        if trace_id:
            logger.info(f"[TRACE:{trace_id}] HTTP tool call: {request.name} with args: {request.arguments}")
        else:
            logger.info(f"HTTP tool call: {request.name} with args: {request.arguments}")
        
        # Use the same tool calling logic as stdio mode, but pass trace_id to handlers
        result = await call_tool_with_trace(request.name, request.arguments, trace_id)
        
        return ToolCallResponse(result=result, success=True)
    except Exception as e:
        if trace_id:
            logger.error(f"[TRACE:{trace_id}] Error calling tool {request.name} via HTTP: {e}")
        else:
            logger.error(f"Error calling tool {request.name} via HTTP: {e}")
        return ToolCallResponse(
            result=[TextContent(type="text", text=f"Error: {str(e)}")],
            success=False,
            error=str(e)
        )


async def main():
    """Main function to start the MCP server"""
    logger.info(f"Starting MCP Server: {MCP_SERVER_NAME} v{MCP_SERVER_VERSION}")
    logger.info(f"Mode: {MCP_SERVER_MODE}")
    logger.info(f"Connecting to Airtable Gateway at: {AIRTABLE_GATEWAY_URL}")
    
    # Test gateway connection
    try:
        await gateway.get("/health")
        logger.info("✅ Connected to Airtable Gateway")
    except Exception as e:
        logger.warning(f"⚠️  Could not connect to Airtable Gateway: {e}")
    
    try:
        if MCP_SERVER_MODE == "http":
            # Start HTTP server for better performance
            import uvicorn
            logger.info(f"🚀 Starting MCP Server in HTTP mode on port {MCP_SERVER_PORT}")
            config = uvicorn.Config(http_app, host="0.0.0.0", port=MCP_SERVER_PORT, log_level="info")
            server_instance = uvicorn.Server(config)
            await server_instance.serve()
        else:
            # Start MCP server with stdio transport (legacy mode)
            logger.info("🚀 Starting MCP Server in stdio mode")
            async with stdio_server() as (read_stream, write_stream):
                await server.run(read_stream, write_stream, server.create_initialization_options())
    finally:
        # Cleanup configuration
        await cleanup_config()


async def main_http():
    """Entry point for HTTP mode"""
    import os
    os.environ["MCP_SERVER_MODE"] = "http"
    await main()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--http":
        # Start in HTTP mode
        asyncio.run(main_http())
    else:
        # Default stdio mode for backward compatibility
        asyncio.run(main())