#!/usr/bin/env python3
"""
MCP Server for Airtable Integration - Refactored with PyAirtableService Base Class
Exposes Airtable operations as MCP tools for LLM integration

Now supports both stdio (legacy) and HTTP modes for better performance
Refactored for modularity and maintainability
"""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, List

from fastapi import HTTPException
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Add pyairtable-common to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../pyairtable-common'))

from pyairtable_common.service import PyAirtableService, ServiceConfig

# Import configuration and handlers
from .config import (
    MCP_SERVER_NAME, MCP_SERVER_VERSION, MCP_SERVER_MODE, MCP_SERVER_PORT,
    AIRTABLE_GATEWAY_URL, gateway, cleanup_config
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

logger = logging.getLogger(__name__)

# Initialize MCP server (for stdio mode)
server = Server(MCP_SERVER_NAME)


class MCPServerService(PyAirtableService):
    """
    MCP Server service extending PyAirtableService base class.
    Supports both HTTP and stdio modes.
    """
    
    def __init__(self, mode: str = "http"):
        self.mode = mode
        
        # Initialize service configuration
        config = ServiceConfig(
            title="MCP Server HTTP API",
            description="HTTP API for MCP tools (replaces stdio for better performance)",
            version=MCP_SERVER_VERSION,
            service_name="mcp-server",
            port=MCP_SERVER_PORT,
            api_key=os.getenv("API_KEY"),
            cors_methods=["GET", "POST", "OPTIONS"],  # Limited methods for MCP
            rate_limit_calls=100,
            rate_limit_period=60,
            startup_tasks=[self._test_gateway_connection],
            shutdown_tasks=[self._cleanup_config]
        )
        
        super().__init__(config)
        
        # Setup MCP routes
        self._setup_mcp_routes()
        
        # Setup MCP tools for stdio mode
        self._setup_mcp_tools()
    
    async def _test_gateway_connection(self) -> None:
        """Test connection to Airtable Gateway."""
        try:
            await gateway.get("/health")
            self.logger.info("✅ Connected to Airtable Gateway")
        except Exception as e:
            self.logger.warning(f"⚠️  Could not connect to Airtable Gateway: {e}")
    
    async def _cleanup_config(self) -> None:
        """Cleanup configuration."""
        await cleanup_config()
    
    def _setup_mcp_routes(self) -> None:
        """Setup MCP HTTP API routes."""
        
        @self.app.get("/tools", response_model=ToolListResponse)
        async def http_list_tools():
            """HTTP endpoint to list available tools"""
            try:
                tools = await self._get_mcp_tools()
                return ToolListResponse(tools=tools)
            except Exception as e:
                self.logger.error(f"Error listing tools via HTTP: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/tools/call", response_model=ToolCallResponse)
        async def http_call_tool(request: ToolCallRequest):
            """HTTP endpoint to call a tool (replaces subprocess stdio)"""
            try:
                self.logger.info(f"HTTP tool call: {request.name} with args: {request.arguments}")
                
                # Use the same tool calling logic as stdio mode
                result = await self._call_mcp_tool(request.name, request.arguments)
                
                return ToolCallResponse(result=result, success=True)
            except Exception as e:
                self.logger.error(f"Error calling tool {request.name} via HTTP: {e}")
                return ToolCallResponse(
                    result=[TextContent(type="text", text=f"Error: {str(e)}")],
                    success=False,
                    error=str(e)
                )
    
    def _setup_mcp_tools(self) -> None:
        """Setup MCP tools for stdio mode."""
        
        @server.list_tools()
        async def list_tools() -> List[Tool]:
            """List all available MCP tools"""
            return await self._get_mcp_tools()

        @server.call_tool()
        async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            """Handle tool execution - delegates to appropriate handler"""
            return await self._call_mcp_tool(name, arguments)
    
    async def _get_mcp_tools(self) -> List[Tool]:
        """Get list of available MCP tools."""
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
    
    async def _call_mcp_tool(self, name: str, arguments: Dict[str, Any]) -> List[TextContent]:
        """Handle tool execution - delegates to appropriate handler"""
        self.logger.info(f"Executing tool: {name} with arguments: {arguments}")
        
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
            self.logger.error(f"Error executing tool {name}: {e}")
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    async def health_check(self) -> Dict[str, Any]:
        """Custom health check for MCP server."""
        return {
            "mode": self.mode,
            "airtable_gateway": AIRTABLE_GATEWAY_URL,
            "tools_available": len(await self._get_mcp_tools())
        }
    
    async def run_stdio_mode(self) -> None:
        """Run MCP server in stdio mode (legacy)."""
        self.logger.info("🚀 Starting MCP Server in stdio mode")
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())


def create_mcp_server_service(mode: str = "http") -> MCPServerService:
    """Factory function to create MCP server service."""
    return MCPServerService(mode)


async def main():
    """Main function to start the MCP server"""
    mode = os.getenv("MCP_SERVER_MODE", "http")
    
    logger.info(f"Starting MCP Server: {MCP_SERVER_NAME} v{MCP_SERVER_VERSION}")
    logger.info(f"Mode: {mode}")
    logger.info(f"Connecting to Airtable Gateway at: {AIRTABLE_GATEWAY_URL}")
    
    service = create_mcp_server_service(mode)
    
    try:
        if mode == "http":
            # Start HTTP server for better performance
            logger.info(f"🚀 Starting MCP Server in HTTP mode on port {MCP_SERVER_PORT}")
            service.run()
        else:
            # Start MCP server with stdio transport (legacy mode)
            await service.run_stdio_mode()
    finally:
        # Cleanup is handled by the service base class
        pass


async def main_http():
    """Entry point for HTTP mode"""
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