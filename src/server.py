#!/usr/bin/env python3
"""
MCP Server for Airtable Integration
Exposes Airtable operations as MCP tools for LLM integration
"""

import asyncio
import os
import logging
from typing import Any, Dict, List, Optional
import json

import httpx
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from pydantic import BaseModel

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# Configuration
AIRTABLE_GATEWAY_URL = os.getenv("AIRTABLE_GATEWAY_URL", "http://localhost:8002")
AIRTABLE_GATEWAY_API_KEY = os.getenv("AIRTABLE_GATEWAY_API_KEY", "simple-api-key")
MCP_SERVER_NAME = os.getenv("MCP_SERVER_NAME", "airtable-mcp")
MCP_SERVER_VERSION = os.getenv("MCP_SERVER_VERSION", "1.0.0")


class AirtableGatewayClient:
    """HTTP client for communicating with the Airtable Gateway service"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def get(self, endpoint: str, **params) -> Dict[str, Any]:
        """Make GET request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make POST request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
    
    async def patch(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make PATCH request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.patch(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
    
    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make DELETE request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.delete(url, headers=self.headers)
        response.raise_for_status()
        return response.json()


# Initialize gateway client
gateway = AirtableGatewayClient(AIRTABLE_GATEWAY_URL, AIRTABLE_GATEWAY_API_KEY)

# Initialize MCP server
server = Server(MCP_SERVER_NAME)


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
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID (e.g., appXXXXXXXXXXXXXX)"
                    }
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
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID"
                    },
                    "table_id": {
                        "type": "string",
                        "description": "Table ID or name"
                    },
                    "max_records": {
                        "type": "integer",
                        "description": "Maximum number of records to return (default: 100)",
                        "default": 100
                    },
                    "view": {
                        "type": "string",
                        "description": "View name or ID to filter by"
                    },
                    "filter_by_formula": {
                        "type": "string",
                        "description": "Airtable formula to filter records"
                    }
                },
                "required": ["base_id", "table_id"]
            }
        ),
        Tool(
            name="create_record",
            description="Create a new record in an Airtable table",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID"
                    },
                    "table_id": {
                        "type": "string",
                        "description": "Table ID or name"
                    },
                    "fields": {
                        "type": "object",
                        "description": "Field values for the new record"
                    }
                },
                "required": ["base_id", "table_id", "fields"]
            }
        ),
        Tool(
            name="update_record",
            description="Update an existing record in an Airtable table",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID"
                    },
                    "table_id": {
                        "type": "string",
                        "description": "Table ID or name"
                    },
                    "record_id": {
                        "type": "string",
                        "description": "Record ID to update"
                    },
                    "fields": {
                        "type": "object",
                        "description": "Field values to update"
                    }
                },
                "required": ["base_id", "table_id", "record_id", "fields"]
            }
        ),
        Tool(
            name="delete_record",
            description="Delete a record from an Airtable table",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID"
                    },
                    "table_id": {
                        "type": "string",
                        "description": "Table ID or name"
                    },
                    "record_id": {
                        "type": "string",
                        "description": "Record ID to delete"
                    }
                },
                "required": ["base_id", "table_id", "record_id"]
            }
        ),
        Tool(
            name="search_records",
            description="Search records in an Airtable table with advanced filtering",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID"
                    },
                    "table_id": {
                        "type": "string",
                        "description": "Table ID or name"
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query text"
                    },
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific fields to search in"
                    },
                    "max_records": {
                        "type": "integer",
                        "description": "Maximum number of records to return",
                        "default": 50
                    }
                },
                "required": ["base_id", "table_id", "query"]
            }
        ),
        Tool(
            name="create_metadata_table",
            description="Create a table containing metadata about all tables in a base",
            inputSchema={
                "type": "object",
                "properties": {
                    "base_id": {
                        "type": "string",
                        "description": "Airtable base ID to analyze"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name for the metadata table",
                        "default": "Table Metadata"
                    }
                },
                "required": ["base_id"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool execution"""
    logger.info(f"Executing tool: {name} with arguments: {arguments}")
    
    try:
        if name == "list_tables":
            return await handle_list_tables(arguments)
        elif name == "get_records":
            return await handle_get_records(arguments)
        elif name == "create_record":
            return await handle_create_record(arguments)
        elif name == "update_record":
            return await handle_update_record(arguments)
        elif name == "delete_record":
            return await handle_delete_record(arguments)
        elif name == "search_records":
            return await handle_search_records(arguments)
        elif name == "create_metadata_table":
            return await handle_create_metadata_table(arguments)
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_list_tables(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle list_tables tool"""
    base_id = arguments["base_id"]
    
    result = await gateway.get(f"/bases/{base_id}/schema")
    tables = result.get("tables", [])
    
    # Format table information
    table_info = []
    for table in tables:
        table_info.append({
            "id": table["id"],
            "name": table["name"],
            "description": table.get("description", ""),
            "field_count": len(table.get("fields", [])),
            "view_count": len(table.get("views", []))
        })
    
    response = {
        "base_id": base_id,
        "table_count": len(tables),
        "tables": table_info
    }
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_get_records(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle get_records tool"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    
    params = {}
    if "max_records" in arguments:
        params["max_records"] = arguments["max_records"]
    if "view" in arguments:
        params["view"] = arguments["view"]
    if "filter_by_formula" in arguments:
        params["filter_by_formula"] = arguments["filter_by_formula"]
    
    result = await gateway.get(f"/bases/{base_id}/tables/{table_id}/records", **params)
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def handle_create_record(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle create_record tool"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    fields = arguments["fields"]
    
    result = await gateway.post(f"/bases/{base_id}/tables/{table_id}/records", fields)
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def handle_update_record(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle update_record tool"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    record_id = arguments["record_id"]
    fields = arguments["fields"]
    
    result = await gateway.patch(f"/bases/{base_id}/tables/{table_id}/records/{record_id}", fields)
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def handle_delete_record(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle delete_record tool"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    record_id = arguments["record_id"]
    
    result = await gateway.delete(f"/bases/{base_id}/tables/{table_id}/records/{record_id}")
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def handle_search_records(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle search_records tool"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    query = arguments["query"]
    fields = arguments.get("fields", [])
    max_records = arguments.get("max_records", 50)
    
    # Build filter formula for search
    if fields:
        # Search in specific fields
        conditions = []
        for field in fields:
            conditions.append(f"FIND(LOWER('{query}'), LOWER({{{field}}})) > 0")
        filter_formula = f"OR({', '.join(conditions)})"
    else:
        # Generic search across all text fields
        filter_formula = f"SEARCH(LOWER('{query}'), LOWER(CONCATENATE(VALUES())))"
    
    params = {
        "filter_by_formula": filter_formula,
        "max_records": max_records
    }
    
    result = await gateway.get(f"/bases/{base_id}/tables/{table_id}/records", **params)
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def handle_create_metadata_table(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle create_metadata_table tool - analyzes base and creates metadata table"""
    base_id = arguments["base_id"]
    table_name = arguments.get("table_name", "Table Metadata")
    
    # First get the base schema
    schema_result = await gateway.get(f"/bases/{base_id}/schema")
    tables = schema_result.get("tables", [])
    
    # Prepare metadata records
    metadata_records = []
    for table in tables:
        fields = table.get("fields", [])
        
        # Analyze field types
        field_types = {}
        for field in fields:
            field_type = field.get("type", "unknown")
            field_types[field_type] = field_types.get(field_type, 0) + 1
        
        # Create metadata record
        metadata_record = {
            "Table Name": table["name"],
            "Table ID": table["id"],
            "Description": table.get("description", ""),
            "Field Count": len(fields),
            "View Count": len(table.get("views", [])),
            "Field Types": ", ".join([f"{k}: {v}" for k, v in field_types.items()]),
            "Primary Fields": ", ".join([f["name"] for f in fields[:3]]),  # First 3 fields
            "Created Date": "",  # Would need additional API call to get creation date
            "Purpose": _infer_table_purpose(table["name"], fields)
        }
        metadata_records.append(metadata_record)
    
    # Create the metadata table
    # Note: This would require creating a new table via Airtable API
    # For now, we'll return the metadata as structured data
    
    result = {
        "message": f"Metadata analysis complete for base {base_id}",
        "suggested_table_name": table_name,
        "metadata_records": metadata_records,
        "summary": {
            "total_tables": len(tables),
            "total_fields": sum(len(t.get("fields", [])) for t in tables),
            "table_types": _categorize_tables(tables)
        }
    }
    
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


def _infer_table_purpose(table_name: str, fields: List[Dict[str, Any]]) -> str:
    """Infer the purpose of a table based on its name and fields"""
    name_lower = table_name.lower()
    field_names = [f.get("name", "").lower() for f in fields]
    
    # Common patterns
    if any(word in name_lower for word in ["project", "task", "todo"]):
        return "Project/Task Management"
    elif any(word in name_lower for word in ["contact", "people", "user", "client"]):
        return "Contact/People Management"
    elif any(word in name_lower for word in ["product", "inventory", "item"]):
        return "Product/Inventory Tracking"
    elif any(word in name_lower for word in ["event", "calendar", "schedule"]):
        return "Event/Schedule Management"
    elif any(word in field_names for word in ["email", "phone", "address"]):
        return "Contact Information"
    elif any(word in field_names for word in ["price", "cost", "amount", "budget"]):
        return "Financial/Budget Tracking"
    else:
        return "General Data Storage"


def _categorize_tables(tables: List[Dict[str, Any]]) -> Dict[str, int]:
    """Categorize tables by their inferred purpose"""
    categories = {}
    for table in tables:
        purpose = _infer_table_purpose(table["name"], table.get("fields", []))
        categories[purpose] = categories.get(purpose, 0) + 1
    return categories


async def main():
    """Main function to start the MCP server"""
    logger.info(f"Starting MCP Server: {MCP_SERVER_NAME} v{MCP_SERVER_VERSION}")
    logger.info(f"Connecting to Airtable Gateway at: {AIRTABLE_GATEWAY_URL}")
    
    # Test gateway connection
    try:
        await gateway.get("/health")
        logger.info("✅ Connected to Airtable Gateway")
    except Exception as e:
        logger.warning(f"⚠️  Could not connect to Airtable Gateway: {e}")
    
    # Start MCP server with stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())