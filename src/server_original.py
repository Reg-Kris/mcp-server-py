#!/usr/bin/env python3
"""
MCP Server for Airtable Integration
Exposes Airtable operations as MCP tools for LLM integration

Now supports both stdio (legacy) and HTTP modes for better performance
"""

import asyncio
import os
import logging
from typing import Any, Dict, List, Optional
import json

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# Security imports - add path to find pyairtable-common (development mode)
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../pyairtable-common'))

try:
    from pyairtable_common.security import (
        sanitize_user_query,
        sanitize_field_name,
        build_safe_search_formula,
        validate_filter_formula,
        AirtableFormulaInjectionError
    )
    SECURITY_AVAILABLE = True
    logger.info("✅ Security module loaded - formula injection protection enabled")
except ImportError as e:
    logger.warning(f"⚠️ Security module not available: {e}")
    logger.warning("Formula injection protection DISABLED - this is a security risk!")
    SECURITY_AVAILABLE = False

# Secure configuration imports
try:
    from pyairtable_common.config import initialize_secrets, get_secret, close_secrets, ConfigurationError
    from pyairtable_common.middleware import setup_security_middleware
    SECURE_CONFIG_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ Secure configuration not available: {e}")
    SECURE_CONFIG_AVAILABLE = False

# Initialize secure configuration manager  
config_manager = None
if SECURE_CONFIG_AVAILABLE:
    try:
        config_manager = initialize_secrets()
        logger.info("✅ Secure configuration manager initialized")
    except Exception as e:
        logger.error(f"💥 Failed to initialize secure configuration: {e}")
        raise

# Configuration
AIRTABLE_GATEWAY_URL = os.getenv("AIRTABLE_GATEWAY_URL", "http://localhost:8002")

# Get API key from secure manager or fallback to environment
AIRTABLE_GATEWAY_API_KEY = None
if config_manager:
    try:
        AIRTABLE_GATEWAY_API_KEY = get_secret("API_KEY")  # Use internal API_KEY for gateway communication
    except Exception as e:
        logger.error(f"💥 Failed to get API_KEY from secure config: {e}")
        raise ValueError("API_KEY could not be retrieved from secure configuration")
else:
    AIRTABLE_GATEWAY_API_KEY = os.getenv("AIRTABLE_GATEWAY_API_KEY")
    if not AIRTABLE_GATEWAY_API_KEY:
        logger.error("💥 CRITICAL: AIRTABLE_GATEWAY_API_KEY environment variable is required")
        raise ValueError("AIRTABLE_GATEWAY_API_KEY environment variable is required")
MCP_SERVER_NAME = os.getenv("MCP_SERVER_NAME", "airtable-mcp")
MCP_SERVER_VERSION = os.getenv("MCP_SERVER_VERSION", "1.0.0")
MCP_SERVER_MODE = os.getenv("MCP_SERVER_MODE", "stdio")  # "stdio" or "http"
MCP_SERVER_PORT = int(os.getenv("MCP_SERVER_PORT", "8001"))


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

# Initialize MCP server (for stdio mode)
server = Server(MCP_SERVER_NAME)

# HTTP API Models for new HTTP mode
class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any]

class ToolCallResponse(BaseModel):
    result: List[TextContent]
    success: bool
    error: Optional[str] = None

class ToolListResponse(BaseModel):
    tools: List[Tool]

# Initialize FastAPI app for HTTP mode
http_app = FastAPI(
    title="MCP Server HTTP API",
    description="HTTP API for MCP tools (replaces stdio for better performance)",
    version=MCP_SERVER_VERSION
)

# Add CORS middleware for HTTP mode with security hardening
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")
http_app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

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
        ),
        Tool(
            name="batch_create_records",
            description="Create multiple records in a single operation (efficient for bulk data)",
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
                    "records": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "description": "Record fields object"
                        },
                        "description": "Array of record objects to create"
                    }
                },
                "required": ["base_id", "table_id", "records"]
            }
        ),
        Tool(
            name="batch_update_records",
            description="Update multiple records in a single operation",
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
                    "records": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"},
                                "fields": {"type": "object"}
                            },
                            "required": ["id", "fields"]
                        },
                        "description": "Array of records with IDs and fields to update"
                    }
                },
                "required": ["base_id", "table_id", "records"]
            }
        ),
        Tool(
            name="get_field_info",
            description="Get detailed information about fields in a table",
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
                    }
                },
                "required": ["base_id", "table_id"]
            }
        ),
        Tool(
            name="analyze_table_data",
            description="Analyze table data to show statistics, patterns, and data quality insights",
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
                    "sample_size": {
                        "type": "integer",
                        "description": "Number of records to analyze (default: 100)",
                        "default": 100
                    }
                },
                "required": ["base_id", "table_id"]
            }
        ),
        Tool(
            name="find_duplicates",
            description="Find duplicate records in a table based on specified fields",
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
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Field names to check for duplicates"
                    },
                    "ignore_empty": {
                        "type": "boolean",
                        "description": "Whether to ignore empty values when checking duplicates",
                        "default": true
                    }
                },
                "required": ["base_id", "table_id", "fields"]
            }
        ),
        Tool(
            name="export_table_csv",
            description="Export table data to CSV format (useful for data analysis)",
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
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific fields to export (optional - all fields if not specified)"
                    },
                    "view": {
                        "type": "string",
                        "description": "View name or ID to export"
                    },
                    "max_records": {
                        "type": "integer",
                        "description": "Maximum number of records to export",
                        "default": 1000
                    }
                },
                "required": ["base_id", "table_id"]
            }
        ),
        Tool(
            name="sync_tables",
            description="Compare and sync data between two tables (useful for data migration/backup)",
            inputSchema={
                "type": "object",
                "properties": {
                    "source_base_id": {
                        "type": "string",
                        "description": "Source base ID"
                    },
                    "source_table_id": {
                        "type": "string",
                        "description": "Source table ID"
                    },
                    "target_base_id": {
                        "type": "string",
                        "description": "Target base ID"
                    },
                    "target_table_id": {
                        "type": "string",
                        "description": "Target table ID"
                    },
                    "key_field": {
                        "type": "string",
                        "description": "Field name to use as unique identifier for syncing"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, only show what would be synced without making changes",
                        "default": true
                    }
                },
                "required": ["source_base_id", "source_table_id", "target_base_id", "target_table_id", "key_field"]
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
        elif name == "batch_create_records":
            return await handle_batch_create_records(arguments)
        elif name == "batch_update_records":
            return await handle_batch_update_records(arguments)
        elif name == "get_field_info":
            return await handle_get_field_info(arguments)
        elif name == "analyze_table_data":
            return await handle_analyze_table_data(arguments)
        elif name == "find_duplicates":
            return await handle_find_duplicates(arguments)
        elif name == "export_table_csv":
            return await handle_export_table_csv(arguments)
        elif name == "sync_tables":
            return await handle_sync_tables(arguments)
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
        # SECURITY: Validate user-provided formulas to prevent injection
        if SECURITY_AVAILABLE:
            try:
                safe_formula = validate_filter_formula(arguments["filter_by_formula"])
                params["filter_by_formula"] = safe_formula
                logger.info("✅ Formula validated and sanitized")
            except AirtableFormulaInjectionError as e:
                logger.error(f"🚨 Formula injection attempt blocked: {e}")
                return [TextContent(type="text", text=f"Security Error: {str(e)}")]
        else:
            # No security module - log warning but allow (insecure)
            logger.warning("⚠️ Unsanitized formula used (security module unavailable)")
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
    
    # SECURITY: Build safe search formula using sanitized inputs
    if SECURITY_AVAILABLE:
        try:
            # Use the secure formula builder
            filter_formula = build_safe_search_formula(query, fields)
            logger.info("✅ Search formula built with security sanitization")
        except AirtableFormulaInjectionError as e:
            logger.error(f"🚨 Search injection attempt blocked: {e}")
            return [TextContent(type="text", text=f"Security Error: {str(e)}")]
    else:
        # FALLBACK (INSECURE): Original implementation with warning
        logger.warning("⚠️ Using INSECURE formula building (security module unavailable)")
        logger.warning(f"⚠️ Raw query input: {query}")
        logger.warning(f"⚠️ Raw fields input: {fields}")
        
        # Build filter formula for search (VULNERABLE - kept for fallback only)
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


async def handle_batch_create_records(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle batch_create_records tool - create multiple records efficiently"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    records = arguments["records"]
    
    # Validate records format
    if not records or not isinstance(records, list):
        return [TextContent(type="text", text="Error: 'records' must be a non-empty array")]
    
    if len(records) > 10:
        return [TextContent(type="text", text="Error: Maximum 10 records per batch operation (Airtable limit)")]
    
    result = await gateway.post(f"/bases/{base_id}/tables/{table_id}/records/batch", {"records": records})
    
    response = {
        "message": f"Successfully created {len(result.get('records', []))} records",
        "created_records": result.get("records", []),
        "base_id": base_id,
        "table_id": table_id
    }
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_batch_update_records(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle batch_update_records tool - update multiple records efficiently"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    records = arguments["records"]
    
    # Validate records format
    if not records or not isinstance(records, list):
        return [TextContent(type="text", text="Error: 'records' must be a non-empty array")]
    
    if len(records) > 10:
        return [TextContent(type="text", text="Error: Maximum 10 records per batch operation (Airtable limit)")]
    
    # Validate each record has id and fields
    for i, record in enumerate(records):
        if not isinstance(record, dict) or "id" not in record or "fields" not in record:
            return [TextContent(type="text", text=f"Error: Record {i} must have 'id' and 'fields' properties")]
    
    # Since the gateway doesn't have batch update, we'll do individual updates
    # This is still more efficient than the user doing them one by one
    updated_records = []
    errors = []
    
    for record in records:
        try:
            result = await gateway.patch(
                f"/bases/{base_id}/tables/{table_id}/records/{record['id']}", 
                record["fields"]
            )
            updated_records.append(result)
        except Exception as e:
            errors.append({"record_id": record["id"], "error": str(e)})
    
    response = {
        "message": f"Batch update completed: {len(updated_records)} success, {len(errors)} errors",
        "updated_records": updated_records,
        "errors": errors,
        "base_id": base_id,
        "table_id": table_id
    }
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_get_field_info(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle get_field_info tool - get detailed field information"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    
    # Get schema to find the specific table
    schema_result = await gateway.get(f"/bases/{base_id}/schema")
    tables = schema_result.get("tables", [])
    
    target_table = None
    for table in tables:
        if table["id"] == table_id or table["name"] == table_id:
            target_table = table
            break
    
    if not target_table:
        return [TextContent(type="text", text=f"Error: Table '{table_id}' not found")]
    
    # Analyze fields
    field_analysis = []
    for field in target_table.get("fields", []):
        field_info = {
            "name": field["name"],
            "id": field["id"],
            "type": field["type"],
            "description": field.get("description", ""),
            "is_primary": field.get("id") == target_table.get("primaryFieldId"),
            "options": field.get("options", {}),
        }
        
        # Add type-specific insights
        if field["type"] == "singleSelect":
            choices = field.get("options", {}).get("choices", [])
            field_info["choice_count"] = len(choices)
            field_info["choices"] = [choice.get("name") for choice in choices]
        elif field["type"] == "multipleSelect":
            choices = field.get("options", {}).get("choices", [])
            field_info["choice_count"] = len(choices)
            field_info["choices"] = [choice.get("name") for choice in choices]
        elif field["type"] == "formula":
            field_info["formula"] = field.get("options", {}).get("formula", "")
        elif field["type"] == "lookup":
            field_info["linked_table"] = field.get("options", {}).get("relationshipTableId")
            field_info["lookup_field"] = field.get("options", {}).get("fieldIdInLinkedTable")
        
        field_analysis.append(field_info)
    
    response = {
        "table_name": target_table["name"],
        "table_id": target_table["id"],
        "total_fields": len(field_analysis),
        "field_types": {},
        "fields": field_analysis
    }
    
    # Count field types
    for field in field_analysis:
        field_type = field["type"]
        response["field_types"][field_type] = response["field_types"].get(field_type, 0) + 1
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_analyze_table_data(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle analyze_table_data tool - provide data quality insights"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    sample_size = arguments.get("sample_size", 100)
    
    # Get table schema first
    schema_result = await gateway.get(f"/bases/{base_id}/schema")
    tables = schema_result.get("tables", [])
    
    target_table = None
    for table in tables:
        if table["id"] == table_id or table["name"] == table_id:
            target_table = table
            break
    
    if not target_table:
        return [TextContent(type="text", text=f"Error: Table '{table_id}' not found")]
    
    # Get sample records
    params = {"max_records": min(sample_size, 100)}
    records_result = await gateway.get(f"/bases/{base_id}/tables/{table_id}/records", **params)
    records = records_result.get("records", [])
    
    if not records:
        return [TextContent(type="text", text="No records found in table")]
    
    # Analyze data
    field_stats = {}
    total_records = len(records)
    
    for field in target_table.get("fields", []):
        field_name = field["name"]
        field_type = field["type"]
        
        values = []
        empty_count = 0
        
        for record in records:
            value = record.get("fields", {}).get(field_name)
            if value is None or value == "":
                empty_count += 1
            else:
                values.append(value)
        
        field_stat = {
            "field_name": field_name,
            "field_type": field_type,
            "total_records": total_records,
            "filled_count": len(values),
            "empty_count": empty_count,
            "fill_rate": round((len(values) / total_records) * 100, 1) if total_records > 0 else 0
        }
        
        # Type-specific analysis
        if field_type in ["singleLineText", "multilineText", "email", "url"]:
            if values:
                lengths = [len(str(v)) for v in values]
                field_stat["avg_length"] = round(sum(lengths) / len(lengths), 1)
                field_stat["max_length"] = max(lengths)
                field_stat["min_length"] = min(lengths)
        
        elif field_type == "number":
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (ValueError, TypeError):
                    pass
            
            if numeric_values:
                field_stat["avg_value"] = round(sum(numeric_values) / len(numeric_values), 2)
                field_stat["max_value"] = max(numeric_values)
                field_stat["min_value"] = min(numeric_values)
        
        elif field_type in ["singleSelect", "multipleSelect"]:
            unique_values = set()
            for v in values:
                if isinstance(v, list):
                    unique_values.update(v)
                else:
                    unique_values.add(v)
            field_stat["unique_values"] = list(unique_values)
            field_stat["unique_count"] = len(unique_values)
        
        field_stats[field_name] = field_stat
    
    # Overall table analysis
    response = {
        "table_name": target_table["name"],
        "analysis_summary": {
            "records_analyzed": total_records,
            "total_fields": len(target_table.get("fields", [])),
            "avg_fill_rate": round(
                sum(stat["fill_rate"] for stat in field_stats.values()) / len(field_stats), 1
            ) if field_stats else 0
        },
        "field_analysis": field_stats,
        "data_quality_insights": _generate_data_quality_insights(field_stats, total_records)
    }
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_find_duplicates(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle find_duplicates tool - find duplicate records based on specified fields"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    fields = arguments["fields"]
    ignore_empty = arguments.get("ignore_empty", True)
    
    # Get all records (up to 1000 for duplicate checking)
    params = {"max_records": 1000}
    records_result = await gateway.get(f"/bases/{base_id}/tables/{table_id}/records", **params)
    records = records_result.get("records", [])
    
    if not records:
        return [TextContent(type="text", text="No records found in table")]
    
    # Group records by field values
    value_groups = {}
    
    for record in records:
        # Create a tuple of field values for comparison
        values = []
        skip_record = False
        
        for field in fields:
            value = record.get("fields", {}).get(field)
            
            if ignore_empty and (value is None or value == ""):
                skip_record = True
                break
            
            # Normalize value for comparison
            if isinstance(value, str):
                value = value.strip().lower()
            
            values.append(value)
        
        if skip_record:
            continue
        
        key = tuple(values)
        if key not in value_groups:
            value_groups[key] = []
        value_groups[key].append(record)
    
    # Find duplicates
    duplicates = []
    for key, group in value_groups.items():
        if len(group) > 1:
            duplicate_group = {
                "duplicate_values": dict(zip(fields, key)),
                "record_count": len(group),
                "records": [
                    {
                        "id": record["id"],
                        "fields": {field: record.get("fields", {}).get(field) for field in fields},
                        "created_time": record.get("createdTime")
                    }
                    for record in group
                ]
            }
            duplicates.append(duplicate_group)
    
    response = {
        "table_id": table_id,
        "duplicate_check_fields": fields,
        "total_records_checked": len(records),
        "duplicate_groups_found": len(duplicates),
        "total_duplicate_records": sum(len(group["records"]) for group in duplicates),
        "duplicates": duplicates
    }
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_export_table_csv(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle export_table_csv tool - export table data as CSV"""
    base_id = arguments["base_id"]
    table_id = arguments["table_id"]
    fields = arguments.get("fields")
    view = arguments.get("view")
    max_records = arguments.get("max_records", 1000)
    
    # Get records
    params = {"max_records": max_records}
    if view:
        params["view"] = view
    
    records_result = await gateway.get(f"/bases/{base_id}/tables/{table_id}/records", **params)
    records = records_result.get("records", [])
    
    if not records:
        return [TextContent(type="text", text="No records found to export")]
    
    # Determine fields to export
    if not fields:
        # Get all field names from first record
        first_record = records[0]
        fields = list(first_record.get("fields", {}).keys())
    
    # Generate CSV content
    import csv
    import io
    
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    
    # Write header
    header = ["Record ID"] + fields + ["Created Time"]
    writer.writerow(header)
    
    # Write data rows
    for record in records:
        row = [record["id"]]
        
        for field in fields:
            value = record.get("fields", {}).get(field, "")
            
            # Handle different field types
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            elif value is None:
                value = ""
            else:
                value = str(value)
            
            row.append(value)
        
        row.append(record.get("createdTime", ""))
        writer.writerow(row)
    
    csv_content = csv_buffer.getvalue()
    csv_buffer.close()
    
    response = {
        "message": f"Exported {len(records)} records to CSV",
        "table_id": table_id,
        "fields_exported": fields,
        "record_count": len(records),
        "csv_preview": "\n".join(csv_content.split("\n")[:6]),  # First 5 rows + header
        "full_csv_data": csv_content
    }
    
    return [TextContent(type="text", text=json.dumps(response, indent=2))]


async def handle_sync_tables(arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle sync_tables tool - compare and sync data between tables"""
    source_base_id = arguments["source_base_id"]
    source_table_id = arguments["source_table_id"]
    target_base_id = arguments["target_base_id"]
    target_table_id = arguments["target_table_id"]
    key_field = arguments["key_field"]
    dry_run = arguments.get("dry_run", True)
    
    # Get source records
    source_params = {"max_records": 1000}
    source_result = await gateway.get(f"/bases/{source_base_id}/tables/{source_table_id}/records", **source_params)
    source_records = source_result.get("records", [])
    
    # Get target records
    target_params = {"max_records": 1000}
    target_result = await gateway.get(f"/bases/{target_base_id}/tables/{target_table_id}/records", **target_params)
    target_records = target_result.get("records", [])
    
    # Index target records by key field
    target_index = {}
    for record in target_records:
        key_value = record.get("fields", {}).get(key_field)
        if key_value:
            target_index[str(key_value)] = record
    
    # Analyze differences
    to_create = []
    to_update = []
    existing_keys = set()
    
    for source_record in source_records:
        key_value = source_record.get("fields", {}).get(key_field)
        if not key_value:
            continue
        
        key_str = str(key_value)
        existing_keys.add(key_str)
        
        if key_str in target_index:
            # Compare records for differences
            target_record = target_index[key_str]
            if source_record["fields"] != target_record["fields"]:
                to_update.append({
                    "source_record": source_record,
                    "target_record": target_record,
                    "key_value": key_value
                })
        else:
            # Record doesn't exist in target
            to_create.append(source_record)
    
    # Find records that exist in target but not in source
    to_delete = []
    for key_str, target_record in target_index.items():
        if key_str not in existing_keys:
            to_delete.append(target_record)
    
    sync_plan = {
        "sync_summary": {
            "source_records": len(source_records),
            "target_records": len(target_records),
            "records_to_create": len(to_create),
            "records_to_update": len(to_update),
            "records_to_delete": len(to_delete)
        },
        "key_field": key_field,
        "dry_run": dry_run,
        "changes": {
            "create": [{"key": r.get("fields", {}).get(key_field), "fields": r["fields"]} for r in to_create[:5]],
            "update": [{"key": u["key_value"], "changes": "Field differences detected"} for u in to_update[:5]],
            "delete": [{"key": r.get("fields", {}).get(key_field), "id": r["id"]} for r in to_delete[:5]]
        }
    }
    
    if dry_run:
        sync_plan["message"] = "Dry run completed - no changes made. Set dry_run=false to execute sync."
    else:
        sync_plan["message"] = "Sync feature not yet implemented for safety - use dry_run=true to preview changes."
    
    return [TextContent(type="text", text=json.dumps(sync_plan, indent=2))]


def _generate_data_quality_insights(field_stats: Dict[str, Any], total_records: int) -> List[str]:
    """Generate data quality insights from field statistics"""
    insights = []
    
    # Check for fields with low fill rates
    low_fill_fields = [name for name, stats in field_stats.items() if stats["fill_rate"] < 50]
    if low_fill_fields:
        insights.append(f"Low data completion: {', '.join(low_fill_fields)} have <50% fill rate")
    
    # Check for completely empty fields
    empty_fields = [name for name, stats in field_stats.items() if stats["fill_rate"] == 0]
    if empty_fields:
        insights.append(f"Unused fields: {', '.join(empty_fields)} are completely empty")
    
    # Check for high-quality fields
    complete_fields = [name for name, stats in field_stats.items() if stats["fill_rate"] == 100]
    if complete_fields:
        insights.append(f"Complete data: {', '.join(complete_fields)} have 100% fill rate")
    
    if not insights:
        insights.append("Data quality looks good - no major issues detected")
    
    return insights


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
async def http_call_tool(request: ToolCallRequest):
    """HTTP endpoint to call a tool (replaces subprocess stdio)"""
    try:
        logger.info(f"HTTP tool call: {request.name} with args: {request.arguments}")
        
        # Use the same tool calling logic as stdio mode
        result = await call_tool(request.name, request.arguments)
        
        return ToolCallResponse(
            result=result,
            success=True
        )
    except Exception as e:
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
            config = uvicorn.Config(
                http_app, 
                host="0.0.0.0", 
                port=MCP_SERVER_PORT,
                log_level="info"
            )
            server = uvicorn.Server(config)
            await server.serve()
        else:
            # Start MCP server with stdio transport (legacy mode)
            logger.info("🚀 Starting MCP Server in stdio mode")
            async with stdio_server() as (read_stream, write_stream):
                await server.run(read_stream, write_stream, server.create_initialization_options())
    finally:
        # Cleanup secure configuration manager
        if config_manager:
            await close_secrets()
            logger.info("Closed secure configuration manager")


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