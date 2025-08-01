"""
Record Handlers for MCP Server
Handles CRUD operations for Airtable records
"""

import json
import logging
from typing import Any, Dict, List
from mcp.types import TextContent

from ..config import gateway

logger = logging.getLogger(__name__)


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