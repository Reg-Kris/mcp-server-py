"""
Table Handlers for MCP Server
Handles operations related to Airtable tables and schema
"""

import json
import logging
from typing import Any, Dict, List
from mcp.types import TextContent

from ..config import gateway, SECURITY_AVAILABLE
if SECURITY_AVAILABLE:
    from pyairtable_common.security import validate_filter_formula, AirtableFormulaInjectionError

logger = logging.getLogger(__name__)


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