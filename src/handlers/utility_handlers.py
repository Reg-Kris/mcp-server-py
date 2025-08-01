"""
Utility Handlers for MCP Server
Handles utility operations like search, export, sync, and metadata generation
"""

import csv
import io
import json
import logging
from typing import Any, Dict, List
from mcp.types import TextContent

from ..config import gateway, SECURITY_AVAILABLE
if SECURITY_AVAILABLE:
    from pyairtable_common.security import build_safe_search_formula, AirtableFormulaInjectionError

logger = logging.getLogger(__name__)


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