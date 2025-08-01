"""
Utility Handlers for MCP Server
Handles utility operations like search, export, sync, and metadata generation
"""

import csv
import io
import json
import logging
from datetime import datetime
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


async def handle_create_metadata_table(arguments: Dict[str, Any], trace_id: str = None) -> List[TextContent]:
    """Handle create_metadata_table tool - analyzes base and creates actual metadata table using Web API"""
    base_id = arguments["base_id"]
    table_name = arguments.get("table_name", "Table Metadata")
    
    if trace_id:
        logger.info(f"[TRACE:{trace_id}] Creating metadata table '{table_name}' for base {base_id}")
    else:
        logger.info(f"Creating metadata table '{table_name}' for base {base_id}")
    
    try:
        # First get the base schema
        schema_result = await gateway.get(f"/bases/{base_id}/schema")
        tables = schema_result.get("tables", [])
        
        if trace_id:
            logger.info(f"[TRACE:{trace_id}] Found {len(tables)} tables to analyze")
        
        # Prepare metadata records
        metadata_records = []
        for table in tables:
            fields = table.get("fields", [])
            
            # Analyze field types
            field_types = {}
            for field in fields:
                field_type = field.get("type", "unknown")
                field_types[field_type] = field_types.get(field_type, 0) + 1
            
            # Create metadata record fields
            metadata_record = {
                "Table Name": table["name"],
                "Table ID": table["id"],
                "Description": table.get("description", "") or "No description",
                "Field Count": len(fields),
                "View Count": len(table.get("views", [])),
                "Field Types": ", ".join([f"{k}: {v}" for k, v in field_types.items()]),
                "Primary Fields": ", ".join([f["name"] for f in fields[:3]]),  # First 3 fields
                "Purpose": _infer_table_purpose(table["name"], fields),
                "Analysis Date": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            }
            metadata_records.append(metadata_record)
        
        # Try to find an existing metadata table first
        existing_metadata_table = None
        for table in tables:
            if table_name.lower() in table["name"].lower() or "metadata" in table["name"].lower():
                existing_metadata_table = table
                break
        
        if existing_metadata_table:
            # Add records to existing metadata table
            table_id = existing_metadata_table["id"]
            
            if trace_id:
                logger.info(f"[TRACE:{trace_id}] Found existing metadata table: {existing_metadata_table['name']}")
            
            # Create records in batches
            batch_size = 10
            created_records = []
            
            for i in range(0, len(metadata_records), batch_size):
                batch = metadata_records[i:i + batch_size]
                
                # Create batch of records
                batch_data = [{"fields": record} for record in batch]
                result = await gateway.post(f"/bases/{base_id}/tables/{table_id}/records/batch", {"records": batch_data})
                created_records.extend(result.get("records", []))
                
                if trace_id:
                    logger.info(f"[TRACE:{trace_id}] Created batch of {len(batch)} metadata records")
            
            result = {
                "success": True,
                "message": f"Successfully created {len(created_records)} metadata records in existing table '{existing_metadata_table['name']}'",
                "table_id": table_id,
                "table_name": existing_metadata_table["name"],
                "table_url": f"https://airtable.com/{base_id}/{table_id}",
                "records_created": len(created_records),
                "metadata_summary": {
                    "total_tables_analyzed": len(tables),
                    "total_fields": sum(len(t.get("fields", [])) for t in tables),
                    "table_types": _categorize_tables(tables)
                }
            }
        else:
            # No existing metadata table found - create a new one using Web API
            if trace_id:
                logger.info(f"[TRACE:{trace_id}] No existing metadata table found, creating new table using Web API")
            
            # Define the fields for the metadata table
            field_definitions = [
                {"name": "Table Name", "type": "singleLineText", "description": "Name of the table"},
                {"name": "Table ID", "type": "singleLineText", "description": "Unique identifier for the table"},
                {"name": "Description", "type": "multilineText", "description": "Table description"},
                {"name": "Field Count", "type": "number", "description": "Number of fields in the table"},
                {"name": "View Count", "type": "number", "description": "Number of views in the table"},
                {"name": "Field Types", "type": "multilineText", "description": "Summary of field types and counts"},
                {"name": "Primary Fields", "type": "multilineText", "description": "First three fields of the table"},
                {"name": "Purpose", "type": "singleSelect", "description": "Inferred purpose of the table",
                 "options": [
                     {"name": "Project/Task Management", "color": "blueLight2"}, 
                     {"name": "Contact/People Management", "color": "greenLight2"}, 
                     {"name": "Product/Inventory Tracking", "color": "yellowLight2"}, 
                     {"name": "Event/Schedule Management", "color": "orangeLight2"},
                     {"name": "Contact Information", "color": "redLight2"}, 
                     {"name": "Financial/Budget Tracking", "color": "purpleLight2"},
                     {"name": "General Data Storage", "color": "grayLight2"}
                 ]},
                {"name": "Analysis Date", "type": "dateTime", "description": "When this analysis was performed"}
            ]
            
            # Create the table using Web API
            table_create_data = {
                "name": table_name,
                "description": "Automatically generated metadata analysis of all tables in this base",
                "fields": field_definitions
            }
            
            try:
                # Create the table
                create_result = await gateway.post(f"/api/web/bases/{base_id}/tables", table_create_data)
                new_table_id = create_result.get("id")
                
                if trace_id:
                    logger.info(f"[TRACE:{trace_id}] Created new metadata table with ID: {new_table_id}")
                
                # Now populate the table with metadata records
                batch_size = 10
                created_records = []
                
                for i in range(0, len(metadata_records), batch_size):
                    batch = metadata_records[i:i + batch_size]
                    
                    # Create batch of records
                    batch_data = [{"fields": record} for record in batch]
                    result = await gateway.post(f"/bases/{base_id}/tables/{new_table_id}/records/batch", {"records": batch_data})
                    created_records.extend(result.get("records", []))
                    
                    if trace_id:
                        logger.info(f"[TRACE:{trace_id}] Created batch of {len(batch)} metadata records")
                
                result = {
                    "success": True,
                    "message": f"Successfully created new metadata table '{table_name}' with {len(created_records)} records",
                    "table_id": new_table_id,
                    "table_name": table_name,
                    "table_url": f"https://airtable.com/{base_id}/{new_table_id}",
                    "records_created": len(created_records),
                    "table_created": True,
                    "metadata_summary": {
                        "total_tables_analyzed": len(tables),
                        "total_fields": sum(len(t.get("fields", [])) for t in tables),
                        "table_types": _categorize_tables(tables)
                    }
                }
                
            except Exception as web_api_error:
                # If Web API fails, provide fallback instructions
                error_msg = f"Failed to create table using Web API: {str(web_api_error)}"
                if trace_id:
                    logger.error(f"[TRACE:{trace_id}] {error_msg}")
                else:
                    logger.error(error_msg)
                
                result = {
                    "success": False,
                    "message": f"Web API table creation failed. Error: {str(web_api_error)}",
                    "action_required": "MANUAL_TABLE_CREATION",
                    "suggested_table_name": table_name,
                    "suggested_fields": field_definitions,
                    "prepared_records": metadata_records,
                    "records_ready": len(metadata_records),
                    "web_api_error": str(web_api_error),
                    "metadata_summary": {
                        "total_tables_analyzed": len(tables),
                        "total_fields": sum(len(t.get("fields", [])) for t in tables),
                        "table_types": _categorize_tables(tables)
                    },
                    "fallback_instructions": [
                        f"1. Go to your Airtable base: https://airtable.com/{base_id}",
                        f"2. Create a new table named '{table_name}'",
                        "3. Add the fields listed in 'suggested_fields' with their specified types",
                        "4. Run this tool again to populate the table with metadata"
                    ]
                }
        
        if trace_id:
            logger.info(f"[TRACE:{trace_id}] Metadata table operation completed: {result.get('success', False)}")
        
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
    except Exception as e:
        error_msg = f"Error creating metadata table: {str(e)}"
        if trace_id:
            logger.error(f"[TRACE:{trace_id}] {error_msg}")
        else:
            logger.error(error_msg)
        
        return [TextContent(type="text", text=json.dumps({
            "success": False,
            "error": error_msg,
            "base_id": base_id,
            "requested_table_name": table_name
        }, indent=2))]


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