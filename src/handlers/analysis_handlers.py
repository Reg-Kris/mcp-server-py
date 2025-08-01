"""
Analysis Handlers for MCP Server
Handles data analysis operations like statistics and duplicate detection
"""

import json
import logging
from typing import Any, Dict, List
from mcp.types import TextContent

from ..config import gateway

logger = logging.getLogger(__name__)


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