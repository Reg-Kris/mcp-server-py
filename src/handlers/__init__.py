"""
MCP Server Handlers Package
Contains all MCP tool handlers organized by functionality
"""

from .table_handlers import *
from .record_handlers import *
from .analysis_handlers import *
from .utility_handlers import *

__all__ = [
    # Re-export all handler functions
    "handle_list_tables",
    "handle_get_records", 
    "handle_get_field_info",
    "handle_create_record",
    "handle_update_record",
    "handle_delete_record",
    "handle_batch_create_records",
    "handle_batch_update_records",
    "handle_analyze_table_data",
    "handle_find_duplicates",
    "handle_search_records",
    "handle_create_metadata_table",
    "handle_export_table_csv",
    "handle_sync_tables"
]