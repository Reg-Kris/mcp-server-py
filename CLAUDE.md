# MCP Server - Claude Context

## 🎯 Service Purpose
This is the **brain of Airtable operations** - implementing the Model Context Protocol (MCP) to expose Airtable functionality as tools that LLMs can use. It's the bridge between natural language requests and structured Airtable API calls.

## 🏗️ Current State
- **MCP Tools**: ✅ 7 tools implemented and working
- **Protocol**: ✅ Official Python MCP SDK
- **Transport**: ✅ Both stdio AND HTTP modes supported (HTTP eliminates subprocess overhead!)
- **Caching**: ❌ Tool results not cached
- **Security**: ✅ Formula injection protection implemented
- **Testing**: ❌ No automated tests
- **Performance**: ✅ HTTP mode reduces latency from 200ms to <10ms

## 🛠️ Available MCP Tools (13 Total)

### **Basic CRUD Operations** (7 tools) ✅
1. **`list_tables`** - List all tables in an Airtable base
2. **`get_records`** - Retrieve records with filtering and pagination
3. **`create_record`** - Create single record in table
4. **`update_record`** - Update existing record 
5. **`delete_record`** - Delete a record
6. **`search_records`** - Advanced search with natural language queries
7. **`create_metadata_table`** - Analyze base and create comprehensive metadata

### **Advanced Batch Operations** (2 tools) ✅ NEW!
8. **`batch_create_records`** - Create up to 10 records in single operation
   - **Perfect for**: Bulk data import, CSV uploads, data migration
   - **Input**: `base_id`, `table_id`, `records[]` (max 10)
   - **Output**: Created records with confirmation

9. **`batch_update_records`** - Update multiple records efficiently  
   - **Perfect for**: Mass data updates, status changes, field corrections
   - **Input**: `base_id`, `table_id`, `records[]` (with IDs and fields)
   - **Output**: Update results with error handling

### **Data Analysis & Quality** (3 tools) ✅ NEW!
10. **`get_field_info`** - Detailed field analysis and schema insights
    - **Perfect for**: Understanding data structure, field types, relationships
    - **Input**: `base_id`, `table_id`
    - **Output**: Field types, options, formulas, linked tables

11. **`analyze_table_data`** - Data quality analysis with statistics
    - **Perfect for**: Data auditing, completeness checks, insights
    - **Input**: `base_id`, `table_id`, `sample_size`
    - **Output**: Fill rates, value distributions, data quality insights

12. **`find_duplicates`** - Smart duplicate detection across fields
    - **Perfect for**: Data cleaning, deduplication, quality control
    - **Input**: `base_id`, `table_id`, `fields[]`, `ignore_empty`
    - **Output**: Duplicate groups with record details

### **Export & Sync Operations** (2 tools) ✅ NEW!
13. **`export_table_csv`** - Export table data to CSV format
    - **Perfect for**: Data analysis, reporting, external processing
    - **Input**: `base_id`, `table_id`, `fields[]`, `view`, `max_records`
    - **Output**: CSV data with preview and full content

14. **`sync_tables`** - Compare and sync data between tables
    - **Perfect for**: Data migration, backup, synchronization
    - **Input**: `source_base_id`, `source_table_id`, `target_base_id`, `target_table_id`, `key_field`
    - **Output**: Sync plan with differences and change preview

## 🚀 Recent Improvements

1. **HTTP Mode Added** ✅ (COMPLETED)
   ```python
   # Start in HTTP mode for better performance
   python -m src.server --http
   # OR set environment variable
   MCP_SERVER_MODE=http
   ```

2. **Formula Injection Protection** ✅ (COMPLETED)
   - User queries are sanitized
   - Field names are validated
   - Formulas are checked against whitelist
   - Dangerous patterns blocked

3. **Performance Optimization** ✅ (COMPLETED)
   - HTTP mode eliminates subprocess overhead
   - Connection pooling for all downstream calls
   - 200ms → <10ms latency improvement

## 🚀 Remaining Priorities

1. **Add Tool Result Caching** (HIGH)
   - Cache `list_tables` results (5 min TTL)
   - Cache `get_records` with query fingerprint
   - Invalidate on write operations

2. **Implement Tool Analytics** (MEDIUM)
   - Track tool usage frequency
   - Monitor execution times
   - Log failure patterns

## 🔮 Future Enhancements

### Phase 1 (Next Sprint)
- [ ] HTTP/SSE transport option (eliminate subprocess)
- [ ] Tool result validation with Pydantic
- [ ] Parallel tool execution support
- [ ] Error recovery with retries

### Phase 2 (Next Month)
- [ ] Dynamic tool registration
- [ ] Custom tool creation API
- [ ] Tool composition (multi-step operations)
- [ ] Advanced formula builder

### Phase 3 (Future)
- [ ] AI-powered tool suggestions
- [ ] Natural language to formula conversion
- [ ] Batch tool operations
- [ ] Tool versioning support

## ⚠️ Known Issues
1. ✅ ~~**Process spawning overhead** - 200ms per tool call~~ FIXED with HTTP mode
2. ✅ ~~**No connection pooling** - Inefficient resource usage~~ FIXED in HTTP mode
3. ✅ ~~**Formula injection risk** - User input not sanitized~~ FIXED with security module
4. **Limited error context** - Generic error messages
5. **No result caching** - Repeated calls hit Airtable API

## 🧪 Testing Strategy
```python
# Priority test coverage:
- Unit tests for each tool handler
- Integration tests with mock Airtable Gateway
- MCP protocol compliance tests
- Performance benchmarks for tool execution
```

## 🔧 Technical Details
- **Protocol**: MCP 1.0 specification
- **Transport**: stdio (JSON-RPC over stdin/stdout)
- **Python**: 3.12 with asyncio
- **Dependencies**: mcp SDK, httpx

## 📊 Performance Targets
- **Tool Execution**: < 100ms (excluding API calls)
- **Process Pool**: 5-10 persistent connections
- **Memory Usage**: < 100MB per process
- **Concurrent Tools**: 20+ simultaneous executions

## 🤝 Service Communication
```python
# Current flow:
LLM Orchestrator --stdio--> MCP Server --HTTP--> Airtable Gateway

# Future optimization:
LLM Orchestrator --HTTP/gRPC--> MCP Server --HTTP--> Airtable Gateway
```

## 💡 Development Tips
1. Test tools with MCP Inspector: `mcp-inspector python -m src.server`
2. Always validate tool inputs before forwarding
3. Include detailed error messages for debugging
4. Log all tool executions for analytics

## 🚨 Critical Configuration
```python
# Required environment variables:
AIRTABLE_GATEWAY_URL=http://airtable-gateway:8002
AIRTABLE_GATEWAY_API_KEY=internal_api_key
MCP_SERVER_NAME=airtable-mcp
LOG_LEVEL=INFO
```

## 🔒 Security Considerations
- **Input Validation**: Sanitize all formula inputs
- **Rate Limiting**: Implement per-tool rate limits
- **Access Control**: Future: tool-level permissions
- **Audit Logging**: Track all tool usage

## 📈 Monitoring Metrics
```python
# Key metrics to track:
mcp_tool_calls_total{tool_name}      # Tool usage frequency
mcp_tool_duration_seconds{tool_name}  # Execution time
mcp_tool_errors_total{tool_name}     # Error rate
mcp_active_connections               # Connection pool size
```

Remember: This service is the **intelligence layer** that makes Airtable operations accessible to AI. Every optimization here directly improves the AI's ability to work with data!