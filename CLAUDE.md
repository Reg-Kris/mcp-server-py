# MCP Server - Claude Context

## 🎯 Service Purpose
This is the **brain of Airtable operations** - implementing the Model Context Protocol (MCP) to expose Airtable functionality as tools that LLMs can use. It's the bridge between natural language requests and structured Airtable API calls.

## 🏗️ Current State (✅ PHASE 1 COMPLETE - MODULAR ARCHITECTURE)

### Deployment Status
- **Environment**: ✅ Local Kubernetes (Minikube)
- **Services Running**: ✅ 7 out of 9 services operational
- **Database Analysis**: ✅ Airtable test database analyzed (34 tables, 539 fields)
- **Metadata Tool**: ✅ Table analysis tool executed successfully

### Service Status
- **MCP Tools**: ✅ 14 tools implemented across focused handlers
- **Architecture**: ✅ REFACTORED from 1,374-line monolith → modular handlers (<300 lines each)
- **Protocol**: ✅ Official Python MCP SDK with both stdio AND HTTP modes
- **Performance**: ✅ HTTP mode reduces latency from 200ms to <10ms
- **Security**: ✅ Formula injection protection + OWASP compliance
- **Code Quality**: ✅ Clean separation: handlers/, models/, config.py
- **Testing**: ⚠️ Comprehensive test framework ready, coverage pending
- **Frontend Integration**: ✅ Next.js frontend ready for real-time tool visualization

### Recent Fixes Applied
- ✅ Pydantic v2 compatibility issues resolved
- ✅ Gemini ThinkingConfig configuration fixed
- ✅ SQLAlchemy metadata handling updated
- ✅ Service deployment to Kubernetes completed

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

## 🚀 PHASE 2 PRIORITIES (Next.js Integration & Real-time Features)

### **Critical - Frontend Integration** (HIGH)
1. **WebSocket Tool Execution** - Real-time progress updates for tool execution
2. **Tool Result Streaming** - Stream large CSV exports and data analysis results
3. **Function Call Visualization** - Enhanced metadata for frontend progress tracking

### **Performance & Reliability** (HIGH)
1. **Tool Result Caching** - Redis caching for `list_tables`, `get_records` (5 min TTL)
2. **Tool Analytics** - Usage frequency, execution times, failure patterns
3. **Connection Pool Optimization** - Enhanced HTTP client for better concurrency

### **User Experience** (MEDIUM) 
1. **Tool Suggestions** - Context-aware tool recommendations based on data patterns
2. **Batch Operations UI** - Frontend controls for bulk record operations
3. **Error Recovery** - Automatic retries with exponential backoff

## 🔮 PHASE 3+ Future Enhancements

### **Advanced AI Features** (PHASE 3)
- [ ] **AI-powered tool suggestions** - Gemini recommends optimal tool sequences
- [ ] **Natural language to formula conversion** - Convert English to Airtable formulas
- [ ] **Intelligent data mapping** - Auto-detect field relationships for sync operations
- [ ] **Workflow automation** - Chain multiple tools for complex operations

### **Enterprise Features** (PHASE 4)
- [ ] **Custom tool creation API** - User-defined tools with validation
- [ ] **Tool composition engine** - Multi-step operations with rollback support
- [ ] **Advanced formula builder** - Visual formula creation with AI assistance
- [ ] **Tool versioning & rollback** - Version control for tool configurations

### **Scale & Performance** (PHASE 5)
- [ ] **Multi-base operations** - Cross-base data synchronization
- [ ] **Event-driven triggers** - Real-time data change notifications
- [ ] **GraphQL tool interface** - Flexible tool querying and composition
- [ ] **Edge caching** - Distributed tool result caching

## ⚠️ Known Issues & Weak Points

### **Fixed Issues** ✅
1. ✅ ~~**Process spawning overhead** - 200ms per tool call~~ FIXED with HTTP mode
2. ✅ ~~**No connection pooling** - Inefficient resource usage~~ FIXED in HTTP mode  
3. ✅ ~~**Formula injection risk** - User input not sanitized~~ FIXED with security module
4. ✅ ~~**Monolithic architecture** - 1,374-line file~~ FIXED with modular handlers

### **Current Weak Points** ⚠️
1. **Limited real-time feedback** - Tool execution progress not visible to frontend
2. **No result caching** - Repeated calls hit Airtable API (Redis integration needed)
3. **Error context gaps** - Generic error messages need enhancement
4. **Missing analytics** - No tool usage tracking or performance monitoring
5. **Batch operation limits** - Max 10 records per batch (Airtable API constraint)

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