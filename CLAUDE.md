# MCP Server - Claude Context

## 🎯 Service Purpose
This is the **brain of Airtable operations** - implementing the Model Context Protocol (MCP) to expose Airtable functionality as tools that LLMs can use. It's the bridge between natural language requests and structured Airtable API calls.

## 🏗️ Current State
- **MCP Tools**: ✅ 7 tools implemented and working
- **Protocol**: ✅ Official Python MCP SDK
- **Transport**: ⚠️ stdio only (subprocess spawning overhead)
- **Caching**: ❌ Tool results not cached
- **Testing**: ❌ No automated tests
- **Performance**: ⚠️ New process per tool call (200ms overhead)

## 🛠️ Available MCP Tools

### 1. `list_tables`
- **Purpose**: List all tables in an Airtable base
- **Input**: `base_id` (required)
- **Output**: Table list with metadata

### 2. `get_records`
- **Purpose**: Retrieve records from a table
- **Input**: `base_id`, `table_id`, `max_records`, `view`, `filter_by_formula`
- **Output**: Paginated record list

### 3. `create_record`
- **Purpose**: Create new record in table
- **Input**: `base_id`, `table_id`, `fields` (object)
- **Output**: Created record with ID

### 4. `update_record`
- **Purpose**: Update existing record
- **Input**: `base_id`, `table_id`, `record_id`, `fields`
- **Output**: Updated record

### 5. `delete_record`
- **Purpose**: Delete a record
- **Input**: `base_id`, `table_id`, `record_id`
- **Output**: Deletion confirmation

### 6. `search_records`
- **Purpose**: Advanced search with filtering
- **Input**: `base_id`, `table_id`, `query`, `fields[]`, `max_records`
- **Output**: Matching records

### 7. `create_metadata_table`
- **Purpose**: Analyze base and create metadata
- **Input**: `base_id`, `table_name`
- **Output**: Base analysis with table descriptions

## 🚀 Immediate Priorities

1. **Fix Process Spawning** (CRITICAL)
   ```python
   # Current: New process per call (BAD)
   process = await asyncio.create_subprocess_exec(...)
   
   # Needed: Persistent connection pool
   class MCPConnectionPool:
       async def get_connection(self):
           # Reuse existing process
   ```

2. **Add Tool Result Caching** (HIGH)
   - Cache `list_tables` results (5 min TTL)
   - Cache `get_records` with query fingerprint
   - Invalidate on write operations

3. **Implement Tool Analytics** (MEDIUM)
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
1. **Process spawning overhead** - 200ms per tool call
2. **No connection pooling** - Inefficient resource usage
3. **Formula injection risk** - User input not sanitized
4. **Limited error context** - Generic error messages

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