# mcp-server-py

Python MCP (Model Context Protocol) server exposing Airtable tools for LLM integration

## Overview

This microservice implements the Model Context Protocol (MCP) to expose Airtable operations as tools that can be used by LLMs like Claude or Gemini. It communicates with the airtable-gateway-py service to perform actual Airtable operations.

## Available Tools

1. `list_tables` - List all tables in a base
2. `get_records` - Retrieve records from a table
3. `create_record` - Create a new record
4. `update_record` - Update an existing record
5. `delete_record` - Delete a record
6. `search_records` - Search records with filters
7. `create_metadata_table` - Create a table with metadata about all tables in a base

## Quick Start

```bash
# Clone the repository
git clone https://github.com/Reg-Kris/mcp-server-py.git
cd mcp-server-py

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your configuration

# Run the MCP server
python -m src.server

# Or run with stdio transport
python -m src.server --transport stdio
```

## Testing with MCP Inspector

```bash
# Install MCP inspector
npm install -g @modelcontextprotocol/inspector

# Run inspector
mcp-inspector python -m src.server
```

## Environment Variables

```
AIRTABLE_GATEWAY_URL=http://localhost:8002
AIRTABLE_GATEWAY_API_KEY=simple-api-key
LOG_LEVEL=INFO
```

## Integration

This server can be integrated with:
- Claude Desktop (via claude_desktop_config.json)
- Custom LLM applications
- The llm-orchestrator-py service

## Docker

```bash
# Build image
docker build -t mcp-server-py .

# Run container
docker run -p 8001:8001 --env-file .env mcp-server-py
```