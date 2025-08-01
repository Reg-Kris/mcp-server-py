#!/usr/bin/env python3
"""
Configuration and Gateway Client for MCP Server
Contains all configuration constants, security setup, and Airtable Gateway client
"""

import os
import logging
from typing import Any, Dict
import httpx
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# Security imports - add path to find pyairtable-common (development mode)
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../pyairtable-common'))

try:
    from pyairtable_common.security import (
        sanitize_user_query,
        sanitize_field_name,
        build_safe_search_formula,
        validate_filter_formula,
        AirtableFormulaInjectionError
    )
    SECURITY_AVAILABLE = True
    logger.info("✅ Security module loaded - formula injection protection enabled")
except ImportError as e:
    logger.warning(f"⚠️ Security module not available: {e}")
    logger.warning("Formula injection protection DISABLED - this is a security risk!")
    SECURITY_AVAILABLE = False

# Secure configuration imports
try:
    from pyairtable_common.config import initialize_secrets, get_secret, close_secrets, ConfigurationError
    from pyairtable_common.middleware import setup_security_middleware
    SECURE_CONFIG_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ Secure configuration not available: {e}")
    SECURE_CONFIG_AVAILABLE = False

# Initialize secure configuration manager  
config_manager = None
if SECURE_CONFIG_AVAILABLE:
    try:
        config_manager = initialize_secrets()
        logger.info("✅ Secure configuration manager initialized")
    except Exception as e:
        logger.error(f"💥 Failed to initialize secure configuration: {e}")
        raise

# Configuration Constants
AIRTABLE_GATEWAY_URL = os.getenv("AIRTABLE_GATEWAY_URL", "http://localhost:8002")

# Get API key from secure manager or fallback to environment
AIRTABLE_GATEWAY_API_KEY = None
if config_manager:
    try:
        AIRTABLE_GATEWAY_API_KEY = get_secret("API_KEY")  # Use internal API_KEY for gateway communication
    except Exception as e:
        logger.error(f"💥 Failed to get API_KEY from secure config: {e}")
        raise ValueError("API_KEY could not be retrieved from secure configuration")
else:
    AIRTABLE_GATEWAY_API_KEY = os.getenv("AIRTABLE_GATEWAY_API_KEY")
    if not AIRTABLE_GATEWAY_API_KEY:
        logger.error("💥 CRITICAL: AIRTABLE_GATEWAY_API_KEY environment variable is required")
        raise ValueError("AIRTABLE_GATEWAY_API_KEY environment variable is required")

MCP_SERVER_NAME = os.getenv("MCP_SERVER_NAME", "airtable-mcp")
MCP_SERVER_VERSION = os.getenv("MCP_SERVER_VERSION", "1.0.0")
MCP_SERVER_MODE = os.getenv("MCP_SERVER_MODE", "stdio")  # "stdio" or "http"
MCP_SERVER_PORT = int(os.getenv("MCP_SERVER_PORT", "8001"))

# CORS Configuration
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")


class AirtableGatewayClient:
    """HTTP client for communicating with the Airtable Gateway service"""
    
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def get(self, endpoint: str, **params) -> Dict[str, Any]:
        """Make GET request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    async def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make POST request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.post(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
    
    async def patch(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Make PATCH request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.patch(url, headers=self.headers, json=data)
        response.raise_for_status()
        return response.json()
    
    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """Make DELETE request to gateway"""
        url = f"{self.base_url}{endpoint}"
        response = await self.client.delete(url, headers=self.headers)
        response.raise_for_status()
        return response.json()


# Initialize singleton gateway client
gateway = AirtableGatewayClient(AIRTABLE_GATEWAY_URL, AIRTABLE_GATEWAY_API_KEY)


async def cleanup_config():
    """Cleanup configuration resources"""
    if config_manager:
        await close_secrets()
        logger.info("Closed secure configuration manager")