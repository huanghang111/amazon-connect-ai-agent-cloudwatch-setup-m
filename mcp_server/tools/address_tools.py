"""Address Tool

Provides address verification and administrative region parsing functionality.
"""

import json
import logging

from util import call_iservice

log = logging.getLogger("mcp_server")


def register_address_tools(mcp):
    """Register address-related tools to the MCP server.
    
    Args:
        mcp: FastMCP server instance
    """
    
    @mcp.tool(structured_output=False)
    async def verifyAddress(rawAddress: str) -> str:
        """Verify and parse administrative regions from user-provided address.
        
        This tool parses a user-described address into structured administrative regions
        (province, city, district).

        INTERNAL TOOL - Do NOT call this tool directly from chatbot.
        
        Args:
            rawAddress: User-described address string
        
        Returns:
            {
                "status": "success" or "failed",
                "data": {                         # Present only when status is "success"
                    "province": "...",
                    "city": "...",
                    "district": "...",
                },
                "message": "..."                 # Error reason when status is "failed"
            }
        """
        api_result = await call_iservice('/api/iservice-ccm/bot/api/address/verification', {'rawAddress': rawAddress})
        
        if api_result.get('resultCode') == 'ISC-000':
            api_data = api_result.get('data', {})
            if api_data.get('matched'):
                return json.dumps({
                    "status": "success",
                    "data": {
                        "province": api_data.get('province'),
                        "city": api_data.get('city'),
                        "district": api_data.get('district'),
                    }
                }, ensure_ascii=False)
            else:
                return json.dumps({
                    "status": "failed",
                    "message": api_data.get('unmatchedRemark') or "Address not matched"
                }, ensure_ascii=False)
        
        return json.dumps({
            "status": "failed",
            "message": f"API error: {api_result.get('resultMsg', 'Unknown error')}"
        }, ensure_ascii=False)
