"""Customer Verification Tools

This module provides customer identity verification functionality.
"""

import json
import logging
from util import call_iservice

log = logging.getLogger("mcp_server")


def register_verification_tools(mcp, session_client):
    """Register customer verification tools to the MCP server.
    
    Args:
        mcp: FastMCP server instance
        session_client: session store client
    """

    @mcp.tool(structured_output=False)
    async def checkCustomer(contactId: str) -> str:
        """Check if customer information exists in the current session.

        INTERNAL TOOL - Do NOT call this tool directly from chatbot.

        Args:
            contactId: The current contact ID from system-variables.

        Returns:
            JSON string with one of the following statuses:
            - "customer_found": Customer information exists in the session, you can proceed with operations that require customer identity.
            - "customer_not_found": Customer information not found, you MUST call setCustomerByPhoneAndName to set customer information first.
        """
        session = session_client.get(contactId)
        if not session:
            return json.dumps({"status": "customer_not_found"})
        
        return json.dumps({"status": "customer_found"})

    @mcp.tool(structured_output=False)
    async def setCustomerByPhoneAndName(contactId: str, bu: str, countryCode: str, phoneNumber: str, customerName: str) -> str:
        """set customer information by phone number and customer name.

        INTERNAL TOOL - Do NOT call this tool directly from chatbot.

        Follow this verification flow strictly before calling:

        CASE A — customer_info.userNumber is available:
        1. Parse countryCode and phoneNumber from customer_info.userNumber.
           Format: the digits after "+" up to the first space are the countryCode;
           the remaining digits are the phoneNumber.
        2. Present the parsed number to the customer as "+{countryCode} {phoneNumber}"
           and ask them to confirm whether it is correct.
        3. Wait for the customer's confirmation:
           - If correct: ask for full name (customerName), then present
             "+{countryCode} {phoneNumber}, name: {customerName}" and ask to confirm.
             Only call this tool after receiving explicit confirmation.
           - If incorrect: follow CASE B below.

        CASE B — customer_info.userNumber is empty or customer says number is wrong:
        1. Ask for international dialing code ONLY. Wait for response.
        2. Ask for phone number ONLY. Wait for response.
        3. Ask for full name ONLY. Wait for response.
        4. Present "+{countryCode} {phoneNumber}, name: {customerName}" and ask to confirm.
           Only call this tool after receiving explicit confirmation.

        Do NOT provide example phone numbers or country codes to the customer.
        Do NOT invent or suggest any specific number format to the customer.

        Args:
            contactId: The current contact ID from system-variables.
            bu: Use the BU value from customer_info.
            countryCode: International dialing code digits only, no "+" prefix.
            phoneNumber: Phone number digits only, no country code.
            customerName: The customer's full name.

        Returns:
            JSON string with one of the following statuses:
            - "success": Customer information set successfully.
        """
        countryCode = countryCode.strip()
        phoneNumber = phoneNumber.strip()
        customerName = customerName.strip()
        
        # Set customer information in session
        session_client.put(contactId, {
            "bu": bu,
            "countryCode": countryCode,
            "phoneNumber": phoneNumber,
            "customerName": customerName
        })
        
        return json.dumps({"status": "success",})
