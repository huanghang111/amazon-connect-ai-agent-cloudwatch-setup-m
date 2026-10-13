"""Repair Service Tools

This module provides repair service management functionality.
"""

import json
import logging
from util import call_iservice

log = logging.getLogger("mcp_server")


def register_repair_tools(mcp, session_client):
    """Register repair service tools to the MCP server.

    Args:
        mcp: FastMCP server instance
        session_client: session store client
    """

    @mcp.tool(structured_output=False)
    async def createRepair(
        contactId: str,
        addressRemark: str,
        description: str,
        zipCode: str,
        email: str,
        srType: str,
        srSubType: str,
        productModel: str = "",
        serialNumber: str = "",
        productCategory: str = "",
        brand: str = "",
    ) -> str:
        """Create a repair service request.

        ═══════════════════════════════════════════════════════════════════════════════════
        INVOCATION TRIGGER (CRITICAL)
        ═══════════════════════════════════════════════════════════════════════════════════

        ONLY initiate this tool (field collection, confirmation summary, and tool call) when the customer EXPLICITLY requests to create a repair order. Do NOT offer, suggest, or proactively propose creating a repair order.

        Examples of explicit requests:
        - "I need to create a repair order"
        - "I want to submit a repair request"
        - "Can you help me set up a repair?"

        The following situations DO NOT constitute a request to create a repair order, and you MUST NOT proactively propose creating one in any of them:
        - faqSearch returned no match / no relevant information.
        - The customer said the FAQ answer did not help, or answered "no" to "Did this solve your problem?".
        - The customer describes a malfunction, defect, or problem with a product (describing a problem is NOT the same as asking to book a repair).
        - You cannot otherwise help and are considering next steps.

        NEVER bridge from "I couldn't find an answer" to "would you like to create a repair order?". Only the customer may open that door, with an explicit request.

        If the customer is only asking questions or gathering information, respond to their questions (e.g., via FAQ search, order status queries) without initiating createRepair.

        ═══════════════════════════════════════════════════════════════════════════════════
        PREREQUISITE
        ═══════════════════════════════════════════════════════════════════════════════════

        Call checkCustomer BEFORE collecting any fields from the customer.

        ═══════════════════════════════════════════════════════════════════════════════════
        FIELD COLLECTION FLOW
        ═══════════════════════════════════════════════════════════════════════════════════

        Fixed values (set automatically — do NOT ask the customer):
        - srType = "Inquiry"
        - srSubType = "Product"
        - Customer name — retrieved automatically from verified session (do NOT ask the customer)

        COLLECTION ORDER — ask ONE field per message, IN THIS EXACT ORDER (do NOT reorder):
        1. Brand — OPTIONAL
        2. Product category — OPTIONAL
        3. Model — OPTIONAL
        4. Serial number — OPTIONAL
        5. Issue — REQUIRED
        6. Service address — OPTIONAL
        7. Zip code — REQUIRED
        8. Email — REQUIRED

        Move to the confirmation phase only after every field above has been asked once, in order, and all REQUIRED fields hold valid values.

        ═══════════════════════════════════════════════════════════════════════════════════
        FIELD VALIDATION RULES
        ═══════════════════════════════════════════════════════════════════════════════════

        REQUIRED fields (Issue, Zip code, Email):
        - Must be collected and hold valid values before the confirmation phase.
        - If the customer has failed to provide valid values for REQUIRED fields a cumulative total of 3 times in this createRepair session (across any REQUIRED field), stop collecting and transfer to a human agent (see escalation-rules in prompt).

        OPTIONAL fields (Brand, Product category, Model, Serial number, Service address):
        - Ask each one, but do NOT require an answer — the customer may not know.
        - If the customer says they don't know, don't have it, or wants to skip, accept it, leave the field blank, and move to the next field.
        - Do NOT re-ask a skipped OPTIONAL field.
        - Do NOT count skipping an OPTIONAL field toward escalation.

        ═══════════════════════════════════════════════════════════════════════════════════
        CONFIRMATION PROTOCOL
        ═══════════════════════════════════════════════════════════════════════════════════

        This tool requires user confirmation. After collecting ALL fields (both REQUIRED and OPTIONAL), present a complete summary of every field to the customer in a single <message></message>. The summary must list every field and its value, then ask the customer to confirm. Only call the tool after receiving explicit confirmation.

        The confirmation summary is presented EXACTLY ONCE. There are two and only two outcomes:
        - Customer says "Yes" or equivalent approval: Proceed immediately with the tool call. Do NOT ask for confirmation again.
        - Customer says "No", indicates something is wrong, or wants to change anything: Tell the customer you cannot modify the details here in this conversation and transfer to a human agent to help with modifications.

        ═══════════════════════════════════════════════════════════════════════════════════
        PARAMETER MAPPING (Prompt → Tool)
        ═══════════════════════════════════════════════════════════════════════════════════

        USER-PROVIDED INPUTS (from customer via Prompt):
            brand (str, optional)
                Mapping: Brand (field #1)
                Customer-provided product brand.

            productCategory (str, optional)
                Mapping: Product category (field #2)
                Customer-provided main product category (e.g., Refrigerator, Air Conditioner).

            productModel (str, optional)
                Mapping: Model (field #3)
                Customer-provided product model number.

            serialNumber (str, optional)
                Mapping: Serial number (field #4)
                Customer-provided product serial number.

            description (str, required)
                Mapping: Issue (field #5)
                Customer-provided problem description.

            addressRemark (str, optional)
                Mapping: Service address (field #6)
                Customer-provided service address as raw text.

            zipCode (str, required)
                Mapping: Zip code (field #7)
                Customer-provided postal/zip code of service address.

            email (str, required)
                Mapping: Email (field #8)
                Customer-provided email address.

        AUTO-POPULATED FROM SESSION (NOT collected from customer):
            contactId (str, required)
                Source: system-variables ({{$.contactId}})
                Used to look up verified customer information in session.

            srType (str, fixed)
                Fixed value: "Inquiry" (set automatically by Prompt, do NOT ask customer)
                Tool will use provided value or default to "Inquiry" if empty.

            srSubType (str, fixed)
                Fixed value: "Product" (set automatically by Prompt, do NOT ask customer)
                Tool will use provided value or default to "Product" if empty.

            countryCode (NOT a parameter)
                Source: Retrieved from session using contactId
                Used when calling API, not exposed as function parameter.

            phoneNumber (NOT a parameter)
                Source: Retrieved from session using contactId
                Used when calling API, not exposed as function parameter.

            customerName (NOT a parameter)
                Source: Retrieved automatically from verified session using contactId
                NOTE: Customer name is NOT collected from user — Prompt never asks for it.
                It is always taken from verified session data to ensure consistency.
                Used when calling API, not exposed as function parameter.

        Returns:
            JSON string with one of the following statuses:
            - "success": Repair request created successfully, includes srNumber (the service request number assigned to this order)
            - "failed": Creation failed, includes reason for failure

        POST-CALL: After a SUCCESSFUL tool call, append the questionFinish placeholder ({{$.Custom.questionFinish}}) at the end of your customer-facing message, separated by one blank line. Do NOT append it if the call failed or returned an error. Appending the questionFinish placeholder counts as your single question for that turn.
        """
        log.info(f"createRepair: contactId='{contactId}'")

        session = session_client.get(contactId)
        if not session:
            return json.dumps({"status": "failed", "message": "customer_not_found"})

        productCategory = productCategory.strip() if productCategory else ""
        productModel = productModel.strip() if productModel else ""
        serialNumber = serialNumber.strip() if serialNumber else ""
        brand = brand.strip() if brand else ""
        addressRemark = addressRemark.strip()
        description = description.strip()
        zipCode = zipCode.strip()
        email = email.strip()
        srType = srType.strip() if srType else "Inquiry"
        srSubType = srSubType.strip() if srSubType else "Product"

        # Format description with productCategory if provided
        formatted_description = (
            f"[{productCategory}] {description}" if productCategory else description
        )

        api_result = await call_iservice(
            "/api/iservice-ccm/bot/api/chatBotCreateDraftSr",
            {
                "bu": session.get("bu"),
                "countryCode": session.get("countryCode"),
                "phoneNumber": session.get("phoneNumber"),
                "customerName": session.get("customerName"),
                "srType": srType,
                "srSubType": srSubType,
                "productModel": productModel,
                "serialNumber": serialNumber,
                "brand": brand,
                "addressRemark": addressRemark,
                "description": formatted_description,
                "zipCode": zipCode,
                "email": email,
                "contactId": contactId,
                "status": "Draft",
            },
        )

        error_msg = None
        if api_result.get("resultCode") == "ISC-000":
            api_data = api_result.get("data", {})
            if api_data.get("success"):
                sr_number = api_data.get("srNumber", "Unknown")
                return json.dumps(
                    {
                        "status": "success",
                        "message": f"Your repair order has been created successfully. ",
                        "srNumber": sr_number,
                    }
                )
            else:
                error_msg = api_data.get("message", "Unknown error")
        else:
            error_msg = {api_result.get("resultMsg", "Unknown error")}

        log.info(f"createRepair error: {error_msg}")
        return json.dumps(
            {"status": "failed", "message": "Failed to create repair order."}
        )

    @mcp.tool(structured_output=False)
    async def listRepairs(contactId: str) -> str:
        """Query the list of customer's repair service requests.

        WHEN TO CALL: ONLY call this tool when the customer explicitly requests to QUERY or VIEW their repair/service order list.
        Trigger phrases include: "show my repairs", "list my service requests", "what repairs do I have", "check my orders", etc.

        IMPORTANT: Call checkCustomer BEFORE asking the customer anything.

        ═══════════════════════════════════════════════════════════════════════════════════
        FIELD COLLECTION FLOW
        ═══════════════════════════════════════════════════════════════════════════════════

        Collection priority:
        1. Brand (OPTIONAL) — filter by brand; if not provided, list all orders for this contact
        2. Status filter (OPTIONAL) — e.g. "pending", "completed", "in-progress"; if not specified, return all

        Confirmation: NO — Attempt the tool call immediately with best interpretation after collecting parameters (no confirmation summary needed).

        ═══════════════════════════════════════════════════════════════════════════════════
        PARAMETER MAPPING (Prompt → Tool)
        ═══════════════════════════════════════════════════════════════════════════════════

        AUTO-POPULATED FROM SESSION (NOT collected from customer):
            contactId (str, required)
                Source: system-variables ({{$.contactId}})
                Used to look up verified customer information in session.

            countryCode, phoneNumber, customerName (NOT parameters)
                Source: Retrieved from session using contactId
                Used when calling API, not exposed as function parameters.

        Returns:
            JSON string with one of the following statuses:
            - "success": Successfully retrieved repair list, includes repairs array
            - "failed": Query failed, includes reason for failure

        POST-CALL: After a SUCCESSFUL tool call, append the questionFinish placeholder ({{$.Custom.questionFinish}}) at the end of your customer-facing message, separated by one blank line. Do NOT append it if the call failed or returned an error. Appending the questionFinish placeholder counts as your single question for that turn.
        """
        log.info(f"listRepairs: contactId='{contactId}'")

        session = session_client.get(contactId)
        if not session:
            return json.dumps({"status": "failed", "message": "customer_not_found"})

        api_result = await call_iservice(
            "/api/iservice-ccm/bot/api/serviceReques/list",
            {
                "bu": session.get("bu"),
                "countryCode": session.get("countryCode"),
                "phoneNumber": session.get("phoneNumber"),
                "customerName": session.get("customerName"),
                "srCount": 10,
            },
        )

        error_msg = None
        if api_result.get("resultCode") == "ISC-000":
            api_data = api_result.get("data", {})
            service_requests = api_data.get("serviceRequestItems", [])

            if isinstance(service_requests, list):
                return json.dumps(
                    {
                        "status": "success",
                        "message": f"Found {len(service_requests)} repair request(s)",
                        "count": len(service_requests),
                        "repairs": service_requests,
                    },
                    ensure_ascii=False,
                )
            else:
                error_msg = "Invalid data format returned from API"
        else:
            error_msg = api_result.get("resultMsg", "Unknown error")

        log.info(f"listRepairs error: {error_msg}")
        return json.dumps({"status": "failed", "message": "listRepairs error"})

    @mcp.tool(structured_output=False)
    async def cancelRepair(contactId: str, srNumber: str, cancelReason: str) -> str:
        """Cancel a repair service request.

        WHEN TO CALL: ONLY call this tool when the customer explicitly requests to CANCEL a specific repair/service order.
        Trigger phrases include: "cancel my repair", "cancel service request", "I want to cancel order", etc.

        IMPORTANT: Call checkCustomer BEFORE asking the customer anything.

        ═══════════════════════════════════════════════════════════════════════════════════
        FIELD COLLECTION FLOW
        ═══════════════════════════════════════════════════════════════════════════════════

        Collection priority (ask ONE field per message):
        1. Repair order ID (srNumber) — REQUIRED — which order to cancel?
        2. Reason for cancellation (cancelReason) — OPTIONAL — feedback only

        ═══════════════════════════════════════════════════════════════════════════════════
        CONFIRMATION PROTOCOL
        ═══════════════════════════════════════════════════════════════════════════════════

        This tool requires user confirmation. After collecting all fields, present a summary and ask the customer to confirm once before calling the tool.

        The confirmation summary is presented EXACTLY ONCE. There are two and only two outcomes:
        - Customer says "Yes" or equivalent approval: Proceed immediately with the tool call. Do NOT ask for confirmation again.
        - Customer says "No", indicates something is wrong, or wants to change anything: Tell the customer you cannot modify the details here in this conversation and transfer to a human agent to help with modifications.

        ═══════════════════════════════════════════════════════════════════════════════════
        PARAMETER MAPPING (Prompt → Tool)
        ═══════════════════════════════════════════════════════════════════════════════════

        USER-PROVIDED INPUTS (from customer via Prompt):
            srNumber (str, required)
                Mapping: Repair order ID (field #1)
                Customer-provided service request number to cancel.

            cancelReason (str, optional)
                Mapping: Reason for cancellation (field #2)
                Customer-provided feedback about why they are cancelling.

        AUTO-POPULATED FROM SESSION (NOT collected from customer):
            contactId (str, required)
                Source: system-variables ({{$.contactId}})
                Used to look up verified customer information in session.

            countryCode, phoneNumber, customerName (NOT parameters)
                Source: Retrieved from session using contactId
                Used when calling API, not exposed as function parameters.

        Returns:
            JSON string with one of the following statuses:
            - "success": Repair cancelled successfully
            - "failed": Cancellation failed, includes reason for failure

        POST-CALL: After a SUCCESSFUL tool call, append the questionFinish placeholder ({{$.Custom.questionFinish}}) at the end of your customer-facing message, separated by one blank line. Do NOT append it if the call failed or returned an error. Appending the questionFinish placeholder counts as your single question for that turn.
        """
        log.info(
            f"cancelRepair: contactId='{contactId}', srNumber='{srNumber}', cancelReason='{cancelReason}'"
        )

        session = session_client.get(contactId)
        if not session:
            return json.dumps({"status": "failed", "message": "customer_not_found"})

        srNumber = srNumber.strip()
        cancelReason = cancelReason.strip()

        api_result = await call_iservice(
            "/api/iservice-ccm/bot/api/serviceReques/cancel",
            {
                "bu": session.get("bu"),
                "countryCode": session.get("countryCode"),
                "phoneNumber": session.get("phoneNumber"),
                "customerName": session.get("customerName"),
                "srNumber": srNumber,
                "cancelReason": cancelReason,
            },
        )

        error_msg = None
        if api_result.get("resultCode") == "ISC-000":
            api_data = api_result.get("data", {})
            if api_data.get("success"):
                return json.dumps(
                    {"status": "success", "message": "Repair cancelled successfully"}
                )
            else:
                error_msg = api_data.get("message", "Unknown error")
        else:
            error_msg = api_result.get("resultMsg", "Unknown error")

        log.info(f"cancelRepair error: {error_msg}")
        return json.dumps({"status": "failed", "message": "cancelRepair error"})

    @mcp.tool(structured_output=False)
    async def getRepairDetail(contactId: str, srNumber: str) -> str:
        """Query detailed information of a specific repair service request.

        WHEN TO CALL: ONLY call this tool when the customer explicitly requests to VIEW or CHECK the details of a specific repair/service order.
        Trigger phrases include: "show details of my repair", "check status of SR number", "what is the detail of my service request", etc.

        IMPORTANT: Call checkCustomer BEFORE asking the customer anything.

        ═══════════════════════════════════════════════════════════════════════════════════
        FIELD COLLECTION FLOW
        ═══════════════════════════════════════════════════════════════════════════════════

        Collection priority:
        1. Repair order ID (srNumber) — REQUIRED — which order to retrieve details for?

        Confirmation: NO — Attempt the tool call immediately with best interpretation after collecting the REQUIRED parameter (no confirmation summary needed).

        ═══════════════════════════════════════════════════════════════════════════════════
        PARAMETER MAPPING (Prompt → Tool)
        ═══════════════════════════════════════════════════════════════════════════════════

        USER-PROVIDED INPUTS (from customer via Prompt):
            srNumber (str, required)
                Mapping: Repair order ID
                Customer-provided service request number to get details for.

        AUTO-POPULATED FROM SESSION (NOT collected from customer):
            contactId (str, required)
                Source: system-variables ({{$.contactId}})
                Used to look up verified customer information in session.

            countryCode, phoneNumber, customerName (NOT parameters)
                Source: Retrieved from session using contactId
                Used when calling API, not exposed as function parameters.

        Returns:
            JSON string with one of the following statuses:
            - "success": Successfully retrieved repair details, includes full repair information
            - "failed": Query failed, includes reason for failure

        POST-CALL: After a SUCCESSFUL tool call, append the questionFinish placeholder ({{$.Custom.questionFinish}}) at the end of your customer-facing message, separated by one blank line. Do NOT append it if the call failed or returned an error. Appending the questionFinish placeholder counts as your single question for that turn.
        """
        log.info(f"getRepairDetail: contactId='{contactId}', srNumber='{srNumber}'")

        session = session_client.get(contactId)
        if not session:
            return json.dumps({"status": "failed", "message": "customer_not_found"})

        srNumber = srNumber.strip()

        api_result = await call_iservice(
            "/api/iservice-ccm/bot/api/serviceReques/detail",
            {
                "bu": session.get("bu"),
                "countryCode": session.get("countryCode"),
                "phoneNumber": session.get("phoneNumber"),
                "customerName": session.get("customerName"),
                "srNumber": srNumber,
            },
        )

        if api_result.get("resultCode") == "ISC-000":
            api_data = api_result.get("data", {})

            # 判断是否查到单据：srNumber 不为空表示查到了
            if api_data and api_data.get("srNumber"):
                return json.dumps(
                    {
                        "status": "success",
                        "message": "Repair details retrieved successfully",
                        "detail": api_data,
                    },
                    ensure_ascii=False,
                )
            else:
                return json.dumps(
                    {
                        "status": "failed",
                        "message": "No repair record found with the provided information",
                    }
                )

        log.info(
            f"getRepairDetail error: {api_result.get('resultMsg') or 'Unknown error'}"
        )
        return json.dumps({"status": "failed", "message": "getRepairDetail error"})
