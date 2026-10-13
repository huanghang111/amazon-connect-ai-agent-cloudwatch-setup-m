"""FAQ Search Tool

This module provides FAQ knowledge base search functionality.
"""

import json
import logging

from util import call_api


log = logging.getLogger("mcp_server")


def register_faq_tools(mcp, session_client, faq_url: str):
    """Register FAQ-related tools to the MCP server.

    Args:
        mcp: FastMCP server instance
        session_client: Session storage client used to persist FAQ contexts
        faq_url: FAQ API URL from environment
    """

    @mcp.tool(structured_output=False)
    async def faqSearch(
        query: str, kb_key: str, productCategory: list[str], contactId: str
    ) -> str:
        """Search the FAQ knowledge base for product-related customer questions.

        Call this tool when the customer's intent does not clearly match any other
        available tool, or for general inquiries such as product usage, troubleshooting,
        warranty, and after-sales policy. When in doubt, prefer this tool.

        IMPORTANT:
        - Call this tool directly without customer identity verification. Do NOT call checkCustomer or setCustomerByPhoneAndName first.
        - FAQ results may be in any language. Silently translate into the customer's language (responseLanguage in system-variables) and present the answer directly. NEVER mention that the result was in another language.

        ═══════════════════════════════════════════════════════════════════════════════════
        PRE-CALL DECISION FLOW (before calling this tool)
        ═══════════════════════════════════════════════════════════════════════════════════

        Step 1 — Brand gate:
        - Customer explicitly names a brand NOT in <supported-scope>.brands → decline, redirect. Do NOT call faqSearch.
        - No brand mentioned → assume in scope, continue.

        Step 2 — Classify intent:
        - Type PRODUCT: about an appliance or its accessories/parts/consumables (map accessory/part to its parent product category).
        - Type GENERAL: policy, warranty, repair process, hours — not tied to a specific product.
        - Unsure Type PRODUCT vs Type GENERAL → treat as product-related, ask ONE clarification question first.

        Step 3 — Resolve product category (for Type PRODUCT):
        - One match in <supported-scope>.productCategories → call faqSearch with that category.
        - Uncertain/multiple matches possible → Ask ONE clarification question to the customer to narrow down the product category. Once the customer provides clarification, do NOT attempt to infer again — use their answer directly to map to the product category. Do NOT search until the category is unambiguous.
        - No match in <supported-scope>.productCategories → decline, redirect. Do NOT call faqSearch.
        - Type GENERAL → call faqSearch with productCategory=[].

        CATEGORY DECISION — anti-stall rule (CRITICAL):
        You get AT MOST ONE short reasoning cycle to pick the product category. Do NOT loop, re-debate, or go back and forth between candidate categories. Deciding the category must NEVER consume the turn without producing an action. Resolve it deterministically as follows:
        - If exactly one category clearly fits → use it and call faqSearch immediately.
        - If TWO OR MORE categories seem plausible and the difference genuinely needs the customer to disambiguate → ask ONE clarification question this turn (as a <message>) and do NOT call the tool. Ask only once; do not deliberate further. Per CLARIFY-XOR-ACT, that clarification question ENDS the turn: you MUST NOT then decide the question is "specific enough" and search anyway in the same turn. Wait for the customer's answer, and on the NEXT turn use that answer to pick the category and search.
        - If you find yourself hesitating between plausible categories but a clarification question would feel trivial/obvious to the customer → do NOT stall: pick the single MOST likely category and call faqSearch immediately. If truly none stands out, call faqSearch with productCategory=[] rather than continuing to deliberate.
        - It is FORBIDDEN to end a turn still undecided about the category with neither a faqSearch call nor a clarification <message>. Undecided-and-silent is the worst outcome and strands the customer.

        ═══════════════════════════════════════════════════════════════════════════════════
        PARAMETER FORMAT
        ═══════════════════════════════════════════════════════════════════════════════════

        productCategory format: JSON array, exact strings from <supported-scope>.productCategories, no extra quotes or whitespace.
        - Correct: ["Washing machine"] | []
        - Wrong:   ["\"Washing machine\""] | [" Washing machine "]

        TWO DISTINCT USES of product categories (never confuse them):
        - TOOL PARAMETER use: When passing productCategory to faqSearch, you MUST use the exact English enumeration strings from <supported-scope>.productCategories, verbatim, unchanged. These English strings are internal identifiers for tool calls ONLY.
        - CUSTOMER-FACING use: Whenever you mention, list, or ask the customer to choose a product category in a <message>, you MUST translate the category name into responseLanguage. NEVER show the raw English category identifiers from <supported-scope> to the customer.
        - The translation is for display only. It does NOT change the value you pass to tools: keep mapping the customer's (possibly translated) answer back to the exact English enumeration string for the tool parameter.

        ═══════════════════════════════════════════════════════════════════════════════════
        ANSWER SELECTION (STRICT — non-negotiable)
        ═══════════════════════════════════════════════════════════════════════════════════

        After receiving results, apply the following response rules (these rules align with escalation counter tracking in the prompt):

        - First, judge RELEVANCE: does any returned entry DIRECTLY answer the customer's actual question? An entry is a match ONLY if its content genuinely addresses what the customer asked. Superficially related, off-topic, or loosely-similar entries are NOT matches.
        - If NO returned entry directly answers the question (empty result, or entries exist but none are relevant), treat this as NO ANSWER FOUND and follow "No-result handling" below. Do NOT ask a clarifying question, do NOT try to salvage an off-topic entry, and do NOT use your own knowledge to construct or guess an answer.
        - If exactly one or more entries DO directly answer: Select EXACTLY ONE FAQ entry: the one with the HIGHEST confidence value. If tie, the first.
        - Output that entry's "answer" field content. If the answer is already in responseLanguage, reproduce it verbatim, line for line. If it is in another language, translate it faithfully into responseLanguage — convey the exact meaning of every line without adding, removing, reordering, or altering information. In that case faithful translation takes precedence over literal line-for-line reproduction; the constraint is fidelity of meaning, not identical characters.
        - FORBIDDEN: merging or quoting any other FAQ entry; paraphrasing, summarizing, reordering, or omitting any line.
        - The ONLY text you may append after the verbatim answer is the questionFinish line required by <final-instructions>, separated by one blank line. Nothing else.
        - Never reference other entries even if they seem relevant. Never combine entries. If no single entry directly answers, do NOT ask a clarifying question — go to "No-result handling".

        Escalation counter alignment:
        - ONE strong semantic match (highest confidence): translate into responseLanguage and reply using that answer. Do NOT expand or paraphrase. This is RESOLVED — do NOT increment faqSearch unresolved counter.
        - MULTIPLE relevant matches but no clear best: ask the customer ONE clarifying question to narrow intent. Do NOT list out the FAQ entries. Do NOT increment counter yet (clarification may resolve it).
        - NO match (empty result) OR returned entries but none address the customer's actual question: This is UNRESOLVED. Do NOT supplement with your own knowledge. Tell the customer you could not find relevant information. Increment faqSearch unresolved counter (tracked in prompt escalation-rules). When counter reaches 3 in a conversation, transfer to human agent immediately.

        ═══════════════════════════════════════════════════════════════════════════════════
        NO-RESULT HANDLING (STRICT)
        ═══════════════════════════════════════════════════════════════════════════════════

        Applies whenever faqSearch returns an empty result OR returns entries but none directly answer the customer's question (per the relevance judgment above).
        - Tell the customer, in responseLanguage, that you could not find an answer to their question. Keep it brief.
        - Do NOT ask a clarifying question or a follow-up question to refine the search. Do NOT re-run faqSearch on your own for the same question.
        - Do NOT guess, infer, paraphrase, or use your own general knowledge to fabricate an answer. Only tool results may be used as factual content.
        - You MUST NOT propose creating a repair order (createRepair) here, or anywhere, on your own initiative. A no-result FAQ outcome is NEVER a reason to suggest a repair order. See the createRepair invocation trigger in <tool-instructions>.
        - After stating that no answer was found, you may follow the escalation-rules (e.g. the faqSearch-unresolved counter). When that threshold is reached, offer to connect the customer to a human agent.

        Args:
            query: Natural language question or search query from the customer.
            kb_key: Knowledge base key identifying the brand/BU. Use the KB_KEY value from customer_info.
            productCategory: Identify ALL product categories mentioned or implied in the customer's question. Values must come from productCategories in <supported-scope>. Multiple categories are allowed. Pass an empty list if none can be identified.
            contactId: Contact ID used to store and retrieve FAQ search contexts.

        Returns:
            JSON string containing a list of matched FAQ entries with question, answer, answerId, and confidence fields.
            Results may be in any language and should be translated to the customer's language before presenting.
            Returns an empty list if no match is found.

        POST-CALL: After a SUCCESSFUL tool call, append the questionFinish placeholder ({{$.Custom.questionFinish}}) at the end of your customer-facing message, separated by one blank line. Do NOT append it if the call failed or returned an error. Appending the questionFinish placeholder counts as your single question for that turn.
        """

        query = (query or "").strip()
        kb_key = (kb_key or "").strip()
        contactId = (contactId or "").strip()
        categories = list(dict.fromkeys(productCategory or []))
        if categories and "Common" not in categories:
            categories.append("Common")

        api_result = await call_api(
            faq_url,
            {
                "query": query,
                "kb_key": kb_key,
                "product_categorys": ",".join(categories),
                "model_key": "gpt5.6-luna",
                "mode": "search",
                "top_k": 5,
            },
        )

        retrieved_contexts = api_result.get("retrieved_contexts", [])

        if contactId:
            session_client.update(
                contactId,
                {
                    "last_faq_query": query,
                    "last_faq_retrieved_contexts": json.dumps(
                        retrieved_contexts, ensure_ascii=False
                    ),
                },
            )
        result = []
        if retrieved_contexts:
            result = [
                {
                    "question": item.get("question"),
                    "answer": item.get("answer"),
                    "answer_id": item.get("id"),
                    "confidence": item.get("similarity"),
                }
                for item in retrieved_contexts
            ]

        result = json.dumps(result, ensure_ascii=False)

        return result
