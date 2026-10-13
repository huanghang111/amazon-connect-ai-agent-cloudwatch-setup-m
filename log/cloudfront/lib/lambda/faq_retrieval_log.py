"""
FAQ Retrieval Log Lambda

Scheduled Lambda that reads instance configurations from env files,
queries CloudWatch logs for FAQ events, and uploads hit/miss analysis results to external service.

Trigger: EventBridge scheduled rule (e.g., every 1 hour)

Configuration:
- Reads config files from ~/env/ directory (e.g., uat-meg.env, prod-mis.env)
- Queries CloudWatch logs for FAQ search events
- Analyzes if LLM response matched the FAQ answer using LLM
- Uploads results to external FAQ retrieval log service via FAQ_RETRIEVAL_LOG_URL

Environment variables:
- ENV_DIR: Path to env files directory (default: ./env)
- LOG_GROUP_NAME: CloudWatch log group to query
- LLM_LAMBDA_NAME: LLM Lambda function name
- AWS_REGION: AWS region (default: us-east-1)
"""

import json
import logging
import os
import re
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
logs_client = boto3.client("logs", config=Config(retries={"max_attempts": 3}))
lambda_client = boto3.client(
    "lambda",
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    config=Config(retries={"max_attempts": 2}),
)

# Environment
ENV_DIR = os.environ.get("ENV_DIR", "./env")
LOG_GROUP_NAME = os.environ.get("LOG_GROUP_NAME", "/aws/lambda/bedrock-agent")
LLM_LAMBDA_NAME = os.environ.get("LLM_LAMBDA_NAME", "llm_lambda")
LLM_MODEL = os.environ.get("LLM_MODEL", "claude-3-5-sonnet")


def lambda_handler(event, context):
    """
    Main handler for scheduled FAQ analysis Lambda.

    Workflow:
    1. Load all instance configurations from env files
    2. For each instance, query CloudWatch logs for recent FAQ events
    3. Parse and analyze FAQ search results
    4. Use LLM to judge if response matched the FAQ answer
    5. Upload results to external FAQ retrieval log service

    Args:
        event: EventBridge scheduled event
        context: Lambda context

    Returns:
        {"statusCode": 200|500, "body": JSON summary}
    """
    try:
        logger.info(f"Starting FAQ analysis. Event: {json.dumps(event)}")

        # Load configurations from env files
        configs = _load_env_configs()
        if not configs:
            logger.warning("No configurations found in env directory")
            return success_response(
                {
                    "message": "No configurations loaded",
                    "processed": 0,
                }
            )

        logger.info(f"Loaded {len(configs)} instance configurations")

        # Process each instance
        total_processed = 0
        total_success = 0
        total_errors = 0

        for instance_name, config in configs.items():
            try:
                result = _process_instance_logs(instance_name, config)
                total_processed += result.get("processed", 0)
                total_success += result.get("success", 0)
                total_errors += result.get("errors", 0)
            except Exception as e:
                logger.error(f"Error processing instance {instance_name}: {e}")
                total_errors += 1

        return success_response(
            {
                "message": "FAQ analysis completed",
                "instances_processed": len(configs),
                "total_records": total_processed,
                "successful_analyses": total_success,
                "errors": total_errors,
            }
        )

    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}", exc_info=True)
        return error_response(500, f"Internal error: {str(e)}")


def _load_env_configs() -> dict:
    """Load and parse all instance configuration files from env directory."""
    configs = {}

    try:
        env_path = Path(ENV_DIR)
        if not env_path.exists():
            logger.warning(f"ENV_DIR not found: {ENV_DIR}")
            return {}

        # Find all .env files
        for env_file in env_path.glob("*.env"):
            instance_name = env_file.stem  # e.g., 'uat-meg' from 'uat-meg.env'

            try:
                config = _parse_env_file(env_file)
                if config:
                    configs[instance_name] = config
                    logger.info(f"Loaded config for instance: {instance_name}")
            except Exception as e:
                logger.error(f"Error parsing {env_file}: {e}")

        return configs

    except Exception as e:
        logger.error(f"Error loading env configs: {e}")
        return {}


def _parse_env_file(env_file: Path) -> dict:
    """Parse a .env file into a configuration dictionary."""
    config = {
        "instance_name": env_file.stem,
        "log_group": LOG_GROUP_NAME,
        "kb_key": "",
        "bu": "",
        "faq_retrieval_log_url": "",
        "faq_retrieval_log_api_key": "",
    }

    try:
        content = env_file.read_text(encoding="utf-8")

        # Parse KEY=VALUE lines
        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip().upper()
                value = value.strip().strip("\"'")

                # Extract relevant config
                if key == "KB_KEY":
                    config["kb_key"] = value
                elif key == "BU":
                    config["bu"] = value
                elif key == "INSTANCE_NAME":
                    config["instance_name"] = value
                elif key == "FAQ_RETRIEVAL_LOG_URL":
                    config["faq_retrieval_log_url"] = value
                elif key == "FAQ_RETRIEVAL_LOG_API_KEY":
                    config["faq_retrieval_log_api_key"] = value

        return config

    except Exception as e:
        logger.error(f"Error parsing env file {env_file}: {e}")
        return None


def _process_instance_logs(instance_name: str, config: dict) -> dict:
    """Process CloudWatch logs for a specific instance."""
    result = {
        "instance_name": instance_name,
        "processed": 0,
        "success": 0,
        "errors": 0,
    }

    try:
        # Query logs for this instance
        logs = _query_faq_logs(instance_name, hours=1)  # Last 1 hour
        if not logs:
            logger.info(f"No logs found for instance {instance_name}")
            return result

        logger.info(f"Found {len(logs)} log entries for instance {instance_name}")

        # Process each log entry
        for log_entry in logs:
            try:
                faq_event = _extract_faq_event_from_log(log_entry)
                if not faq_event or not faq_event.get("question"):
                    continue

                # Judge with LLM
                judgment = _judge_with_llm(
                    question=faq_event["question"],
                    faq_answer=faq_event.get("answer", ""),
                    llm_response=faq_event.get("llm_response", ""),
                )

                # Store result
                _store_faq_log_record(
                    instance_name=instance_name,
                    config=config,
                    faq_event=faq_event,
                    judgment=judgment,
                )

                result["processed"] += 1
                result["success"] += 1

            except Exception as e:
                logger.error(f"Error processing log entry: {e}")
                result["errors"] += 1

        logger.info(
            f"Instance {instance_name}: processed={result['processed']}, "
            f"success={result['success']}, errors={result['errors']}"
        )

        return result

    except Exception as e:
        logger.error(f"Error processing instance logs for {instance_name}: {e}")
        result["errors"] += 1
        return result


def _query_faq_logs(instance_name: str, hours: int = 1) -> list:
    """Query CloudWatch logs for FAQ events from a specific instance."""
    try:
        # CloudWatch Insights query to find faqSearch events
        query = f"""
        fields @timestamp, @message, @logStream
        | filter @message like /faqSearch|logFaqRetrieval/
        | filter @message like /{re.escape(instance_name)}/
        | stats count() by @logStream
        """

        response = logs_client.start_query(
            logGroupName=LOG_GROUP_NAME,
            startTime=int((datetime.utcnow() - timedelta(hours=hours)).timestamp()),
            endTime=int(datetime.utcnow().timestamp()),
            queryString=query,
        )

        query_id = response["queryId"]

        # Poll for results
        max_attempts = 30
        for _ in range(max_attempts):
            result = logs_client.get_query_results(queryId=query_id)
            if result["status"] == "Complete":
                return _parse_query_results(result.get("results", []))
            elif result["status"] in ("Failed", "Cancelled"):
                logger.warning(f"CloudWatch query failed: {result['status']}")
                return []

            time.sleep(0.5)

        logger.warning("CloudWatch query timed out")
        return []

    except Exception as e:
        logger.error(f"Error querying CloudWatch logs: {e}")
        return []


def _parse_query_results(results: list) -> list:
    """Parse CloudWatch Insights query results."""
    logs = []
    for entry in results:
        message = ""
        for field in entry.get("field", []):
            if field["field"] == "@message":
                message = field["value"]
                break
        if message:
            logs.append(message)
    return logs


def _extract_faq_event_from_log(log_message: str) -> Optional[dict]:
    """Extract FAQ event details from a log message."""
    faq_event = {
        "question": "",
        "hit": False,
        "answer": "",
        "answer_id": "",
        "retrieved_contexts": "",
        "llm_response": "",
    }

    try:
        # Try to parse as JSON first
        if log_message.startswith("{"):
            try:
                data = json.loads(log_message)
                faq_event.update(
                    {
                        "question": data.get("question", ""),
                        "hit": data.get("is_hit", False),
                        "answer": data.get("answer", ""),
                        "answer_id": data.get("answer_id", ""),
                        "retrieved_contexts": data.get("retrieved_contexts", ""),
                        "llm_response": data.get("llm_response", ""),
                    }
                )
                return faq_event if faq_event["question"] else None
            except json.JSONDecodeError:
                pass

        # Fallback: extract using regex
        question_match = re.search(r'"question":\s*"([^"]*)"', log_message)
        if question_match:
            faq_event["question"] = question_match.group(1)

        hit_match = re.search(r'"is_hit":\s*(true|false)', log_message, re.IGNORECASE)
        if hit_match:
            faq_event["hit"] = hit_match.group(1).lower() == "true"

        answer_match = re.search(r'"answer":\s*"([^"]*)"', log_message)
        if answer_match:
            faq_event["answer"] = answer_match.group(1)

        answer_id_match = re.search(r'"answer_id":\s*"([^"]*)"', log_message)
        if answer_id_match:
            faq_event["answer_id"] = answer_id_match.group(1)

        return faq_event if faq_event["question"] else None

    except Exception as e:
        logger.warning(f"Error extracting FAQ event: {e}")
        return None


def _judge_with_llm(question: str, faq_answer: str, llm_response: str) -> bool:
    """Use LLM to judge if response addressed the question using FAQ answer."""
    if not all([question, faq_answer, llm_response]):
        logger.warning("Missing data for LLM judgment")
        return False

    try:
        prompt = f"""Analyze whether the LLM's response adequately addressed the user's question using the provided FAQ answer.

USER QUESTION:
{question}

FAQ ANSWER PROVIDED:
{faq_answer}

LLM RESPONSE TO USER:
{llm_response}

TASK:
1. Did the LLM use the FAQ answer to respond to the user?
2. Did the response adequately address the user's question?

Respond with ONLY "YES" or "NO" (one word)."""

        payload = {
            "model_name": LLM_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 10,
            "temperature": 0.3,
        }

        response = lambda_client.invoke(
            FunctionName=LLM_LAMBDA_NAME,
            Payload=json.dumps(payload),
        )

        result = json.loads(response["Payload"].read())

        # Extract response text
        body = result.get("body", {})
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                logger.error(f"LLM Lambda body is not valid JSON")
                return False

        response_text = body.get("response_text", "").strip().upper()
        matched = response_text == "YES"
        logger.info(f"LLM judgment: {matched}")
        return matched

     except Exception as e:
         logger.error(f"Error invoking LLM Lambda: {e}")
         return False


def _upload_faq_retrieval_log(config: dict, log_record: dict) -> bool:
     """Upload FAQ retrieval log record to external FAQ retrieval log service.
     
     Args:
         config: Instance configuration with FAQ_RETRIEVAL_LOG_URL and FAQ_RETRIEVAL_LOG_API_KEY
         log_record: FAQ log record to upload
     
     Returns:
         True if upload successful, False otherwise
     """
     try:
         faq_log_url = config.get("faq_retrieval_log_url", "")
         faq_log_api_key = config.get("faq_retrieval_log_api_key", "")
         
         if not faq_log_url or not faq_log_api_key:
             logger.warning(
                 f"FAQ retrieval log URL or API key not configured for instance {config.get('instance_name')}"
             )
             return False
         
         # Prepare headers with API key authorization
         headers = {
             "Content-Type": "application/json",
             "Authorization": f"Bearer {faq_log_api_key}",
             "X-API-Key": faq_log_api_key,
         }
         
         # Send POST request to FAQ retrieval log service
         response = requests.post(
             faq_log_url,
             json=log_record,
             headers=headers,
             timeout=10,
         )
         
         if response.status_code == 200:
             logger.info(
                 f"FAQ retrieval log uploaded successfully for instance {config.get('instance_name')}"
             )
             return True
         elif response.status_code == 201:
             logger.info(
                 f"FAQ retrieval log created successfully for instance {config.get('instance_name')}"
             )
             return True
         else:
             logger.error(
                 f"Failed to upload FAQ retrieval log. Status: {response.status_code}, "
                 f"Response: {response.text}"
             )
             return False
         
     except requests.exceptions.Timeout:
         logger.error(f"Timeout uploading FAQ retrieval log to {config.get('faq_retrieval_log_url')}")
         return False
     except requests.exceptions.ConnectionError:
         logger.error(f"Connection error uploading FAQ retrieval log to {config.get('faq_retrieval_log_url')}")
         return False
     except Exception as e:
         logger.error(f"Error uploading FAQ retrieval log: {e}")
         return False


def _store_faq_log_record(
    instance_name: str, config: dict, faq_event: dict, judgment: bool
):
    """Upload FAQ log record to external FAQ retrieval log service."""
    try:
        log_record = {
            "instance_name": instance_name,
            "kb_key": config.get("kb_key", ""),
            "bu": config.get("bu", ""),
            "question": faq_event.get("question", ""),
            "faq_hit": faq_event.get("hit", False),
            "answer_id": faq_event.get("answer_id", ""),
            "answer": faq_event.get("answer", ""),
            "retrieved_contexts": faq_event.get("retrieved_contexts", ""),
            "llm_response": faq_event.get("llm_response", ""),
            "matched_user_expectation": judgment,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Upload to external FAQ retrieval log service
        _upload_faq_retrieval_log(config, log_record)

    except Exception as e:
        logger.error(f"Error uploading FAQ log: {e}")


def success_response(body):
    """Return a 200 success response."""
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, ensure_ascii=False),
    }


def error_response(status_code, message):
    """Return an error response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": message}, ensure_ascii=False),
    }
