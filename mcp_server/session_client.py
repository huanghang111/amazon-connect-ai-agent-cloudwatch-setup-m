"""Session storage client using DynamoDB.

This module provides a simple interface for session data operations
used by the MCP server, with contactId as the primary key.
"""

import logging
import os
import time
from typing import Optional

import boto3
from botocore.exceptions import ClientError


log = logging.getLogger("mcp_server")


class SessionClient:
    """DynamoDB-backed session storage client.
    
    Provides methods to get, put, and delete session data in DynamoDB.
    Uses contactId as the primary key.
    """
    
    def __init__(self, region: str):
        """Initialize session client.
        
        Args:
            region: AWS region (default: from REGION env var)
        """
        
        self._dynamodb = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamodb.Table("connect-ai-agent-store")
        
        log.info(f"SessionClient initialized: region={region}")
    
    def get(self, contact_id: str) -> Optional[dict]:
        """Get entire session item by contactId.
        
        Args:
            contact_id: The contact ID (primary key)
        
        Returns:
            Dictionary containing all attributes, or None if not found
        
        Raises:
            ClientError: If the DynamoDB operation fails (e.g. table not found, access denied)
        """
        try:
            resp = self._table.get_item(Key={"contactId": contact_id})
            item = resp.get("Item")
            return item
        
        except ClientError as e:
            log.error(f"session get_item error: contactId='{contact_id}', error={e}")
            raise
        except Exception as e:
            log.exception(f"Unexpected error in session get_item: contactId='{contact_id}'")
            raise
    
    def update(self, contact_id: str, attributes: dict) -> bool:
        """Update multiple attributes in session item.
        
        Args:
            contact_id: The contact ID (primary key)
            attributes: Dictionary of attributes to update
        
        Returns:
            True if successful, False otherwise
        
        Raises:
            ClientError: If the DynamoDB operation fails (e.g. table not found, access denied)
        """
        try:
            expire_time = int(time.time()) + 86400  # 当前时间 + 24小时
            update_expr = "SET " + ", ".join(f"#k{i} = :v{i}" for i in range(len(attributes))) + ", #ttl = :ttl"
            attr_names  = {f"#k{i}": k for i, k in enumerate(attributes)} | {"#ttl": "expire_time"}
            attr_values = {f":v{i}": v for i, v in enumerate(attributes.values())} | {":ttl": expire_time}
            self._table.update_item(
                Key={"contactId": contact_id},
                UpdateExpression=update_expr,
                ExpressionAttributeNames=attr_names,
                ExpressionAttributeValues=attr_values,
            )
            return True
        
        except ClientError as e:
            log.error(f"session update error: contactId='{contact_id}', error={e}")
            raise
        except Exception as e:
            log.exception(f"Unexpected error in session update: contactId='{contact_id}'")
            raise

    def put(self, contact_id: str, attributes: dict) -> bool:
        """Put or replace entire session item.
        
        Args:
            contact_id: The contact ID (primary key)
            attributes: Dictionary of attributes to store
        
        Returns:
            True if successful, False otherwise
        
        Raises:
            ClientError: If the DynamoDB operation fails (e.g. table not found, access denied)
        """
        try:
            expire_time = int(time.time()) + 86400  # 当前时间 + 24小时
            item = {"contactId": contact_id, "expire_time": expire_time, **attributes}
            self._table.put_item(Item=item)
            return True
        
        except ClientError as e:
            log.error(f"session put_item error: contactId='{contact_id}', error={e}")
            raise
        except Exception as e:
            log.exception(f"Unexpected error in session put_item: contactId='{contact_id}'")
            raise
    
    def remove_keys(self, contact_id: str, keys: list[str]) -> bool:
        """Delete multiple attributes from session item.
        
        Args:
            contact_id: The contact ID (primary key)
            keys: List of attribute keys to delete
        
        Returns:
            True if successful, False otherwise
        
        Raises:
            ClientError: If the DynamoDB operation fails (e.g. table not found, access denied)
        """
        try:
            remove_expr = "REMOVE " + ", ".join(f"#k{i}" for i in range(len(keys)))
            attr_names  = {f"#k{i}": k for i, k in enumerate(keys)}
            self._table.update_item(
                Key={"contactId": contact_id},
                UpdateExpression=remove_expr,
                ExpressionAttributeNames=attr_names,
            )
            return True
        
        except ClientError as e:
            log.error(f"session remove_keys error: contactId='{contact_id}', keys={keys}, error={e}")
            raise
        except Exception as e:
            log.exception(f"Unexpected error in session remove_keys: contactId='{contact_id}', keys={keys}")
            raise
    
    def delete(self, contact_id: str) -> bool:
        """Delete entire session item.
        
        Args:
            contact_id: The contact ID (primary key)
        
        Returns:
            True if successful, False otherwise
        
        Raises:
            ClientError: If the DynamoDB operation fails (e.g. table not found, access denied)
        """
        try:
            self._table.delete_item(Key={"contactId": contact_id})
            return True
        
        except ClientError as e:
            log.error(f"session delete_item error: contactId='{contact_id}', error={e}")
            raise
        except Exception as e:
            log.exception(f"Unexpected error in session delete_item: contactId='{contact_id}'")
            raise
