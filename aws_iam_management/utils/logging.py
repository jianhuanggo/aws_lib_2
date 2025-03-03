"""
IAM Logging Utilities Module

This module provides utilities for logging IAM operations.
It helps implement comprehensive logging for IAM role and policy management.
"""

import logging
import json
import datetime
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError

# Configure the logger
logger = logging.getLogger(__name__)

class LoggingUtils:
    """
    A class for logging IAM operations.
    
    This class provides methods for logging IAM role and policy changes,
    and for retrieving audit trails.
    """
    
    def __init__(self, session: Optional[boto3.Session] = None):
        """
        Initialize the LoggingUtils with an optional boto3 session.
        
        Args:
            session: An optional boto3 Session object. If not provided, a new session will be created.
        """
        self.session = session or boto3.Session()
        self.cloudtrail_client = self.session.client('cloudtrail')
        self.logs_client = self.session.client('logs')
        
        # Create a log group for IAM operations if it doesn't exist
        self.log_group_name = '/aws/iam-management'
        self._ensure_log_group_exists()
        
    def _ensure_log_group_exists(self) -> None:
        """
        Ensure that the log group for IAM operations exists.
        
        If the log group doesn't exist, it will be created.
        
        Raises:
            ClientError: If the log group cannot be created.
        """
        try:
            self.logs_client.create_log_group(logGroupName=self.log_group_name)
            logger.info(f"Created log group: {self.log_group_name}")
        except ClientError as e:
            # If the log group already exists, that's fine
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                logger.error(f"Error creating log group: {e}")
                raise
        
    def log_role_change(
        self, 
        action: str, 
        role_name: str, 
        user: str,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a change to an IAM role.
        
        Args:
            action: The action performed on the role (e.g., 'create', 'update', 'delete').
            role_name: The name of the role that was changed.
            user: The user or service that performed the change.
            details: Additional details about the change.
            
        Raises:
            ClientError: If the change cannot be logged.
        """
        try:
            # Create a log stream for the role if it doesn't exist
            log_stream_name = f"role/{role_name}"
            self._ensure_log_stream_exists(log_stream_name)
            
            # Create the log event
            timestamp = int(datetime.datetime.now().timestamp() * 1000)
            log_event = {
                'timestamp': timestamp,
                'message': json.dumps({
                    'action': action,
                    'role_name': role_name,
                    'user': user,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'details': details or {}
                })
            }
            
            # Put the log event
            self.logs_client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=log_stream_name,
                logEvents=[log_event]
            )
            
            logger.info(f"Logged {action} action on role {role_name} by {user}")
        except ClientError as e:
            logger.error(f"Error logging role change: {e}")
            # Don't raise the exception, as logging failures shouldn't break the main functionality
            
    def log_policy_change(
        self, 
        action: str, 
        policy_name: str, 
        policy_arn: str,
        user: str,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a change to an IAM policy.
        
        Args:
            action: The action performed on the policy (e.g., 'create', 'update', 'delete').
            policy_name: The name of the policy that was changed.
            policy_arn: The ARN of the policy that was changed.
            user: The user or service that performed the change.
            details: Additional details about the change.
            
        Raises:
            ClientError: If the change cannot be logged.
        """
        try:
            # Create a log stream for the policy if it doesn't exist
            log_stream_name = f"policy/{policy_name}"
            self._ensure_log_stream_exists(log_stream_name)
            
            # Create the log event
            timestamp = int(datetime.datetime.now().timestamp() * 1000)
            log_event = {
                'timestamp': timestamp,
                'message': json.dumps({
                    'action': action,
                    'policy_name': policy_name,
                    'policy_arn': policy_arn,
                    'user': user,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'details': details or {}
                })
            }
            
            # Put the log event
            self.logs_client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=log_stream_name,
                logEvents=[log_event]
            )
            
            logger.info(f"Logged {action} action on policy {policy_name} by {user}")
        except ClientError as e:
            logger.error(f"Error logging policy change: {e}")
            # Don't raise the exception, as logging failures shouldn't break the main functionality
            
    def _ensure_log_stream_exists(self, log_stream_name: str) -> None:
        """
        Ensure that a log stream exists.
        
        If the log stream doesn't exist, it will be created.
        
        Args:
            log_stream_name: The name of the log stream to create.
            
        Raises:
            ClientError: If the log stream cannot be created.
        """
        try:
            self.logs_client.create_log_stream(
                logGroupName=self.log_group_name,
                logStreamName=log_stream_name
            )
            logger.info(f"Created log stream: {log_stream_name}")
        except ClientError as e:
            # If the log stream already exists, that's fine
            if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
                logger.error(f"Error creating log stream: {e}")
                raise
                
    def get_audit_trail(
        self, 
        resource_type: str,
        resource_name: str,
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None
    ) -> Dict[str, Any]:
        """
        Get the audit trail for an IAM resource.
        
        Args:
            resource_type: The type of the resource (e.g., 'role', 'policy').
            resource_name: The name of the resource.
            start_time: The start time for the audit trail. If not provided, defaults to 7 days ago.
            end_time: The end time for the audit trail. If not provided, defaults to now.
            
        Returns:
            A dictionary containing the audit trail events.
            
        Raises:
            ClientError: If the audit trail cannot be retrieved.
        """
        try:
            # Set default start and end times if not provided
            if start_time is None:
                start_time = datetime.datetime.now() - datetime.timedelta(days=7)
            if end_time is None:
                end_time = datetime.datetime.now()
            
            # Get events from CloudTrail
            response = self.cloudtrail_client.lookup_events(
                LookupAttributes=[
                    {
                        'AttributeKey': 'ResourceName',
                        'AttributeValue': resource_name
                    }
                ],
                StartTime=start_time,
                EndTime=end_time
            )
            
            # Filter events by resource type
            events = []
            for event in response.get('Events', []):
                if resource_type.lower() in event.get('Resources', [{}])[0].get('ResourceType', '').lower():
                    events.append(event)
            
            # Get events from CloudWatch Logs
            log_stream_name = f"{resource_type}/{resource_name}"
            try:
                logs_response = self.logs_client.get_log_events(
                    logGroupName=self.log_group_name,
                    logStreamName=log_stream_name,
                    startTime=int(start_time.timestamp() * 1000),
                    endTime=int(end_time.timestamp() * 1000)
                )
                
                # Parse log events
                for log_event in logs_response.get('events', []):
                    try:
                        message = json.loads(log_event['message'])
                        events.append({
                            'EventId': log_event['timestamp'],
                            'EventName': message['action'],
                            'EventTime': datetime.datetime.fromisoformat(message['timestamp']),
                            'Username': message['user'],
                            'Resources': [
                                {
                                    'ResourceName': message[f'{resource_type}_name'],
                                    'ResourceType': resource_type
                                }
                            ],
                            'CloudTrailEvent': json.dumps(message)
                        })
                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Error parsing log event: {e}")
            except ClientError as e:
                # If the log stream doesn't exist, that's fine
                if e.response['Error']['Code'] != 'ResourceNotFoundException':
                    logger.error(f"Error getting log events: {e}")
            
            # Sort events by time
            events.sort(key=lambda x: x['EventTime'], reverse=True)
            
            return {
                'Events': events,
                'ResourceType': resource_type,
                'ResourceName': resource_name,
                'StartTime': start_time.isoformat(),
                'EndTime': end_time.isoformat()
            }
        except ClientError as e:
            logger.error(f"Error getting audit trail: {e}")
            raise
