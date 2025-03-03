"""
Unit tests for the LoggingUtils class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import datetime
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.utils.logging import LoggingUtils

class TestLoggingUtils(unittest.TestCase):
    """
    Test cases for the LoggingUtils class.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a mock session and clients
        self.mock_session = MagicMock(spec=boto3.Session)
        self.mock_cloudtrail_client = MagicMock()
        self.mock_logs_client = MagicMock()
        
        # Configure the mock session to return our mock clients
        self.mock_session.client.side_effect = lambda service, **kwargs: {
            'cloudtrail': self.mock_cloudtrail_client,
            'logs': self.mock_logs_client
        }.get(service, MagicMock())
        
        # Create a LoggingUtils instance with the mock session
        self.logging = LoggingUtils(session=self.mock_session)
        
        # Define test data
        self.test_role_name = "test-role"
        self.test_policy_name = "test-policy"
        self.test_policy_arn = "arn:aws:iam::123456789012:policy/test-policy"
        self.test_user = "test-user"
        self.test_details = {
            "action": "create",
            "resource": "role",
            "timestamp": "2023-01-01T00:00:00Z"
        }
        
    def test_ensure_log_group_exists(self):
        """
        Test ensuring that the log group exists.
        """
        # Mock the response from the CloudWatch Logs client
        self.mock_logs_client.create_log_group.return_value = {}
        
        # Call the method under test
        self.logging._ensure_log_group_exists()
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.create_log_group.assert_called_once_with(
            logGroupName='/aws/iam-management'
        )
        
    def test_ensure_log_group_exists_already_exists(self):
        """
        Test ensuring that the log group exists when it already exists.
        """
        # Mock the CloudWatch Logs client to raise a ResourceAlreadyExistsException
        self.mock_logs_client.create_log_group.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ResourceAlreadyExistsException",
                    "Message": "The specified log group already exists"
                }
            },
            "CreateLogGroup"
        )
        
        # Call the method under test
        self.logging._ensure_log_group_exists()
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.create_log_group.assert_called_once_with(
            logGroupName='/aws/iam-management'
        )
        
    def test_ensure_log_stream_exists(self):
        """
        Test ensuring that the log stream exists.
        """
        # Mock the response from the CloudWatch Logs client
        self.mock_logs_client.create_log_stream.return_value = {}
        
        # Call the method under test
        self.logging._ensure_log_stream_exists("test-stream")
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.create_log_stream.assert_called_once_with(
            logGroupName='/aws/iam-management',
            logStreamName='test-stream'
        )
        
    def test_ensure_log_stream_exists_already_exists(self):
        """
        Test ensuring that the log stream exists when it already exists.
        """
        # Mock the CloudWatch Logs client to raise a ResourceAlreadyExistsException
        self.mock_logs_client.create_log_stream.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ResourceAlreadyExistsException",
                    "Message": "The specified log stream already exists"
                }
            },
            "CreateLogStream"
        )
        
        # Call the method under test
        self.logging._ensure_log_stream_exists("test-stream")
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.create_log_stream.assert_called_once_with(
            logGroupName='/aws/iam-management',
            logStreamName='test-stream'
        )
        
    def test_log_role_change(self):
        """
        Test logging a role change.
        """
        # Mock the responses from the CloudWatch Logs client
        self.mock_logs_client.create_log_stream.return_value = {}
        self.mock_logs_client.put_log_events.return_value = {
            "nextSequenceToken": "49590339868259779544652148414600212882886503775294562818"
        }
        
        # Call the method under test
        self.logging.log_role_change(
            action="create",
            role_name=self.test_role_name,
            user=self.test_user,
            details=self.test_details
        )
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.create_log_stream.assert_called_once()
        self.mock_logs_client.put_log_events.assert_called_once()
        
    def test_log_policy_change(self):
        """
        Test logging a policy change.
        """
        # Mock the responses from the CloudWatch Logs client
        self.mock_logs_client.create_log_stream.return_value = {}
        self.mock_logs_client.put_log_events.return_value = {
            "nextSequenceToken": "49590339868259779544652148414600212882886503775294562818"
        }
        
        # Call the method under test
        self.logging.log_policy_change(
            action="create",
            policy_name=self.test_policy_name,
            policy_arn=self.test_policy_arn,
            user=self.test_user,
            details=self.test_details
        )
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.create_log_stream.assert_called_once()
        self.mock_logs_client.put_log_events.assert_called_once()
        
    def test_get_audit_trail(self):
        """
        Test getting the audit trail for a resource.
        """
        # Mock the responses from the CloudTrail client
        self.mock_cloudtrail_client.lookup_events.return_value = {
            "Events": [
                {
                    "EventId": "abcdef01-2345-6789-abcd-ef0123456789",
                    "EventName": "CreateRole",
                    "EventTime": datetime.datetime(2023, 1, 1, 0, 0, 0),
                    "Username": self.test_user,
                    "Resources": [
                        {
                            "ResourceName": self.test_role_name,
                            "ResourceType": "AWS::IAM::Role"
                        }
                    ],
                    "CloudTrailEvent": json.dumps({
                        "eventVersion": "1.08",
                        "eventTime": "2023-01-01T00:00:00Z",
                        "eventSource": "iam.amazonaws.com",
                        "eventName": "CreateRole",
                        "awsRegion": "us-east-1",
                        "sourceIPAddress": "192.0.2.1",
                        "userAgent": "aws-cli/2.0.0 Python/3.8.0 Linux/4.14.0-0.bpo.1-amd64 botocore/2.0.0",
                        "requestParameters": {
                            "roleName": self.test_role_name,
                            "assumeRolePolicyDocument": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}"
                        },
                        "responseElements": {
                            "role": {
                                "roleName": self.test_role_name,
                                "roleId": "ABCDEFGHIJKLMNOPQRSTU",
                                "arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
                                "createDate": "2023-01-01T00:00:00Z",
                                "assumeRolePolicyDocument": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}"
                            }
                        }
                    })
                }
            ]
        }
        
        # Mock the responses from the CloudWatch Logs client
        self.mock_logs_client.get_log_events.return_value = {
            "events": [
                {
                    "timestamp": 1672531200000,  # 2023-01-01T00:00:00Z
                    "message": json.dumps({
                        "action": "create",
                        "role_name": self.test_role_name,
                        "user": self.test_user,
                        "timestamp": "2023-01-01T00:00:00Z",
                        "details": self.test_details
                    }),
                    "ingestionTime": 1672531200000
                }
            ],
            "nextForwardToken": "f/00000000000000000000000000000000000000000000000000000001",
            "nextBackwardToken": "b/00000000000000000000000000000000000000000000000000000001"
        }
        
        # Call the method under test
        result = self.logging.get_audit_trail(
            resource_type="role",
            resource_name=self.test_role_name,
            start_time=datetime.datetime(2023, 1, 1, 0, 0, 0),
            end_time=datetime.datetime(2023, 1, 2, 0, 0, 0)
        )
        
        # Verify the result
        self.assertEqual(len(result["Events"]), 2)
        self.assertEqual(result["ResourceType"], "role")
        self.assertEqual(result["ResourceName"], self.test_role_name)
        
        # Verify that the CloudTrail client was called with the correct parameters
        self.mock_cloudtrail_client.lookup_events.assert_called_once()
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.get_log_events.assert_called_once()
        
    def test_get_audit_trail_no_log_stream(self):
        """
        Test getting the audit trail for a resource when the log stream doesn't exist.
        """
        # Mock the responses from the CloudTrail client
        self.mock_cloudtrail_client.lookup_events.return_value = {
            "Events": [
                {
                    "EventId": "abcdef01-2345-6789-abcd-ef0123456789",
                    "EventName": "CreateRole",
                    "EventTime": datetime.datetime(2023, 1, 1, 0, 0, 0),
                    "Username": self.test_user,
                    "Resources": [
                        {
                            "ResourceName": self.test_role_name,
                            "ResourceType": "AWS::IAM::Role"
                        }
                    ],
                    "CloudTrailEvent": json.dumps({
                        "eventVersion": "1.08",
                        "eventTime": "2023-01-01T00:00:00Z",
                        "eventSource": "iam.amazonaws.com",
                        "eventName": "CreateRole",
                        "awsRegion": "us-east-1",
                        "sourceIPAddress": "192.0.2.1",
                        "userAgent": "aws-cli/2.0.0 Python/3.8.0 Linux/4.14.0-0.bpo.1-amd64 botocore/2.0.0",
                        "requestParameters": {
                            "roleName": self.test_role_name,
                            "assumeRolePolicyDocument": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}"
                        },
                        "responseElements": {
                            "role": {
                                "roleName": self.test_role_name,
                                "roleId": "ABCDEFGHIJKLMNOPQRSTU",
                                "arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
                                "createDate": "2023-01-01T00:00:00Z",
                                "assumeRolePolicyDocument": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"lambda.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}"
                            }
                        }
                    })
                }
            ]
        }
        
        # Mock the CloudWatch Logs client to raise a ResourceNotFoundException
        self.mock_logs_client.get_log_events.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ResourceNotFoundException",
                    "Message": "The specified log stream does not exist"
                }
            },
            "GetLogEvents"
        )
        
        # Call the method under test
        result = self.logging.get_audit_trail(
            resource_type="role",
            resource_name=self.test_role_name,
            start_time=datetime.datetime(2023, 1, 1, 0, 0, 0),
            end_time=datetime.datetime(2023, 1, 2, 0, 0, 0)
        )
        
        # Verify the result
        self.assertEqual(len(result["Events"]), 1)
        self.assertEqual(result["ResourceType"], "role")
        self.assertEqual(result["ResourceName"], self.test_role_name)
        
        # Verify that the CloudTrail client was called with the correct parameters
        self.mock_cloudtrail_client.lookup_events.assert_called_once()
        
        # Verify that the CloudWatch Logs client was called with the correct parameters
        self.mock_logs_client.get_log_events.assert_called_once()

if __name__ == "__main__":
    unittest.main()
