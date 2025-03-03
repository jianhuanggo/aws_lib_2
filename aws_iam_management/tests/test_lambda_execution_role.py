"""
Unit tests for the Lambda execution role example.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.examples.lambda_execution_role_example import (
    create_lambda_execution_role,
    create_lambda_execution_role_with_templates
)

class TestLambdaExecutionRole(unittest.TestCase):
    """
    Test cases for the Lambda execution role example.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a mock session
        self.mock_session = MagicMock(spec=boto3.Session)
        
        # Create mock clients
        self.mock_iam_client = MagicMock()
        self.mock_logs_client = MagicMock()
        
        # Configure the mock session to return our mock clients
        self.mock_session.client.side_effect = lambda service, **kwargs: {
            'iam': self.mock_iam_client,
            'logs': self.mock_logs_client
        }.get(service, MagicMock())
        
        # Define test data
        self.test_role_name = "test-lambda-role"
        self.test_s3_buckets = ["test-bucket-1", "test-bucket-2"]
        self.test_dynamodb_tables = ["test-table"]
        
    @patch('aws_iam_management.examples.lambda_execution_role_example.RoleManager')
    @patch('aws_iam_management.examples.lambda_execution_role_example.PolicyManager')
    @patch('aws_iam_management.examples.lambda_execution_role_example.ValidationUtils')
    @patch('aws_iam_management.examples.lambda_execution_role_example.TaggingUtils')
    @patch('aws_iam_management.examples.lambda_execution_role_example.LoggingUtils')
    def test_create_lambda_execution_role(
        self,
        mock_logging_utils_class,
        mock_tagging_utils_class,
        mock_validation_utils_class,
        mock_policy_manager_class,
        mock_role_manager_class
    ):
        """
        Test creating a Lambda execution role.
        """
        # Create mock instances
        mock_role_manager = MagicMock()
        mock_policy_manager = MagicMock()
        mock_validation = MagicMock()
        mock_tagging = MagicMock()
        mock_logging = MagicMock()
        
        # Configure the mock classes to return our mock instances
        mock_role_manager_class.return_value = mock_role_manager
        mock_policy_manager_class.return_value = mock_policy_manager
        mock_validation_utils_class.return_value = mock_validation
        mock_tagging_utils_class.return_value = mock_tagging
        mock_logging_utils_class.return_value = mock_logging
        
        # Configure the mock validation to return no findings
        mock_validation.validate_trust_relationship.return_value = []
        mock_validation.validate_policy_least_privilege.return_value = []
        
        # Configure the mock role manager to return a role
        mock_role_manager.create_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": f"Lambda execution role for lambda-app in production",
            "AssumeRolePolicyDocument": json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }),
            "MaxSessionDuration": 3600,
            "Path": "/service-role/",
            "Tags": [
                {
                    "Key": "Environment",
                    "Value": "production"
                },
                {
                    "Key": "Application",
                    "Value": "lambda-app"
                },
                {
                    "Key": "Owner",
                    "Value": "platform-team"
                },
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "LambdaExecution"
                }
            ]
        }
        
        # Configure the mock role manager to return success for put_role_policy
        mock_role_manager.put_role_policy.return_value = True
        
        # Call the method under test
        result = create_lambda_execution_role(
            role_name=self.test_role_name,
            s3_buckets=self.test_s3_buckets,
            dynamodb_tables=self.test_dynamodb_tables,
            vpc_access=True,
            xray_tracing=True,
            session=self.mock_session
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], "Lambda execution role for lambda-app in production")
        
        # Verify that the role manager was called with the correct parameters
        mock_role_manager.create_role.assert_called_once()
        
        # Verify that put_role_policy was called multiple times
        # 1 for CloudWatch Logs, 2 for S3 buckets, 1 for DynamoDB, 1 for VPC, 1 for X-Ray
        self.assertEqual(mock_role_manager.put_role_policy.call_count, 6)
        
        # Verify that the logging utils were called
        mock_logging.log_role_change.assert_called_once()
        
    @patch('aws_iam_management.examples.lambda_execution_role_example.RoleTemplates')
    @patch('aws_iam_management.examples.lambda_execution_role_example.LoggingUtils')
    def test_create_lambda_execution_role_with_templates(
        self,
        mock_logging_utils_class,
        mock_role_templates_class
    ):
        """
        Test creating a Lambda execution role using templates.
        """
        # Create mock instances
        mock_templates = MagicMock()
        mock_logging = MagicMock()
        
        # Configure the mock classes to return our mock instances
        mock_role_templates_class.return_value = mock_templates
        mock_logging_utils_class.return_value = mock_logging
        
        # Configure the mock templates to return a role
        mock_templates.create_enhanced_lambda_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": f"Lambda execution role for lambda-app in production",
            "AssumeRolePolicyDocument": json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }),
            "MaxSessionDuration": 3600,
            "Path": "/service-role/",
            "Tags": [
                {
                    "Key": "Environment",
                    "Value": "production"
                },
                {
                    "Key": "Application",
                    "Value": "lambda-app"
                },
                {
                    "Key": "Owner",
                    "Value": "platform-team"
                },
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "LambdaExecution"
                }
            ]
        }
        
        # Call the method under test
        result = create_lambda_execution_role_with_templates(
            role_name=self.test_role_name,
            s3_access=True,
            dynamodb_access=True,
            vpc_access=True,
            session=self.mock_session
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], "Lambda execution role for lambda-app in production")
        
        # Verify that the templates were called with the correct parameters
        mock_templates.create_enhanced_lambda_role.assert_called_once()
        
        # Verify that the logging utils were called
        mock_logging.log_role_change.assert_called_once()
        
    @patch('aws_iam_management.examples.lambda_execution_role_example.RoleManager')
    @patch('aws_iam_management.examples.lambda_execution_role_example.ValidationUtils')
    def test_create_lambda_execution_role_with_validation_findings(
        self,
        mock_validation_utils_class,
        mock_role_manager_class
    ):
        """
        Test creating a Lambda execution role with validation findings.
        """
        # Create mock instances
        mock_role_manager = MagicMock()
        mock_validation = MagicMock()
        
        # Configure the mock classes to return our mock instances
        mock_role_manager_class.return_value = mock_role_manager
        mock_validation_utils_class.return_value = mock_validation
        
        # Configure the mock validation to return findings
        mock_validation.validate_trust_relationship.return_value = [
            {
                'severity': 'WARNING',
                'message': 'Trust policy should use version 2012-10-17',
                'location': 'Version'
            }
        ]
        mock_validation.validate_policy_least_privilege.return_value = [
            {
                'severity': 'WARNING',
                'message': 'Statement 0 uses wildcard resource "*"',
                'location': 'Statement[0].Resource'
            }
        ]
        
        # Configure the mock role manager to return a role
        mock_role_manager.create_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": f"Lambda execution role for lambda-app in production",
            "AssumeRolePolicyDocument": json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }),
            "MaxSessionDuration": 3600,
            "Path": "/service-role/",
            "Tags": []
        }
        
        # Configure the mock role manager to return success for put_role_policy
        mock_role_manager.put_role_policy.return_value = True
        
        # Call the method under test
        result = create_lambda_execution_role(
            role_name=self.test_role_name,
            session=self.mock_session
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        
        # Verify that the validation was called
        mock_validation.validate_trust_relationship.assert_called_once()
        mock_validation.validate_policy_least_privilege.assert_called()
        
    @patch('aws_iam_management.examples.lambda_execution_role_example.RoleManager')
    def test_create_lambda_execution_role_with_error(
        self,
        mock_role_manager_class
    ):
        """
        Test creating a Lambda execution role with an error.
        """
        # Create a mock role manager
        mock_role_manager = MagicMock()
        
        # Configure the mock class to return our mock instance
        mock_role_manager_class.return_value = mock_role_manager
        
        # Configure the mock role manager to raise an exception
        mock_role_manager.create_role.side_effect = ClientError(
            {
                "Error": {
                    "Code": "EntityAlreadyExists",
                    "Message": "Role with name test-lambda-role already exists."
                }
            },
            "CreateRole"
        )
        
        # Call the method under test and verify that it raises an exception
        with self.assertRaises(ClientError):
            create_lambda_execution_role(
                role_name=self.test_role_name,
                session=self.mock_session
            )

if __name__ == "__main__":
    unittest.main()
