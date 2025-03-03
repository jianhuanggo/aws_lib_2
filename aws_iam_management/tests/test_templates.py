"""
Unit tests for the RoleTemplates class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.core.templates import RoleTemplates
from aws_iam_management.core.permission_sets import PermissionSets

class TestRoleTemplates(unittest.TestCase):
    """
    Test cases for the RoleTemplates class.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a mock session
        self.mock_session = MagicMock(spec=boto3.Session)
        
        # Create a mock role manager and policy manager
        self.mock_role_manager = MagicMock()
        self.mock_policy_manager = MagicMock()
        
        # Patch the RoleManager and PolicyManager classes
        self.role_manager_patcher = patch('aws_iam_management.core.templates.RoleManager')
        self.policy_manager_patcher = patch('aws_iam_management.core.templates.PolicyManager')
        
        # Start the patchers
        self.mock_role_manager_class = self.role_manager_patcher.start()
        self.mock_policy_manager_class = self.policy_manager_patcher.start()
        
        # Configure the mock classes to return our mock instances
        self.mock_role_manager_class.return_value = self.mock_role_manager
        self.mock_policy_manager_class.return_value = self.mock_policy_manager
        
        # Create a RoleTemplates instance with the mock session
        self.role_templates = RoleTemplates(session=self.mock_session)
        
        # Define test data
        self.test_role_name = "test-role"
        self.test_description = "Test role"
        self.test_tags = [
            {
                "Key": "Environment",
                "Value": "test"
            }
        ]
        
    def tearDown(self):
        """
        Clean up after the test.
        """
        # Stop the patchers
        self.role_manager_patcher.stop()
        self.policy_manager_patcher.stop()
        
    def test_create_lambda_execution_role(self):
        """
        Test creating a Lambda execution role.
        """
        # Mock the response from the role manager
        self.mock_role_manager.create_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": self.test_description,
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
            "Tags": self.test_tags
        }
        
        # Call the method under test
        result = self.role_templates.create_lambda_execution_role(
            role_name=self.test_role_name,
            description=self.test_description,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the role manager was called with the correct parameters
        self.mock_role_manager.create_role.assert_called_once()
        self.mock_role_manager.put_role_policy.assert_called_once()
        
    def test_create_ec2_instance_role(self):
        """
        Test creating an EC2 instance role.
        """
        # Mock the response from the role manager
        self.mock_role_manager.create_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": self.test_description,
            "AssumeRolePolicyDocument": json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "ec2.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }),
            "MaxSessionDuration": 3600,
            "Path": "/service-role/",
            "Tags": self.test_tags
        }
        
        # Call the method under test
        result = self.role_templates.create_ec2_instance_role(
            role_name=self.test_role_name,
            description=self.test_description,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the role manager was called with the correct parameters
        self.mock_role_manager.create_role.assert_called_once()
        
    def test_create_custom_role(self):
        """
        Test creating a custom role.
        """
        # Define a test trust policy
        test_trust_policy = {
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
        }
        
        # Mock the response from the role manager
        self.mock_role_manager.create_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": self.test_description,
            "AssumeRolePolicyDocument": json.dumps(test_trust_policy),
            "MaxSessionDuration": 3600,
            "Path": "/",
            "Tags": self.test_tags
        }
        
        # Define test permissions
        test_permissions = [PermissionSets.basic_lambda_execution()]
        
        # Call the method under test
        result = self.role_templates.create_custom_role(
            role_name=self.test_role_name,
            trust_policy=test_trust_policy,
            description=self.test_description,
            permissions=test_permissions,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the role manager was called with the correct parameters
        self.mock_role_manager.create_role.assert_called_once()
        self.mock_role_manager.put_role_policy.assert_called_once()
        
    def test_create_enhanced_lambda_role(self):
        """
        Test creating an enhanced Lambda execution role.
        """
        # Mock the response from the role manager
        self.mock_role_manager.create_role.return_value = {
            "RoleName": self.test_role_name,
            "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
            "Description": self.test_description,
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
            "Tags": self.test_tags
        }
        
        # Call the method under test
        result = self.role_templates.create_enhanced_lambda_role(
            role_name=self.test_role_name,
            description=self.test_description,
            s3_access=True,
            dynamodb_access=True,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the role manager was called with the correct parameters
        self.mock_role_manager.create_role.assert_called_once()
        # Should be called 3 times: basic Lambda execution, S3 access, DynamoDB access
        self.assertEqual(self.mock_role_manager.put_role_policy.call_count, 3)

if __name__ == "__main__":
    unittest.main()
