"""
Unit tests for the RoleManager class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.core.role_manager import RoleManager

class TestRoleManager(unittest.TestCase):
    """
    Test cases for the RoleManager class.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a mock session and client
        self.mock_session = MagicMock(spec=boto3.Session)
        self.mock_iam_client = MagicMock()
        self.mock_session.client.return_value = self.mock_iam_client
        
        # Create a RoleManager instance with the mock session
        self.role_manager = RoleManager(session=self.mock_session)
        
        # Define test data
        self.test_role_name = "test-role"
        self.test_trust_policy = {
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
        self.test_description = "Test role"
        self.test_tags = [
            {
                "Key": "Environment",
                "Value": "test"
            }
        ]
        
    def test_create_role(self):
        """
        Test creating a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.create_role.return_value = {
            "Role": {
                "RoleName": self.test_role_name,
                "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
                "Description": self.test_description,
                "AssumeRolePolicyDocument": json.dumps(self.test_trust_policy),
                "MaxSessionDuration": 3600,
                "Path": "/",
                "Tags": self.test_tags
            }
        }
        
        # Call the method under test
        result = self.role_manager.create_role(
            role_name=self.test_role_name,
            trust_policy=self.test_trust_policy,
            description=self.test_description,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.create_role.assert_called_once_with(
            RoleName=self.test_role_name,
            AssumeRolePolicyDocument=json.dumps(self.test_trust_policy),
            Description=self.test_description,
            MaxSessionDuration=3600,
            Path="/"
        )
        
    def test_create_role_with_error(self):
        """
        Test creating a role with an error.
        """
        # Mock the IAM client to raise an exception
        self.mock_iam_client.create_role.side_effect = ClientError(
            {
                "Error": {
                    "Code": "EntityAlreadyExists",
                    "Message": "Role with name test-role already exists."
                }
            },
            "CreateRole"
        )
        
        # Call the method under test and verify that it raises an exception
        with self.assertRaises(ClientError):
            self.role_manager.create_role(
                role_name=self.test_role_name,
                trust_policy=self.test_trust_policy
            )
            
    def test_delete_role(self):
        """
        Test deleting a role.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {
                    "PolicyName": "test-policy",
                    "PolicyArn": "arn:aws:iam::123456789012:policy/test-policy"
                }
            ]
        }
        self.mock_iam_client.list_role_policies.return_value = {
            "PolicyNames": ["inline-policy"]
        }
        
        # Call the method under test
        result = self.role_manager.delete_role(self.test_role_name)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_attached_role_policies.assert_called_once_with(
            RoleName=self.test_role_name
        )
        self.mock_iam_client.detach_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyArn="arn:aws:iam::123456789012:policy/test-policy"
        )
        self.mock_iam_client.list_role_policies.assert_called_once_with(
            RoleName=self.test_role_name
        )
        self.mock_iam_client.delete_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyName="inline-policy"
        )
        self.mock_iam_client.delete_role.assert_called_once_with(
            RoleName=self.test_role_name
        )
        
    def test_get_role(self):
        """
        Test getting a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.get_role.return_value = {
            "Role": {
                "RoleName": self.test_role_name,
                "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
                "Description": self.test_description,
                "AssumeRolePolicyDocument": json.dumps(self.test_trust_policy),
                "MaxSessionDuration": 3600,
                "Path": "/",
                "Tags": self.test_tags
            }
        }
        
        # Call the method under test
        result = self.role_manager.get_role(self.test_role_name)
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.get_role.assert_called_once_with(
            RoleName=self.test_role_name
        )
        
    def test_list_roles(self):
        """
        Test listing roles.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_roles.return_value = {
            "Roles": [
                {
                    "RoleName": self.test_role_name,
                    "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
                    "Description": self.test_description,
                    "AssumeRolePolicyDocument": json.dumps(self.test_trust_policy),
                    "MaxSessionDuration": 3600,
                    "Path": "/",
                    "Tags": self.test_tags
                }
            ]
        }
        
        # Call the method under test
        result = self.role_manager.list_roles()
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["RoleName"], self.test_role_name)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_roles.assert_called_once_with(
            PathPrefix="/",
            MaxItems=100
        )
        
    def test_update_role(self):
        """
        Test updating a role.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.update_role.return_value = {}
        self.mock_iam_client.get_role.return_value = {
            "Role": {
                "RoleName": self.test_role_name,
                "Arn": f"arn:aws:iam::123456789012:role/{self.test_role_name}",
                "Description": "Updated description",
                "AssumeRolePolicyDocument": json.dumps(self.test_trust_policy),
                "MaxSessionDuration": 7200,
                "Path": "/",
                "Tags": self.test_tags
            }
        }
        
        # Call the method under test
        result = self.role_manager.update_role(
            role_name=self.test_role_name,
            description="Updated description",
            max_session_duration=7200
        )
        
        # Verify the result
        self.assertEqual(result["RoleName"], self.test_role_name)
        self.assertEqual(result["Description"], "Updated description")
        self.assertEqual(result["MaxSessionDuration"], 7200)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.update_role.assert_called_once_with(
            RoleName=self.test_role_name,
            Description="Updated description",
            MaxSessionDuration=7200
        )
        self.mock_iam_client.get_role.assert_called_once_with(
            RoleName=self.test_role_name
        )
        
    def test_attach_policy(self):
        """
        Test attaching a policy to a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.attach_role_policy.return_value = {}
        
        # Call the method under test
        result = self.role_manager.attach_policy(
            role_name=self.test_role_name,
            policy_arn="arn:aws:iam::123456789012:policy/test-policy"
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.attach_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyArn="arn:aws:iam::123456789012:policy/test-policy"
        )
        
    def test_detach_policy(self):
        """
        Test detaching a policy from a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.detach_role_policy.return_value = {}
        
        # Call the method under test
        result = self.role_manager.detach_policy(
            role_name=self.test_role_name,
            policy_arn="arn:aws:iam::123456789012:policy/test-policy"
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.detach_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyArn="arn:aws:iam::123456789012:policy/test-policy"
        )
        
    def test_put_role_policy(self):
        """
        Test adding an inline policy to a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.put_role_policy.return_value = {}
        
        # Define a test policy document
        test_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::test-bucket"
                }
            ]
        }
        
        # Call the method under test
        result = self.role_manager.put_role_policy(
            role_name=self.test_role_name,
            policy_name="test-inline-policy",
            policy_document=test_policy_document
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.put_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyName="test-inline-policy",
            PolicyDocument=json.dumps(test_policy_document)
        )
        
    def test_delete_role_policy(self):
        """
        Test deleting an inline policy from a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.delete_role_policy.return_value = {}
        
        # Call the method under test
        result = self.role_manager.delete_role_policy(
            role_name=self.test_role_name,
            policy_name="test-inline-policy"
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.delete_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyName="test-inline-policy"
        )
        
    def test_list_attached_role_policies(self):
        """
        Test listing attached policies for a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {
                    "PolicyName": "test-policy",
                    "PolicyArn": "arn:aws:iam::123456789012:policy/test-policy"
                }
            ]
        }
        
        # Call the method under test
        result = self.role_manager.list_attached_role_policies(self.test_role_name)
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["PolicyName"], "test-policy")
        self.assertEqual(result[0]["PolicyArn"], "arn:aws:iam::123456789012:policy/test-policy")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_attached_role_policies.assert_called_once_with(
            RoleName=self.test_role_name
        )
        
    def test_list_role_policies(self):
        """
        Test listing inline policies for a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_role_policies.return_value = {
            "PolicyNames": ["test-inline-policy"]
        }
        
        # Call the method under test
        result = self.role_manager.list_role_policies(self.test_role_name)
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "test-inline-policy")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_role_policies.assert_called_once_with(
            RoleName=self.test_role_name
        )
        
    def test_get_role_policy(self):
        """
        Test getting an inline policy for a role.
        """
        # Define a test policy document
        test_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::test-bucket"
                }
            ]
        }
        
        # Mock the response from the IAM client
        self.mock_iam_client.get_role_policy.return_value = {
            "RoleName": self.test_role_name,
            "PolicyName": "test-inline-policy",
            "PolicyDocument": json.dumps(test_policy_document)
        }
        
        # Call the method under test
        result = self.role_manager.get_role_policy(
            role_name=self.test_role_name,
            policy_name="test-inline-policy"
        )
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(result["Statement"][0]["Action"], "s3:ListBucket")
        self.assertEqual(result["Statement"][0]["Resource"], "arn:aws:s3:::test-bucket")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.get_role_policy.assert_called_once_with(
            RoleName=self.test_role_name,
            PolicyName="test-inline-policy"
        )

if __name__ == "__main__":
    unittest.main()
