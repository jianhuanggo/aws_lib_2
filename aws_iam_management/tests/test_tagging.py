"""
Unit tests for the TaggingUtils class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.utils.tagging import TaggingUtils

class TestTaggingUtils(unittest.TestCase):
    """
    Test cases for the TaggingUtils class.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a mock session and client
        self.mock_session = MagicMock(spec=boto3.Session)
        self.mock_iam_client = MagicMock()
        self.mock_session.client.return_value = self.mock_iam_client
        
        # Create a TaggingUtils instance with the mock session
        self.tagging = TaggingUtils(session=self.mock_session)
        
        # Define test data
        self.test_role_arn = "arn:aws:iam::123456789012:role/test-role"
        self.test_policy_arn = "arn:aws:iam::123456789012:policy/test-policy"
        self.test_environment = "test"
        self.test_application = "test-app"
        self.test_owner = "test-team"
        self.test_additional_tags = {
            "CostCenter": "123456",
            "Project": "test-project"
        }
        
    def test_apply_standard_tags_to_role(self):
        """
        Test applying standard tags to a role.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.tag_role.return_value = {}
        
        # Call the method under test
        result = self.tagging.apply_standard_tags(
            resource_arn=self.test_role_arn,
            resource_type="role",
            environment=self.test_environment,
            application=self.test_application,
            owner=self.test_owner,
            additional_tags=self.test_additional_tags
        )
        
        # Verify the result
        self.assertEqual(len(result), 6)
        self.assertEqual(result[0]["Key"], "Environment")
        self.assertEqual(result[0]["Value"], self.test_environment)
        self.assertEqual(result[1]["Key"], "Application")
        self.assertEqual(result[1]["Value"], self.test_application)
        self.assertEqual(result[2]["Key"], "Owner")
        self.assertEqual(result[2]["Value"], self.test_owner)
        self.assertEqual(result[3]["Key"], "ManagedBy")
        self.assertEqual(result[3]["Value"], "aws_iam_management")
        self.assertEqual(result[4]["Key"], "CostCenter")
        self.assertEqual(result[4]["Value"], "123456")
        self.assertEqual(result[5]["Key"], "Project")
        self.assertEqual(result[5]["Value"], "test-project")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.tag_role.assert_called_once()
        
    def test_apply_standard_tags_to_policy(self):
        """
        Test applying standard tags to a policy.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.tag_policy.return_value = {}
        
        # Call the method under test
        result = self.tagging.apply_standard_tags(
            resource_arn=self.test_policy_arn,
            resource_type="policy",
            environment=self.test_environment,
            application=self.test_application,
            owner=self.test_owner
        )
        
        # Verify the result
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]["Key"], "Environment")
        self.assertEqual(result[0]["Value"], self.test_environment)
        self.assertEqual(result[1]["Key"], "Application")
        self.assertEqual(result[1]["Value"], self.test_application)
        self.assertEqual(result[2]["Key"], "Owner")
        self.assertEqual(result[2]["Value"], self.test_owner)
        self.assertEqual(result[3]["Key"], "ManagedBy")
        self.assertEqual(result[3]["Value"], "aws_iam_management")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.tag_policy.assert_called_once()
        
    def test_apply_standard_tags_to_unsupported_resource(self):
        """
        Test applying standard tags to an unsupported resource type.
        """
        # Call the method under test
        result = self.tagging.apply_standard_tags(
            resource_arn="arn:aws:iam::123456789012:user/test-user",
            resource_type="user",
            environment=self.test_environment,
            application=self.test_application,
            owner=self.test_owner
        )
        
        # Verify the result
        self.assertEqual(len(result), 0)
        
        # Verify that the IAM client was not called
        self.mock_iam_client.tag_role.assert_not_called()
        self.mock_iam_client.tag_policy.assert_not_called()
        
    def test_get_resources_by_tag_roles(self):
        """
        Test getting roles by tag.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_roles.return_value = {
            "Roles": [
                {
                    "RoleName": "test-role-1",
                    "Arn": "arn:aws:iam::123456789012:role/test-role-1",
                    "Path": "/",
                    "RoleId": "ABCDEFGHIJKLMNOPQRSTU",
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "AssumeRolePolicyDocument": "{}"
                },
                {
                    "RoleName": "test-role-2",
                    "Arn": "arn:aws:iam::123456789012:role/test-role-2",
                    "Path": "/",
                    "RoleId": "VWXYZABCDEFGHIJKLMNO",
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "AssumeRolePolicyDocument": "{}"
                }
            ]
        }
        self.mock_iam_client.list_role_tags.side_effect = [
            {
                "Tags": [
                    {
                        "Key": "Environment",
                        "Value": "test"
                    }
                ]
            },
            {
                "Tags": [
                    {
                        "Key": "Environment",
                        "Value": "prod"
                    }
                ]
            }
        ]
        
        # Call the method under test
        result = self.tagging.get_resources_by_tag(
            resource_type="role",
            tag_key="Environment",
            tag_value="test"
        )
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["RoleName"], "test-role-1")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_roles.assert_called_once()
        self.mock_iam_client.list_role_tags.assert_called()
        
    def test_get_resources_by_tag_policies(self):
        """
        Test getting policies by tag.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_policies.return_value = {
            "Policies": [
                {
                    "PolicyName": "test-policy-1",
                    "PolicyId": "ABCDEFGHIJKLMNOPQRSTU",
                    "Arn": "arn:aws:iam::123456789012:policy/test-policy-1",
                    "Path": "/",
                    "DefaultVersionId": "v1",
                    "AttachmentCount": 0,
                    "PermissionsBoundaryUsageCount": 0,
                    "IsAttachable": True,
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "UpdateDate": "2023-01-01T00:00:00Z"
                },
                {
                    "PolicyName": "test-policy-2",
                    "PolicyId": "VWXYZABCDEFGHIJKLMNO",
                    "Arn": "arn:aws:iam::123456789012:policy/test-policy-2",
                    "Path": "/",
                    "DefaultVersionId": "v1",
                    "AttachmentCount": 0,
                    "PermissionsBoundaryUsageCount": 0,
                    "IsAttachable": True,
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "UpdateDate": "2023-01-01T00:00:00Z"
                }
            ]
        }
        self.mock_iam_client.list_policy_tags.side_effect = [
            {
                "Tags": [
                    {
                        "Key": "Environment",
                        "Value": "test"
                    }
                ]
            },
            {
                "Tags": [
                    {
                        "Key": "Environment",
                        "Value": "prod"
                    }
                ]
            }
        ]
        
        # Call the method under test
        result = self.tagging.get_resources_by_tag(
            resource_type="policy",
            tag_key="Environment",
            tag_value="test"
        )
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["PolicyName"], "test-policy-1")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_policies.assert_called_once()
        self.mock_iam_client.list_policy_tags.assert_called()
        
    def test_get_all_tagged_resources(self):
        """
        Test getting all tagged resources.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_roles.return_value = {
            "Roles": [
                {
                    "RoleName": "test-role-1",
                    "Arn": "arn:aws:iam::123456789012:role/test-role-1",
                    "Path": "/",
                    "RoleId": "ABCDEFGHIJKLMNOPQRSTU",
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "AssumeRolePolicyDocument": "{}"
                }
            ]
        }
        self.mock_iam_client.list_policies.return_value = {
            "Policies": [
                {
                    "PolicyName": "test-policy-1",
                    "PolicyId": "ABCDEFGHIJKLMNOPQRSTU",
                    "Arn": "arn:aws:iam::123456789012:policy/test-policy-1",
                    "Path": "/",
                    "DefaultVersionId": "v1",
                    "AttachmentCount": 0,
                    "PermissionsBoundaryUsageCount": 0,
                    "IsAttachable": True,
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "UpdateDate": "2023-01-01T00:00:00Z"
                }
            ]
        }
        self.mock_iam_client.list_role_tags.return_value = {
            "Tags": [
                {
                    "Key": "Environment",
                    "Value": "test"
                }
            ]
        }
        self.mock_iam_client.list_policy_tags.return_value = {
            "Tags": [
                {
                    "Key": "Environment",
                    "Value": "test"
                }
            ]
        }
        
        # Call the method under test
        result = self.tagging.get_all_tagged_resources()
        
        # Verify the result
        self.assertEqual(len(result["roles"]), 1)
        self.assertEqual(result["roles"][0]["RoleName"], "test-role-1")
        self.assertEqual(len(result["policies"]), 1)
        self.assertEqual(result["policies"][0]["PolicyName"], "test-policy-1")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_roles.assert_called_once()
        self.mock_iam_client.list_policies.assert_called_once()
        self.mock_iam_client.list_role_tags.assert_called_once()
        self.mock_iam_client.list_policy_tags.assert_called_once()

if __name__ == "__main__":
    unittest.main()
