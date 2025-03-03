"""
Unit tests for the PolicyManager class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.core.policy_manager import PolicyManager

class TestPolicyManager(unittest.TestCase):
    """
    Test cases for the PolicyManager class.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a mock session and client
        self.mock_session = MagicMock(spec=boto3.Session)
        self.mock_iam_client = MagicMock()
        self.mock_session.client.return_value = self.mock_iam_client
        
        # Create a PolicyManager instance with the mock session
        self.policy_manager = PolicyManager(session=self.mock_session)
        
        # Define test data
        self.test_policy_name = "test-policy"
        self.test_policy_arn = f"arn:aws:iam::123456789012:policy/{self.test_policy_name}"
        self.test_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::test-bucket"
                }
            ]
        }
        self.test_description = "Test policy"
        self.test_tags = [
            {
                "Key": "Environment",
                "Value": "test"
            }
        ]
        
    def test_create_policy(self):
        """
        Test creating a policy.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.create_policy.return_value = {
            "Policy": {
                "PolicyName": self.test_policy_name,
                "PolicyId": "ABCDEFGHIJKLMNOPQRSTU",
                "Arn": self.test_policy_arn,
                "Path": "/",
                "DefaultVersionId": "v1",
                "AttachmentCount": 0,
                "PermissionsBoundaryUsageCount": 0,
                "IsAttachable": True,
                "Description": self.test_description,
                "CreateDate": "2023-01-01T00:00:00Z",
                "UpdateDate": "2023-01-01T00:00:00Z",
                "Tags": self.test_tags
            }
        }
        
        # Call the method under test
        result = self.policy_manager.create_policy(
            policy_name=self.test_policy_name,
            policy_document=self.test_policy_document,
            description=self.test_description,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertEqual(result["PolicyName"], self.test_policy_name)
        self.assertEqual(result["Arn"], self.test_policy_arn)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.create_policy.assert_called_once_with(
            PolicyName=self.test_policy_name,
            PolicyDocument=json.dumps(self.test_policy_document),
            Description=self.test_description,
            Path="/",
            Tags=self.test_tags
        )
        
    def test_delete_policy(self):
        """
        Test deleting a policy.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_entities_for_policy.return_value = {
            "PolicyGroups": [],
            "PolicyUsers": [],
            "PolicyRoles": []
        }
        self.mock_iam_client.list_policy_versions.return_value = {
            "Versions": [
                {
                    "VersionId": "v1",
                    "IsDefaultVersion": True,
                    "CreateDate": "2023-01-01T00:00:00Z"
                }
            ]
        }
        
        # Call the method under test
        result = self.policy_manager.delete_policy(self.test_policy_arn)
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_entities_for_policy.assert_called_once()
        self.mock_iam_client.list_policy_versions.assert_called_once_with(
            PolicyArn=self.test_policy_arn
        )
        self.mock_iam_client.delete_policy.assert_called_once_with(
            PolicyArn=self.test_policy_arn
        )
        
    def test_get_policy(self):
        """
        Test getting a policy.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.get_policy.return_value = {
            "Policy": {
                "PolicyName": self.test_policy_name,
                "PolicyId": "ABCDEFGHIJKLMNOPQRSTU",
                "Arn": self.test_policy_arn,
                "Path": "/",
                "DefaultVersionId": "v1",
                "AttachmentCount": 0,
                "PermissionsBoundaryUsageCount": 0,
                "IsAttachable": True,
                "Description": self.test_description,
                "CreateDate": "2023-01-01T00:00:00Z",
                "UpdateDate": "2023-01-01T00:00:00Z"
            }
        }
        
        # Call the method under test
        result = self.policy_manager.get_policy(self.test_policy_arn)
        
        # Verify the result
        self.assertEqual(result["PolicyName"], self.test_policy_name)
        self.assertEqual(result["Arn"], self.test_policy_arn)
        self.assertEqual(result["Description"], self.test_description)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.get_policy.assert_called_once_with(
            PolicyArn=self.test_policy_arn
        )
        
    def test_list_policies(self):
        """
        Test listing policies.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_policies.return_value = {
            "Policies": [
                {
                    "PolicyName": self.test_policy_name,
                    "PolicyId": "ABCDEFGHIJKLMNOPQRSTU",
                    "Arn": self.test_policy_arn,
                    "Path": "/",
                    "DefaultVersionId": "v1",
                    "AttachmentCount": 0,
                    "PermissionsBoundaryUsageCount": 0,
                    "IsAttachable": True,
                    "Description": self.test_description,
                    "CreateDate": "2023-01-01T00:00:00Z",
                    "UpdateDate": "2023-01-01T00:00:00Z"
                }
            ]
        }
        
        # Call the method under test
        result = self.policy_manager.list_policies()
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["PolicyName"], self.test_policy_name)
        self.assertEqual(result[0]["Arn"], self.test_policy_arn)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_policies.assert_called_once_with(
            Scope="Local",
            OnlyAttached=False,
            PathPrefix="/",
            MaxItems=100
        )
        
    def test_create_policy_version(self):
        """
        Test creating a policy version.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_policy_versions.return_value = {
            "Versions": [
                {
                    "VersionId": "v1",
                    "IsDefaultVersion": True,
                    "CreateDate": "2023-01-01T00:00:00Z"
                }
            ]
        }
        self.mock_iam_client.create_policy_version.return_value = {
            "PolicyVersion": {
                "VersionId": "v2",
                "IsDefaultVersion": True,
                "CreateDate": "2023-01-02T00:00:00Z"
            }
        }
        
        # Call the method under test
        result = self.policy_manager.create_policy_version(
            policy_arn=self.test_policy_arn,
            policy_document=self.test_policy_document,
            set_as_default=True
        )
        
        # Verify the result
        self.assertEqual(result["VersionId"], "v2")
        self.assertTrue(result["IsDefaultVersion"])
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_policy_versions.assert_called_once_with(
            PolicyArn=self.test_policy_arn
        )
        self.mock_iam_client.create_policy_version.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            PolicyDocument=json.dumps(self.test_policy_document),
            SetAsDefault=True
        )
        
    def test_get_policy_version(self):
        """
        Test getting a policy version.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.get_policy_version.return_value = {
            "PolicyVersion": {
                "Document": json.dumps(self.test_policy_document),
                "VersionId": "v1",
                "IsDefaultVersion": True,
                "CreateDate": "2023-01-01T00:00:00Z"
            }
        }
        
        # Call the method under test
        result = self.policy_manager.get_policy_version(
            policy_arn=self.test_policy_arn,
            version_id="v1"
        )
        
        # Verify the result
        self.assertEqual(result["VersionId"], "v1")
        self.assertTrue(result["IsDefaultVersion"])
        self.assertEqual(result["Document"], self.test_policy_document)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.get_policy_version.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            VersionId="v1"
        )
        
    def test_list_policy_versions(self):
        """
        Test listing policy versions.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_policy_versions.return_value = {
            "Versions": [
                {
                    "VersionId": "v1",
                    "IsDefaultVersion": True,
                    "CreateDate": "2023-01-01T00:00:00Z"
                }
            ]
        }
        
        # Call the method under test
        result = self.policy_manager.list_policy_versions(self.test_policy_arn)
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["VersionId"], "v1")
        self.assertTrue(result[0]["IsDefaultVersion"])
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_policy_versions.assert_called_once_with(
            PolicyArn=self.test_policy_arn
        )
        
    def test_delete_policy_version(self):
        """
        Test deleting a policy version.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.delete_policy_version.return_value = {}
        
        # Call the method under test
        result = self.policy_manager.delete_policy_version(
            policy_arn=self.test_policy_arn,
            version_id="v1"
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.delete_policy_version.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            VersionId="v1"
        )
        
    def test_set_default_policy_version(self):
        """
        Test setting the default policy version.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.set_default_policy_version.return_value = {}
        
        # Call the method under test
        result = self.policy_manager.set_default_policy_version(
            policy_arn=self.test_policy_arn,
            version_id="v1"
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.set_default_policy_version.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            VersionId="v1"
        )
        
    def test_cleanup_policy_versions(self):
        """
        Test cleaning up policy versions.
        """
        # Mock the responses from the IAM client
        self.mock_iam_client.list_policy_versions.return_value = {
            "Versions": [
                {
                    "VersionId": "v1",
                    "IsDefaultVersion": True,
                    "CreateDate": "2023-01-01T00:00:00Z"
                },
                {
                    "VersionId": "v2",
                    "IsDefaultVersion": False,
                    "CreateDate": "2023-01-02T00:00:00Z"
                },
                {
                    "VersionId": "v3",
                    "IsDefaultVersion": False,
                    "CreateDate": "2023-01-03T00:00:00Z"
                },
                {
                    "VersionId": "v4",
                    "IsDefaultVersion": False,
                    "CreateDate": "2023-01-04T00:00:00Z"
                },
                {
                    "VersionId": "v5",
                    "IsDefaultVersion": False,
                    "CreateDate": "2023-01-05T00:00:00Z"
                }
            ]
        }
        
        # Call the method under test
        self.policy_manager.cleanup_policy_versions(
            policy_arn=self.test_policy_arn,
            keep_count=3
        )
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_policy_versions.assert_called_once_with(
            PolicyArn=self.test_policy_arn
        )
        self.mock_iam_client.delete_policy_version.assert_called_with(
            PolicyArn=self.test_policy_arn,
            VersionId="v2"
        )
        
    def test_detach_policy_from_all_entities(self):
        """
        Test detaching a policy from all entities.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_entities_for_policy.return_value = {
            "PolicyGroups": [
                {
                    "GroupName": "test-group",
                    "GroupId": "ABCDEFGHIJKLMNOPQRSTU"
                }
            ],
            "PolicyUsers": [
                {
                    "UserName": "test-user",
                    "UserId": "ABCDEFGHIJKLMNOPQRSTU"
                }
            ],
            "PolicyRoles": [
                {
                    "RoleName": "test-role",
                    "RoleId": "ABCDEFGHIJKLMNOPQRSTU"
                }
            ]
        }
        
        # Call the method under test
        self.policy_manager.detach_policy_from_all_entities(self.test_policy_arn)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_entities_for_policy.assert_called_once()
        self.mock_iam_client.detach_user_policy.assert_called_once_with(
            UserName="test-user",
            PolicyArn=self.test_policy_arn
        )
        self.mock_iam_client.detach_group_policy.assert_called_once_with(
            GroupName="test-group",
            PolicyArn=self.test_policy_arn
        )
        self.mock_iam_client.detach_role_policy.assert_called_once_with(
            RoleName="test-role",
            PolicyArn=self.test_policy_arn
        )
        
    def test_list_entities_for_policy(self):
        """
        Test listing entities for a policy.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.list_entities_for_policy.return_value = {
            "PolicyGroups": [
                {
                    "GroupName": "test-group",
                    "GroupId": "ABCDEFGHIJKLMNOPQRSTU"
                }
            ],
            "PolicyUsers": [
                {
                    "UserName": "test-user",
                    "UserId": "ABCDEFGHIJKLMNOPQRSTU"
                }
            ],
            "PolicyRoles": [
                {
                    "RoleName": "test-role",
                    "RoleId": "ABCDEFGHIJKLMNOPQRSTU"
                }
            ]
        }
        
        # Call the method under test
        result = self.policy_manager.list_entities_for_policy(self.test_policy_arn)
        
        # Verify the result
        self.assertEqual(len(result["PolicyGroups"]), 1)
        self.assertEqual(result["PolicyGroups"][0]["GroupName"], "test-group")
        self.assertEqual(len(result["PolicyUsers"]), 1)
        self.assertEqual(result["PolicyUsers"][0]["UserName"], "test-user")
        self.assertEqual(len(result["PolicyRoles"]), 1)
        self.assertEqual(result["PolicyRoles"][0]["RoleName"], "test-role")
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.list_entities_for_policy.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            EntityFilter="All",
            PathPrefix="/",
            MaxItems=100
        )
        
    def test_tag_policy(self):
        """
        Test tagging a policy.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.tag_policy.return_value = {}
        
        # Call the method under test
        result = self.policy_manager.tag_policy(
            policy_arn=self.test_policy_arn,
            tags=self.test_tags
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.tag_policy.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            Tags=self.test_tags
        )
        
    def test_untag_policy(self):
        """
        Test untagging a policy.
        """
        # Mock the response from the IAM client
        self.mock_iam_client.untag_policy.return_value = {}
        
        # Call the method under test
        result = self.policy_manager.untag_policy(
            policy_arn=self.test_policy_arn,
            tag_keys=["Environment"]
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify that the IAM client was called with the correct parameters
        self.mock_iam_client.untag_policy.assert_called_once_with(
            PolicyArn=self.test_policy_arn,
            TagKeys=["Environment"]
        )

if __name__ == "__main__":
    unittest.main()
