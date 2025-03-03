"""
IAM Policy Manager Module

This module provides a class for managing IAM policies in AWS.
It follows best practices for creating, updating, and deleting IAM policies.
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Union, Any

logger = logging.getLogger(__name__)

class PolicyManager:
    """
    A class for managing IAM policies in AWS.
    
    This class provides methods for creating, updating, and deleting IAM policies.
    """
    
    def __init__(self, session: Optional[boto3.Session] = None):
        """
        Initialize the PolicyManager with an optional boto3 session.
        
        Args:
            session: An optional boto3 Session object. If not provided, a new session will be created.
        """
        self.session = session or boto3.Session()
        self.iam_client = self.session.client('iam')
        
    def create_policy(
        self, 
        policy_name: str, 
        policy_document: Dict[str, Any], 
        description: str = "",
        path: str = "/",
        tags: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Create an IAM policy with the specified parameters.
        
        Args:
            policy_name: The name of the policy to create.
            policy_document: The policy document.
            description: A description of the policy.
            path: The path to the policy.
            tags: A list of tags to attach to the policy.
            
        Returns:
            The newly created policy as a dictionary.
            
        Raises:
            ClientError: If the policy cannot be created.
        """
        try:
            params = {
                'PolicyName': policy_name,
                'PolicyDocument': json.dumps(policy_document),
                'Description': description,
                'Path': path
            }
            
            if tags:
                params['Tags'] = tags
                
            response = self.iam_client.create_policy(**params)
            logger.info(f"Created IAM policy: {policy_name}")
            return response['Policy']
        except ClientError as e:
            logger.error(f"Error creating IAM policy {policy_name}: {e}")
            raise
            
    def delete_policy(self, policy_arn: str) -> bool:
        """
        Delete an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy to delete.
            
        Returns:
            True if the policy was deleted successfully, False otherwise.
            
        Raises:
            ClientError: If the policy cannot be deleted.
        """
        try:
            # First, detach the policy from all entities
            self.detach_policy_from_all_entities(policy_arn)
            
            # Delete all non-default versions of the policy
            self.delete_policy_versions(policy_arn)
            
            # Now delete the policy
            self.iam_client.delete_policy(PolicyArn=policy_arn)
            logger.info(f"Deleted IAM policy: {policy_arn}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting IAM policy {policy_arn}: {e}")
            raise
            
    def get_policy(self, policy_arn: str) -> Dict[str, Any]:
        """
        Get information about an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy to get information about.
            
        Returns:
            The policy information as a dictionary.
            
        Raises:
            ClientError: If the policy cannot be found.
        """
        try:
            response = self.iam_client.get_policy(PolicyArn=policy_arn)
            return response['Policy']
        except ClientError as e:
            logger.error(f"Error getting IAM policy {policy_arn}: {e}")
            raise
            
    def list_policies(
        self, 
        scope: str = "Local", 
        only_attached: bool = False,
        path_prefix: str = "/",
        max_items: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List IAM policies.
        
        Args:
            scope: The scope to filter policies by. Valid values: 'All', 'AWS', 'Local'.
            only_attached: If True, only attached policies are returned.
            path_prefix: The path prefix for filtering the results.
            max_items: The maximum number of items to return.
            
        Returns:
            A list of IAM policies.
            
        Raises:
            ClientError: If the policies cannot be listed.
        """
        try:
            response = self.iam_client.list_policies(
                Scope=scope,
                OnlyAttached=only_attached,
                PathPrefix=path_prefix,
                MaxItems=max_items
            )
            return response['Policies']
        except ClientError as e:
            logger.error(f"Error listing IAM policies: {e}")
            raise
            
    def create_policy_version(
        self, 
        policy_arn: str, 
        policy_document: Dict[str, Any], 
        set_as_default: bool = True
    ) -> Dict[str, Any]:
        """
        Create a new version of an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy to create a new version for.
            policy_document: The policy document.
            set_as_default: If True, the new version is set as the default version.
            
        Returns:
            The newly created policy version as a dictionary.
            
        Raises:
            ClientError: If the policy version cannot be created.
        """
        try:
            # Check if we need to delete old versions (maximum of 5 versions allowed)
            self.cleanup_policy_versions(policy_arn)
            
            response = self.iam_client.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=json.dumps(policy_document),
                SetAsDefault=set_as_default
            )
            logger.info(f"Created new version of IAM policy: {policy_arn}")
            return response['PolicyVersion']
        except ClientError as e:
            logger.error(f"Error creating new version of IAM policy {policy_arn}: {e}")
            raise
            
    def get_policy_version(self, policy_arn: str, version_id: str) -> Dict[str, Any]:
        """
        Get information about a version of an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy.
            version_id: The version ID.
            
        Returns:
            The policy version information as a dictionary.
            
        Raises:
            ClientError: If the policy version cannot be found.
        """
        try:
            response = self.iam_client.get_policy_version(
                PolicyArn=policy_arn,
                VersionId=version_id
            )
            # Convert the policy document from URL-encoded JSON to a dictionary
            policy_version = response['PolicyVersion']
            policy_version['Document'] = json.loads(policy_version['Document'])
            return policy_version
        except ClientError as e:
            logger.error(f"Error getting version {version_id} of IAM policy {policy_arn}: {e}")
            raise
            
    def list_policy_versions(self, policy_arn: str) -> List[Dict[str, Any]]:
        """
        List the versions of an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy to list versions for.
            
        Returns:
            A list of policy versions.
            
        Raises:
            ClientError: If the policy versions cannot be listed.
        """
        try:
            response = self.iam_client.list_policy_versions(PolicyArn=policy_arn)
            return response['Versions']
        except ClientError as e:
            logger.error(f"Error listing versions of IAM policy {policy_arn}: {e}")
            raise
            
    def delete_policy_version(self, policy_arn: str, version_id: str) -> bool:
        """
        Delete a version of an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy.
            version_id: The version ID.
            
        Returns:
            True if the policy version was deleted successfully, False otherwise.
            
        Raises:
            ClientError: If the policy version cannot be deleted.
        """
        try:
            self.iam_client.delete_policy_version(
                PolicyArn=policy_arn,
                VersionId=version_id
            )
            logger.info(f"Deleted version {version_id} of IAM policy: {policy_arn}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting version {version_id} of IAM policy {policy_arn}: {e}")
            raise
            
    def set_default_policy_version(self, policy_arn: str, version_id: str) -> bool:
        """
        Set the default version of an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy.
            version_id: The version ID to set as the default.
            
        Returns:
            True if the default version was set successfully, False otherwise.
            
        Raises:
            ClientError: If the default version cannot be set.
        """
        try:
            self.iam_client.set_default_policy_version(
                PolicyArn=policy_arn,
                VersionId=version_id
            )
            logger.info(f"Set version {version_id} as default for IAM policy: {policy_arn}")
            return True
        except ClientError as e:
            logger.error(f"Error setting version {version_id} as default for IAM policy {policy_arn}: {e}")
            raise
            
    def cleanup_policy_versions(self, policy_arn: str, keep_count: int = 4) -> None:
        """
        Delete old non-default versions of an IAM policy to make room for new versions.
        
        AWS allows a maximum of 5 versions per policy, so we need to delete old versions
        before creating new ones if we're at the limit.
        
        Args:
            policy_arn: The ARN of the policy.
            keep_count: The number of non-default versions to keep (default is 4, which
                        allows for 1 default version and 4 non-default versions).
            
        Raises:
            ClientError: If the policy versions cannot be listed or deleted.
        """
        try:
            versions = self.list_policy_versions(policy_arn)
            
            # Sort versions by creation date (oldest first)
            non_default_versions = [v for v in versions if not v['IsDefaultVersion']]
            non_default_versions.sort(key=lambda x: x['CreateDate'])
            
            # Delete oldest versions if we have more than keep_count
            if len(non_default_versions) >= keep_count:
                versions_to_delete = non_default_versions[:(len(non_default_versions) - keep_count + 1)]
                for version in versions_to_delete:
                    self.delete_policy_version(policy_arn, version['VersionId'])
        except ClientError as e:
            logger.error(f"Error cleaning up versions of IAM policy {policy_arn}: {e}")
            raise
            
    def delete_policy_versions(self, policy_arn: str) -> None:
        """
        Delete all non-default versions of an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy.
            
        Raises:
            ClientError: If the policy versions cannot be listed or deleted.
        """
        try:
            versions = self.list_policy_versions(policy_arn)
            
            # Delete all non-default versions
            for version in versions:
                if not version['IsDefaultVersion']:
                    self.delete_policy_version(policy_arn, version['VersionId'])
        except ClientError as e:
            logger.error(f"Error deleting versions of IAM policy {policy_arn}: {e}")
            raise
            
    def detach_policy_from_all_entities(self, policy_arn: str) -> None:
        """
        Detach an IAM policy from all entities (users, groups, and roles).
        
        Args:
            policy_arn: The ARN of the policy to detach.
            
        Raises:
            ClientError: If the policy cannot be detached.
        """
        try:
            # Get all entities that the policy is attached to
            entities = self.list_entities_for_policy(policy_arn)
            
            # Detach from users
            for user in entities.get('PolicyUsers', []):
                self.iam_client.detach_user_policy(
                    UserName=user['UserName'],
                    PolicyArn=policy_arn
                )
                logger.info(f"Detached policy {policy_arn} from user: {user['UserName']}")
                
            # Detach from groups
            for group in entities.get('PolicyGroups', []):
                self.iam_client.detach_group_policy(
                    GroupName=group['GroupName'],
                    PolicyArn=policy_arn
                )
                logger.info(f"Detached policy {policy_arn} from group: {group['GroupName']}")
                
            # Detach from roles
            for role in entities.get('PolicyRoles', []):
                self.iam_client.detach_role_policy(
                    RoleName=role['RoleName'],
                    PolicyArn=policy_arn
                )
                logger.info(f"Detached policy {policy_arn} from role: {role['RoleName']}")
        except ClientError as e:
            logger.error(f"Error detaching policy {policy_arn} from entities: {e}")
            raise
            
    def list_entities_for_policy(
        self, 
        policy_arn: str, 
        entity_filter: str = "All",
        path_prefix: str = "/",
        max_items: int = 100
    ) -> Dict[str, List[Dict[str, str]]]:
        """
        List all entities (users, groups, and roles) that a policy is attached to.
        
        Args:
            policy_arn: The ARN of the policy.
            entity_filter: The entity type to include. Valid values: 'User', 'Role', 'Group', 'LocalManagedPolicy', 'AWSManagedPolicy', 'All'.
            path_prefix: The path prefix for filtering the results.
            max_items: The maximum number of items to return.
            
        Returns:
            A dictionary containing lists of users, groups, and roles that the policy is attached to.
            
        Raises:
            ClientError: If the entities cannot be listed.
        """
        try:
            response = self.iam_client.list_entities_for_policy(
                PolicyArn=policy_arn,
                EntityFilter=entity_filter,
                PathPrefix=path_prefix,
                MaxItems=max_items
            )
            return {
                'PolicyUsers': response.get('PolicyUsers', []),
                'PolicyGroups': response.get('PolicyGroups', []),
                'PolicyRoles': response.get('PolicyRoles', [])
            }
        except ClientError as e:
            logger.error(f"Error listing entities for policy {policy_arn}: {e}")
            raise
            
    def tag_policy(self, policy_arn: str, tags: List[Dict[str, str]]) -> bool:
        """
        Add tags to an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy to tag.
            tags: A list of tags to add to the policy.
            
        Returns:
            True if the tags were added successfully, False otherwise.
            
        Raises:
            ClientError: If the tags cannot be added.
        """
        try:
            self.iam_client.tag_policy(
                PolicyArn=policy_arn,
                Tags=tags
            )
            logger.info(f"Added tags to IAM policy: {policy_arn}")
            return True
        except ClientError as e:
            logger.error(f"Error adding tags to IAM policy {policy_arn}: {e}")
            raise
            
    def untag_policy(self, policy_arn: str, tag_keys: List[str]) -> bool:
        """
        Remove tags from an IAM policy.
        
        Args:
            policy_arn: The ARN of the policy to remove tags from.
            tag_keys: A list of tag keys to remove.
            
        Returns:
            True if the tags were removed successfully, False otherwise.
            
        Raises:
            ClientError: If the tags cannot be removed.
        """
        try:
            self.iam_client.untag_policy(
                PolicyArn=policy_arn,
                TagKeys=tag_keys
            )
            logger.info(f"Removed tags from IAM policy: {policy_arn}")
            return True
        except ClientError as e:
            logger.error(f"Error removing tags from IAM policy {policy_arn}: {e}")
            raise
            
    def validate_policy(self, policy_document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate an IAM policy document.
        
        Args:
            policy_document: The policy document to validate.
            
        Returns:
            The validation results as a dictionary.
            
        Raises:
            ClientError: If the policy cannot be validated.
        """
        try:
            response = self.iam_client.validate_policy(
                PolicyDocument=json.dumps(policy_document),
                PolicyType='Identity'  # or 'Resource' or 'AssumeRole'
            )
            return response['ValidationResults']
        except ClientError as e:
            logger.error(f"Error validating IAM policy: {e}")
            raise
