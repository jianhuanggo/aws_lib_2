"""
IAM Role Manager Module

This module provides a class for managing IAM roles in AWS.
It follows best practices for creating, updating, and deleting IAM roles.
"""

import json
import logging
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Union, Any

logger = logging.getLogger(__name__)

class RoleManager:
    """
    A class for managing IAM roles in AWS.
    
    This class provides methods for creating, updating, and deleting IAM roles,
    as well as attaching and detaching policies from roles.
    """
    
    def __init__(self, session: Optional[boto3.Session] = None):
        """
        Initialize the RoleManager with an optional boto3 session.
        
        Args:
            session: An optional boto3 Session object. If not provided, a new session will be created.
        """
        self.session = session or boto3.Session()
        self.iam_client = self.session.client('iam')
        
    def create_role(
        self, 
        role_name: str, 
        trust_policy: Dict[str, Any], 
        description: str = "",
        max_session_duration: int = 3600,
        path: str = "/",
        permissions_boundary: Optional[str] = None,
        tags: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Create an IAM role with the specified parameters.
        
        Args:
            role_name: The name of the role to create.
            trust_policy: The trust policy that grants an entity permission to assume the role.
            description: A description of the role.
            max_session_duration: The maximum session duration (in seconds) for the role.
            path: The path to the role.
            permissions_boundary: The ARN of the policy that is used to set the permissions boundary for the role.
            tags: A list of tags to attach to the role.
            
        Returns:
            The newly created role as a dictionary.
            
        Raises:
            ClientError: If the role cannot be created.
        """
        try:
            params = {
                'RoleName': role_name,
                'AssumeRolePolicyDocument': json.dumps(trust_policy),
                'Description': description,
                'MaxSessionDuration': max_session_duration,
                'Path': path
            }
            
            if permissions_boundary:
                params['PermissionsBoundary'] = permissions_boundary
                
            if tags:
                params['Tags'] = tags
                
            response = self.iam_client.create_role(**params)
            logger.info(f"Created IAM role: {role_name}")
            return response['Role']
        except ClientError as e:
            logger.error(f"Error creating IAM role {role_name}: {e}")
            raise
            
    def delete_role(self, role_name: str) -> bool:
        """
        Delete an IAM role.
        
        Args:
            role_name: The name of the role to delete.
            
        Returns:
            True if the role was deleted successfully, False otherwise.
            
        Raises:
            ClientError: If the role cannot be deleted.
        """
        try:
            # First, detach all policies from the role
            attached_policies = self.list_attached_role_policies(role_name)
            for policy in attached_policies:
                self.detach_policy(role_name, policy['PolicyArn'])
                
            # Delete any inline policies
            inline_policies = self.list_role_policies(role_name)
            for policy_name in inline_policies:
                self.delete_role_policy(role_name, policy_name)
                
            # Now delete the role
            self.iam_client.delete_role(RoleName=role_name)
            logger.info(f"Deleted IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting IAM role {role_name}: {e}")
            raise
            
    def get_role(self, role_name: str) -> Dict[str, Any]:
        """
        Get information about an IAM role.
        
        Args:
            role_name: The name of the role to get information about.
            
        Returns:
            The role information as a dictionary.
            
        Raises:
            ClientError: If the role cannot be found.
        """
        try:
            response = self.iam_client.get_role(RoleName=role_name)
            return response['Role']
        except ClientError as e:
            logger.error(f"Error getting IAM role {role_name}: {e}")
            raise
            
    def list_roles(self, path_prefix: str = "/", max_items: int = 100) -> List[Dict[str, Any]]:
        """
        List IAM roles.
        
        Args:
            path_prefix: The path prefix for filtering the results.
            max_items: The maximum number of items to return.
            
        Returns:
            A list of IAM roles.
            
        Raises:
            ClientError: If the roles cannot be listed.
        """
        try:
            response = self.iam_client.list_roles(
                PathPrefix=path_prefix,
                MaxItems=max_items
            )
            return response['Roles']
        except ClientError as e:
            logger.error(f"Error listing IAM roles: {e}")
            raise
            
    def update_role(
        self, 
        role_name: str, 
        description: Optional[str] = None,
        max_session_duration: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Update an IAM role.
        
        Args:
            role_name: The name of the role to update.
            description: A new description for the role.
            max_session_duration: The maximum session duration (in seconds) for the role.
            
        Returns:
            The updated role as a dictionary.
            
        Raises:
            ClientError: If the role cannot be updated.
        """
        try:
            params = {'RoleName': role_name}
            
            if description is not None:
                params['Description'] = description
                
            if max_session_duration is not None:
                params['MaxSessionDuration'] = max_session_duration
                
            response = self.iam_client.update_role(**params)
            logger.info(f"Updated IAM role: {role_name}")
            
            # Get the updated role
            return self.get_role(role_name)
        except ClientError as e:
            logger.error(f"Error updating IAM role {role_name}: {e}")
            raise
            
    def update_assume_role_policy(self, role_name: str, trust_policy: Dict[str, Any]) -> bool:
        """
        Update the trust policy of an IAM role.
        
        Args:
            role_name: The name of the role to update.
            trust_policy: The new trust policy.
            
        Returns:
            True if the trust policy was updated successfully, False otherwise.
            
        Raises:
            ClientError: If the trust policy cannot be updated.
        """
        try:
            self.iam_client.update_assume_role_policy(
                RoleName=role_name,
                PolicyDocument=json.dumps(trust_policy)
            )
            logger.info(f"Updated trust policy for IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error updating trust policy for IAM role {role_name}: {e}")
            raise
            
    def attach_policy(self, role_name: str, policy_arn: str) -> bool:
        """
        Attach a managed policy to an IAM role.
        
        Args:
            role_name: The name of the role to attach the policy to.
            policy_arn: The ARN of the policy to attach.
            
        Returns:
            True if the policy was attached successfully, False otherwise.
            
        Raises:
            ClientError: If the policy cannot be attached.
        """
        try:
            self.iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            logger.info(f"Attached policy {policy_arn} to IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error attaching policy {policy_arn} to IAM role {role_name}: {e}")
            raise
            
    def detach_policy(self, role_name: str, policy_arn: str) -> bool:
        """
        Detach a managed policy from an IAM role.
        
        Args:
            role_name: The name of the role to detach the policy from.
            policy_arn: The ARN of the policy to detach.
            
        Returns:
            True if the policy was detached successfully, False otherwise.
            
        Raises:
            ClientError: If the policy cannot be detached.
        """
        try:
            self.iam_client.detach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn
            )
            logger.info(f"Detached policy {policy_arn} from IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error detaching policy {policy_arn} from IAM role {role_name}: {e}")
            raise
            
    def put_role_policy(self, role_name: str, policy_name: str, policy_document: Dict[str, Any]) -> bool:
        """
        Add or update an inline policy for an IAM role.
        
        Args:
            role_name: The name of the role to add the policy to.
            policy_name: The name of the policy.
            policy_document: The policy document.
            
        Returns:
            True if the policy was added or updated successfully, False otherwise.
            
        Raises:
            ClientError: If the policy cannot be added or updated.
        """
        try:
            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(policy_document)
            )
            logger.info(f"Added/updated inline policy {policy_name} for IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error adding/updating inline policy {policy_name} for IAM role {role_name}: {e}")
            raise
            
    def delete_role_policy(self, role_name: str, policy_name: str) -> bool:
        """
        Delete an inline policy from an IAM role.
        
        Args:
            role_name: The name of the role to delete the policy from.
            policy_name: The name of the policy to delete.
            
        Returns:
            True if the policy was deleted successfully, False otherwise.
            
        Raises:
            ClientError: If the policy cannot be deleted.
        """
        try:
            self.iam_client.delete_role_policy(
                RoleName=role_name,
                PolicyName=policy_name
            )
            logger.info(f"Deleted inline policy {policy_name} from IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting inline policy {policy_name} from IAM role {role_name}: {e}")
            raise
            
    def list_attached_role_policies(self, role_name: str) -> List[Dict[str, str]]:
        """
        List the managed policies attached to an IAM role.
        
        Args:
            role_name: The name of the role to list policies for.
            
        Returns:
            A list of attached policies.
            
        Raises:
            ClientError: If the policies cannot be listed.
        """
        try:
            response = self.iam_client.list_attached_role_policies(RoleName=role_name)
            return response['AttachedPolicies']
        except ClientError as e:
            logger.error(f"Error listing attached policies for IAM role {role_name}: {e}")
            raise
            
    def list_role_policies(self, role_name: str) -> List[str]:
        """
        List the names of the inline policies embedded in an IAM role.
        
        Args:
            role_name: The name of the role to list policies for.
            
        Returns:
            A list of policy names.
            
        Raises:
            ClientError: If the policies cannot be listed.
        """
        try:
            response = self.iam_client.list_role_policies(RoleName=role_name)
            return response['PolicyNames']
        except ClientError as e:
            logger.error(f"Error listing inline policies for IAM role {role_name}: {e}")
            raise
            
    def get_role_policy(self, role_name: str, policy_name: str) -> Dict[str, Any]:
        """
        Get information about an inline policy for an IAM role.
        
        Args:
            role_name: The name of the role.
            policy_name: The name of the policy.
            
        Returns:
            The policy document as a dictionary.
            
        Raises:
            ClientError: If the policy cannot be found.
        """
        try:
            response = self.iam_client.get_role_policy(
                RoleName=role_name,
                PolicyName=policy_name
            )
            # Convert the policy document from JSON string to dictionary
            policy_document = json.loads(response['PolicyDocument'])
            return policy_document
        except ClientError as e:
            logger.error(f"Error getting inline policy {policy_name} for IAM role {role_name}: {e}")
            raise
            
    def tag_role(self, role_name: str, tags: List[Dict[str, str]]) -> bool:
        """
        Add tags to an IAM role.
        
        Args:
            role_name: The name of the role to tag.
            tags: A list of tags to add to the role.
            
        Returns:
            True if the tags were added successfully, False otherwise.
            
        Raises:
            ClientError: If the tags cannot be added.
        """
        try:
            self.iam_client.tag_role(
                RoleName=role_name,
                Tags=tags
            )
            logger.info(f"Added tags to IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error adding tags to IAM role {role_name}: {e}")
            raise
            
    def untag_role(self, role_name: str, tag_keys: List[str]) -> bool:
        """
        Remove tags from an IAM role.
        
        Args:
            role_name: The name of the role to remove tags from.
            tag_keys: A list of tag keys to remove.
            
        Returns:
            True if the tags were removed successfully, False otherwise.
            
        Raises:
            ClientError: If the tags cannot be removed.
        """
        try:
            self.iam_client.untag_role(
                RoleName=role_name,
                TagKeys=tag_keys
            )
            logger.info(f"Removed tags from IAM role: {role_name}")
            return True
        except ClientError as e:
            logger.error(f"Error removing tags from IAM role {role_name}: {e}")
            raise
