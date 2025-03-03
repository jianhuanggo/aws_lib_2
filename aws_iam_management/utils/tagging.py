"""
IAM Tagging Utilities Module

This module provides utilities for tagging IAM resources.
It helps implement a consistent tagging strategy for IAM roles and policies.
"""

import logging
from typing import Dict, List, Optional, Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class TaggingUtils:
    """
    A class for tagging IAM resources.
    
    This class provides methods for applying standard tags to IAM roles and policies,
    and for retrieving resources by tag.
    """
    
    def __init__(self, session: Optional[boto3.Session] = None):
        """
        Initialize the TaggingUtils with an optional boto3 session.
        
        Args:
            session: An optional boto3 Session object. If not provided, a new session will be created.
        """
        self.session = session or boto3.Session()
        self.iam_client = self.session.client('iam')
        
    def apply_standard_tags(
        self, 
        resource_arn: str, 
        resource_type: str,
        environment: str,
        application: str,
        owner: str,
        additional_tags: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, str]]:
        """
        Apply standard tags to an IAM resource.
        
        Args:
            resource_arn: The ARN of the resource to tag.
            resource_type: The type of the resource (e.g., 'role', 'policy').
            environment: The environment (e.g., 'dev', 'test', 'prod').
            application: The application or service that the resource belongs to.
            owner: The owner of the resource (e.g., team name, email).
            additional_tags: Additional tags to apply to the resource.
            
        Returns:
            The list of tags applied to the resource.
            
        Raises:
            ClientError: If the tags cannot be applied.
        """
        # Define standard tags
        tags = [
            {
                "Key": "Environment",
                "Value": environment
            },
            {
                "Key": "Application",
                "Value": application
            },
            {
                "Key": "Owner",
                "Value": owner
            },
            {
                "Key": "ManagedBy",
                "Value": "aws_iam_management"
            }
        ]
        
        # Add additional tags if provided
        if additional_tags:
            for key, value in additional_tags.items():
                tags.append({
                    "Key": key,
                    "Value": value
                })
        
        try:
            # Apply tags based on resource type
            if resource_type.lower() == 'role':
                role_name = resource_arn.split('/')[-1]
                self.iam_client.tag_role(
                    RoleName=role_name,
                    Tags=tags
                )
            elif resource_type.lower() == 'policy':
                self.iam_client.tag_policy(
                    PolicyArn=resource_arn,
                    Tags=tags
                )
            else:
                logger.warning(f"Unsupported resource type for tagging: {resource_type}")
                return []
            
            logger.info(f"Applied standard tags to {resource_type} {resource_arn}")
            return tags
        except ClientError as e:
            logger.error(f"Error applying tags to {resource_type} {resource_arn}: {e}")
            raise
            
    def get_resources_by_tag(
        self, 
        resource_type: str,
        tag_key: str,
        tag_value: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get IAM resources by tag.
        
        Args:
            resource_type: The type of the resource (e.g., 'role', 'policy').
            tag_key: The tag key to filter by.
            tag_value: The tag value to filter by. If not provided, all resources with the tag key are returned.
            
        Returns:
            A list of resources that match the tag criteria.
            
        Raises:
            ClientError: If the resources cannot be retrieved.
        """
        try:
            resources = []
            
            # Get resources based on resource type
            if resource_type.lower() == 'role':
                response = self.iam_client.list_roles()
                for role in response.get('Roles', []):
                    role_name = role['RoleName']
                    
                    # Get tags for the role
                    tag_response = self.iam_client.list_role_tags(RoleName=role_name)
                    tags = tag_response.get('Tags', [])
                    
                    # Check if the role has the specified tag
                    for tag in tags:
                        if tag['Key'] == tag_key and (tag_value is None or tag['Value'] == tag_value):
                            resources.append(role)
                            break
            elif resource_type.lower() == 'policy':
                response = self.iam_client.list_policies(Scope='Local')
                for policy in response.get('Policies', []):
                    policy_arn = policy['Arn']
                    
                    # Get tags for the policy
                    tag_response = self.iam_client.list_policy_tags(PolicyArn=policy_arn)
                    tags = tag_response.get('Tags', [])
                    
                    # Check if the policy has the specified tag
                    for tag in tags:
                        if tag['Key'] == tag_key and (tag_value is None or tag['Value'] == tag_value):
                            resources.append(policy)
                            break
            else:
                logger.warning(f"Unsupported resource type for tag filtering: {resource_type}")
                return []
            
            logger.info(f"Found {len(resources)} {resource_type}s with tag {tag_key}={tag_value or '*'}")
            return resources
        except ClientError as e:
            logger.error(f"Error getting {resource_type}s by tag: {e}")
            raise
            
    def get_all_tagged_resources(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all IAM resources with tags.
        
        Returns:
            A dictionary with 'roles' and 'policies' keys, each containing a list of resources with their tags.
            
        Raises:
            ClientError: If the resources cannot be retrieved.
        """
        try:
            tagged_resources = {
                'roles': [],
                'policies': []
            }
            
            # Get roles with tags
            response = self.iam_client.list_roles()
            for role in response.get('Roles', []):
                role_name = role['RoleName']
                
                # Get tags for the role
                tag_response = self.iam_client.list_role_tags(RoleName=role_name)
                tags = tag_response.get('Tags', [])
                
                if tags:
                    role['Tags'] = tags
                    tagged_resources['roles'].append(role)
            
            # Get policies with tags
            response = self.iam_client.list_policies(Scope='Local')
            for policy in response.get('Policies', []):
                policy_arn = policy['Arn']
                
                # Get tags for the policy
                tag_response = self.iam_client.list_policy_tags(PolicyArn=policy_arn)
                tags = tag_response.get('Tags', [])
                
                if tags:
                    policy['Tags'] = tags
                    tagged_resources['policies'].append(policy)
            
            logger.info(f"Found {len(tagged_resources['roles'])} tagged roles and {len(tagged_resources['policies'])} tagged policies")
            return tagged_resources
        except ClientError as e:
            logger.error(f"Error getting tagged resources: {e}")
            raise
