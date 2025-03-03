"""
IAM Role Templates Module

This module provides templates for creating common IAM roles.
These templates follow best practices for IAM role creation.
"""

import json
import logging
from typing import Dict, List, Optional, Any, Union

import boto3
from botocore.exceptions import ClientError

from aws_iam_management.core.role_manager import RoleManager
from aws_iam_management.core.policy_manager import PolicyManager
from aws_iam_management.core.permission_sets import PermissionSets

logger = logging.getLogger(__name__)

class RoleTemplates:
    """
    A class for creating common IAM roles using templates.
    
    This class provides methods for creating IAM roles for common use cases,
    such as Lambda execution roles, EC2 instance roles, and custom roles.
    """
    
    def __init__(self, session: Optional[boto3.Session] = None):
        """
        Initialize the RoleTemplates with an optional boto3 session.
        
        Args:
            session: An optional boto3 Session object. If not provided, a new session will be created.
        """
        self.session = session or boto3.Session()
        self.role_manager = RoleManager(session=self.session)
        self.policy_manager = PolicyManager(session=self.session)
        
    def create_lambda_execution_role(
        self,
        role_name: str,
        description: str = "Lambda execution role",
        path: str = "/service-role/",
        permissions: List[Dict[str, Any]] = None,
        managed_policy_arns: List[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        max_session_duration: int = 3600
    ) -> Dict[str, Any]:
        """
        Create a Lambda execution role.
        
        Args:
            role_name: The name of the role to create.
            description: A description of the role.
            path: The path to the role.
            permissions: A list of permission sets to include in the role.
            managed_policy_arns: A list of managed policy ARNs to attach to the role.
            tags: A list of tags to attach to the role.
            max_session_duration: The maximum session duration (in seconds) for the role.
            
        Returns:
            The newly created role as a dictionary.
            
        Raises:
            ClientError: If the role cannot be created.
        """
        # Define the trust policy for Lambda
        trust_policy = {
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
        
        # Create standard tags if not provided
        if tags is None:
            tags = [
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "LambdaExecution"
                }
            ]
        
        # Create the role
        role = self.role_manager.create_role(
            role_name=role_name,
            trust_policy=trust_policy,
            description=description,
            path=path,
            tags=tags,
            max_session_duration=max_session_duration
        )
        
        # Attach the basic Lambda execution policy if no permissions are provided
        if permissions is None:
            permissions = [PermissionSets.basic_lambda_execution()]
        
        # Create and attach inline policies for each permission set
        for i, permission in enumerate(permissions):
            policy_name = f"{role_name}-policy-{i+1}"
            self.role_manager.put_role_policy(
                role_name=role_name,
                policy_name=policy_name,
                policy_document=permission
            )
        
        # Attach managed policies if provided
        if managed_policy_arns:
            for policy_arn in managed_policy_arns:
                self.role_manager.attach_policy(
                    role_name=role_name,
                    policy_arn=policy_arn
                )
        
        logger.info(f"Created Lambda execution role: {role_name}")
        return role
        
    def create_ec2_instance_role(
        self,
        role_name: str,
        description: str = "EC2 instance role",
        path: str = "/service-role/",
        permissions: List[Dict[str, Any]] = None,
        managed_policy_arns: List[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        max_session_duration: int = 3600
    ) -> Dict[str, Any]:
        """
        Create an EC2 instance role.
        
        Args:
            role_name: The name of the role to create.
            description: A description of the role.
            path: The path to the role.
            permissions: A list of permission sets to include in the role.
            managed_policy_arns: A list of managed policy ARNs to attach to the role.
            tags: A list of tags to attach to the role.
            max_session_duration: The maximum session duration (in seconds) for the role.
            
        Returns:
            The newly created role as a dictionary.
            
        Raises:
            ClientError: If the role cannot be created.
        """
        # Define the trust policy for EC2
        trust_policy = {
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
        }
        
        # Create standard tags if not provided
        if tags is None:
            tags = [
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "EC2Instance"
                }
            ]
        
        # Create the role
        role = self.role_manager.create_role(
            role_name=role_name,
            trust_policy=trust_policy,
            description=description,
            path=path,
            tags=tags,
            max_session_duration=max_session_duration
        )
        
        # Create and attach inline policies for each permission set
        if permissions:
            for i, permission in enumerate(permissions):
                policy_name = f"{role_name}-policy-{i+1}"
                self.role_manager.put_role_policy(
                    role_name=role_name,
                    policy_name=policy_name,
                    policy_document=permission
                )
        
        # Attach managed policies if provided
        if managed_policy_arns:
            for policy_arn in managed_policy_arns:
                self.role_manager.attach_policy(
                    role_name=role_name,
                    policy_arn=policy_arn
                )
        
        logger.info(f"Created EC2 instance role: {role_name}")
        return role
        
    def create_custom_role(
        self,
        role_name: str,
        trust_policy: Dict[str, Any],
        description: str = "Custom role",
        path: str = "/",
        permissions: List[Dict[str, Any]] = None,
        managed_policy_arns: List[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        max_session_duration: int = 3600
    ) -> Dict[str, Any]:
        """
        Create a custom IAM role.
        
        Args:
            role_name: The name of the role to create.
            trust_policy: The trust policy that grants an entity permission to assume the role.
            description: A description of the role.
            path: The path to the role.
            permissions: A list of permission sets to include in the role.
            managed_policy_arns: A list of managed policy ARNs to attach to the role.
            tags: A list of tags to attach to the role.
            max_session_duration: The maximum session duration (in seconds) for the role.
            
        Returns:
            The newly created role as a dictionary.
            
        Raises:
            ClientError: If the role cannot be created.
        """
        # Create standard tags if not provided
        if tags is None:
            tags = [
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "Custom"
                }
            ]
        
        # Create the role
        role = self.role_manager.create_role(
            role_name=role_name,
            trust_policy=trust_policy,
            description=description,
            path=path,
            tags=tags,
            max_session_duration=max_session_duration
        )
        
        # Create and attach inline policies for each permission set
        if permissions:
            for i, permission in enumerate(permissions):
                policy_name = f"{role_name}-policy-{i+1}"
                self.role_manager.put_role_policy(
                    role_name=role_name,
                    policy_name=policy_name,
                    policy_document=permission
                )
        
        # Attach managed policies if provided
        if managed_policy_arns:
            for policy_arn in managed_policy_arns:
                self.role_manager.attach_policy(
                    role_name=role_name,
                    policy_arn=policy_arn
                )
        
        logger.info(f"Created custom role: {role_name}")
        return role
        
    def create_enhanced_lambda_role(
        self,
        role_name: str,
        description: str = "Enhanced Lambda execution role",
        path: str = "/service-role/",
        s3_access: bool = False,
        dynamodb_access: bool = False,
        sqs_access: bool = False,
        sns_access: bool = False,
        kms_access: bool = False,
        vpc_access: bool = False,
        xray_access: bool = False,
        additional_permissions: List[Dict[str, Any]] = None,
        managed_policy_arns: List[str] = None,
        tags: Optional[List[Dict[str, str]]] = None,
        max_session_duration: int = 3600
    ) -> Dict[str, Any]:
        """
        Create an enhanced Lambda execution role with access to multiple AWS services.
        
        Args:
            role_name: The name of the role to create.
            description: A description of the role.
            path: The path to the role.
            s3_access: Whether to include S3 access permissions.
            dynamodb_access: Whether to include DynamoDB access permissions.
            sqs_access: Whether to include SQS access permissions.
            sns_access: Whether to include SNS access permissions.
            kms_access: Whether to include KMS access permissions.
            vpc_access: Whether to include VPC access permissions.
            xray_access: Whether to include X-Ray access permissions.
            additional_permissions: Additional permission sets to include in the role.
            managed_policy_arns: A list of managed policy ARNs to attach to the role.
            tags: A list of tags to attach to the role.
            max_session_duration: The maximum session duration (in seconds) for the role.
            
        Returns:
            The newly created role as a dictionary.
            
        Raises:
            ClientError: If the role cannot be created.
        """
        # Define the trust policy for Lambda
        trust_policy = {
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
        
        # Create standard tags if not provided
        if tags is None:
            tags = [
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "EnhancedLambdaExecution"
                }
            ]
        
        # Create the role
        role = self.role_manager.create_role(
            role_name=role_name,
            trust_policy=trust_policy,
            description=description,
            path=path,
            tags=tags,
            max_session_duration=max_session_duration
        )
        
        # Start with basic Lambda execution permissions
        permissions = [PermissionSets.basic_lambda_execution()]
        
        # Add service-specific permissions based on flags
        if s3_access:
            permissions.append(PermissionSets.s3_read_write())
        
        if dynamodb_access:
            permissions.append(PermissionSets.dynamodb_read_write())
        
        if sqs_access:
            permissions.append(PermissionSets.sqs_consumer())
            permissions.append(PermissionSets.sqs_producer())
        
        if sns_access:
            permissions.append(PermissionSets.sns_publisher())
        
        if kms_access:
            permissions.append(PermissionSets.kms_encrypt_decrypt())
        
        if vpc_access:
            permissions.append(PermissionSets.vpc_execution())
        
        if xray_access:
            permissions.append(PermissionSets.xray_write())
        
        # Add additional permissions if provided
        if additional_permissions:
            permissions.extend(additional_permissions)
        
        # Create and attach inline policies for each permission set
        for i, permission in enumerate(permissions):
            policy_name = f"{role_name}-policy-{i+1}"
            self.role_manager.put_role_policy(
                role_name=role_name,
                policy_name=policy_name,
                policy_document=permission
            )
        
        # Attach managed policies if provided
        if managed_policy_arns:
            for policy_arn in managed_policy_arns:
                self.role_manager.attach_policy(
                    role_name=role_name,
                    policy_arn=policy_arn
                )
        
        logger.info(f"Created enhanced Lambda execution role: {role_name}")
        return role
