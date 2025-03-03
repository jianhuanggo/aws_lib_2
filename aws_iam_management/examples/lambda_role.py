"""
Lambda Role Example Module

This module provides an example of how to create an IAM role for Lambda functions.
It demonstrates best practices for creating Lambda execution roles.
"""

import logging
import boto3
from typing import Dict, List, Optional, Any

from aws_iam_management.core.role_manager import RoleManager
from aws_iam_management.core.policy_manager import PolicyManager
from aws_iam_management.core.permission_sets import PermissionSets
from aws_iam_management.core.templates import RoleTemplates
from aws_iam_management.utils.validation import ValidationUtils
from aws_iam_management.utils.tagging import TaggingUtils
from aws_iam_management.utils.logging import LoggingUtils

logger = logging.getLogger(__name__)

def create_basic_lambda_role(
    role_name: str,
    description: str = "Basic Lambda execution role",
    tags: Optional[List[Dict[str, str]]] = None,
    session: Optional[boto3.Session] = None
) -> Dict[str, Any]:
    """
    Create a basic Lambda execution role.
    
    This role allows Lambda functions to write logs to CloudWatch Logs.
    
    Args:
        role_name: The name of the role to create.
        description: A description of the role.
        tags: A list of tags to attach to the role.
        session: An optional boto3 Session object. If not provided, a new session will be created.
        
    Returns:
        The newly created role as a dictionary.
        
    Raises:
        Exception: If the role cannot be created.
    """
    try:
        # Initialize the session and clients
        session = session or boto3.Session()
        templates = RoleTemplates(session=session)
        
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
                },
                {
                    "Key": "Environment",
                    "Value": "production"
                }
            ]
        
        # Create the role using the template
        role = templates.create_lambda_execution_role(
            role_name=role_name,
            description=description,
            tags=tags
        )
        
        # Log the role creation
        logging_utils = LoggingUtils(session=session)
        logging_utils.log_role_change(
            action="create",
            role_name=role_name,
            user="aws_iam_management",
            details={
                "description": description,
                "tags": tags
            }
        )
        
        logger.info(f"Created basic Lambda execution role: {role_name}")
        return role
    except Exception as e:
        logger.error(f"Error creating basic Lambda execution role: {e}")
        raise

def create_enhanced_lambda_role(
    role_name: str,
    description: str = "Enhanced Lambda execution role",
    s3_access: bool = False,
    dynamodb_access: bool = False,
    sqs_access: bool = False,
    sns_access: bool = False,
    kms_access: bool = False,
    vpc_access: bool = False,
    xray_access: bool = False,
    tags: Optional[List[Dict[str, str]]] = None,
    session: Optional[boto3.Session] = None
) -> Dict[str, Any]:
    """
    Create an enhanced Lambda execution role with access to multiple AWS services.
    
    Args:
        role_name: The name of the role to create.
        description: A description of the role.
        s3_access: Whether to include S3 access permissions.
        dynamodb_access: Whether to include DynamoDB access permissions.
        sqs_access: Whether to include SQS access permissions.
        sns_access: Whether to include SNS access permissions.
        kms_access: Whether to include KMS access permissions.
        vpc_access: Whether to include VPC access permissions.
        xray_access: Whether to include X-Ray access permissions.
        tags: A list of tags to attach to the role.
        session: An optional boto3 Session object. If not provided, a new session will be created.
        
    Returns:
        The newly created role as a dictionary.
        
    Raises:
        Exception: If the role cannot be created.
    """
    try:
        # Initialize the session and clients
        session = session or boto3.Session()
        templates = RoleTemplates(session=session)
        
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
                },
                {
                    "Key": "Environment",
                    "Value": "production"
                }
            ]
        
        # Create the role using the template
        role = templates.create_enhanced_lambda_role(
            role_name=role_name,
            description=description,
            s3_access=s3_access,
            dynamodb_access=dynamodb_access,
            sqs_access=sqs_access,
            sns_access=sns_access,
            kms_access=kms_access,
            vpc_access=vpc_access,
            xray_access=xray_access,
            tags=tags
        )
        
        # Log the role creation
        logging_utils = LoggingUtils(session=session)
        logging_utils.log_role_change(
            action="create",
            role_name=role_name,
            user="aws_iam_management",
            details={
                "description": description,
                "s3_access": s3_access,
                "dynamodb_access": dynamodb_access,
                "sqs_access": sqs_access,
                "sns_access": sns_access,
                "kms_access": kms_access,
                "vpc_access": vpc_access,
                "xray_access": xray_access,
                "tags": tags
            }
        )
        
        logger.info(f"Created enhanced Lambda execution role: {role_name}")
        return role
    except Exception as e:
        logger.error(f"Error creating enhanced Lambda execution role: {e}")
        raise

def create_custom_lambda_role(
    role_name: str,
    description: str = "Custom Lambda execution role",
    permissions: List[Dict[str, Any]] = None,
    managed_policy_arns: List[str] = None,
    tags: Optional[List[Dict[str, str]]] = None,
    session: Optional[boto3.Session] = None
) -> Dict[str, Any]:
    """
    Create a custom Lambda execution role with specific permissions.
    
    Args:
        role_name: The name of the role to create.
        description: A description of the role.
        permissions: A list of permission sets to include in the role.
        managed_policy_arns: A list of managed policy ARNs to attach to the role.
        tags: A list of tags to attach to the role.
        session: An optional boto3 Session object. If not provided, a new session will be created.
        
    Returns:
        The newly created role as a dictionary.
        
    Raises:
        Exception: If the role cannot be created.
    """
    try:
        # Initialize the session and clients
        session = session or boto3.Session()
        templates = RoleTemplates(session=session)
        validation = ValidationUtils()
        
        # Create standard tags if not provided
        if tags is None:
            tags = [
                {
                    "Key": "ManagedBy",
                    "Value": "aws_iam_management"
                },
                {
                    "Key": "RoleType",
                    "Value": "CustomLambdaExecution"
                },
                {
                    "Key": "Environment",
                    "Value": "production"
                }
            ]
        
        # Validate permissions
        if permissions:
            for i, permission in enumerate(permissions):
                findings = validation.validate_policy_least_privilege(permission)
                if findings:
                    logger.warning(f"Policy {i+1} has {len(findings)} validation findings:")
                    for finding in findings:
                        logger.warning(f"  {finding['severity']}: {finding['message']} at {finding['location']}")
        
        # Create the role using the template
        role = templates.create_lambda_execution_role(
            role_name=role_name,
            description=description,
            permissions=permissions,
            managed_policy_arns=managed_policy_arns,
            tags=tags
        )
        
        # Log the role creation
        logging_utils = LoggingUtils(session=session)
        logging_utils.log_role_change(
            action="create",
            role_name=role_name,
            user="aws_iam_management",
            details={
                "description": description,
                "permissions": permissions,
                "managed_policy_arns": managed_policy_arns,
                "tags": tags
            }
        )
        
        logger.info(f"Created custom Lambda execution role: {role_name}")
        return role
    except Exception as e:
        logger.error(f"Error creating custom Lambda execution role: {e}")
        raise

def main():
    """
    Main function to demonstrate the creation of Lambda execution roles.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Create a basic Lambda execution role
    basic_role = create_basic_lambda_role(
        role_name="basic-lambda-execution-role",
        description="Basic Lambda execution role for demonstration"
    )
    print(f"Created basic Lambda execution role: {basic_role['RoleName']}")
    
    # Create an enhanced Lambda execution role with S3 and DynamoDB access
    enhanced_role = create_enhanced_lambda_role(
        role_name="enhanced-lambda-execution-role",
        description="Enhanced Lambda execution role for demonstration",
        s3_access=True,
        dynamodb_access=True
    )
    print(f"Created enhanced Lambda execution role: {enhanced_role['RoleName']}")
    
    # Create a custom Lambda execution role with specific permissions
    custom_permissions = [
        PermissionSets.basic_lambda_execution(),
        PermissionSets.s3_read_only(bucket_name="my-specific-bucket"),
        PermissionSets.dynamodb_read_only(table_name="my-specific-table")
    ]
    custom_role = create_custom_lambda_role(
        role_name="custom-lambda-execution-role",
        description="Custom Lambda execution role for demonstration",
        permissions=custom_permissions
    )
    print(f"Created custom Lambda execution role: {custom_role['RoleName']}")

if __name__ == "__main__":
    main()
