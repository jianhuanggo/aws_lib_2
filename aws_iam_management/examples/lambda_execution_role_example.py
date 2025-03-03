"""
Lambda Execution Role Example

This module demonstrates how to create a production-grade IAM role for Lambda functions.
It shows best practices for creating Lambda execution roles with appropriate permissions.
"""

import boto3
import logging
import json
from typing import Dict, List, Any, Optional

from aws_iam_management.core.role_manager import RoleManager
from aws_iam_management.core.policy_manager import PolicyManager
from aws_iam_management.core.permission_sets import PermissionSets
from aws_iam_management.core.templates import RoleTemplates
from aws_iam_management.utils.validation import ValidationUtils
from aws_iam_management.utils.tagging import TaggingUtils
from aws_iam_management.utils.logging import LoggingUtils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_lambda_execution_role(
    role_name: str,
    s3_buckets: Optional[List[str]] = None,
    dynamodb_tables: Optional[List[str]] = None,
    sqs_queues: Optional[List[str]] = None,
    sns_topics: Optional[List[str]] = None,
    kms_keys: Optional[List[str]] = None,
    vpc_access: bool = False,
    xray_tracing: bool = False,
    environment: str = "production",
    application: str = "lambda-app",
    owner: str = "platform-team",
    session: Optional[boto3.Session] = None
) -> Dict[str, Any]:
    """
    Create a production-grade Lambda execution role with appropriate permissions.
    
    This function creates an IAM role that can be attached to Lambda functions,
    with permissions tailored to the specific needs of the function.
    
    Args:
        role_name: The name of the role to create.
        s3_buckets: A list of S3 bucket names that the Lambda function needs access to.
        dynamodb_tables: A list of DynamoDB table names that the Lambda function needs access to.
        sqs_queues: A list of SQS queue names that the Lambda function needs access to.
        sns_topics: A list of SNS topic names that the Lambda function needs access to.
        kms_keys: A list of KMS key IDs that the Lambda function needs access to.
        vpc_access: Whether the Lambda function needs access to VPC resources.
        xray_tracing: Whether the Lambda function uses X-Ray tracing.
        environment: The environment (e.g., 'dev', 'test', 'production').
        application: The application or service that the Lambda function belongs to.
        owner: The owner of the Lambda function (e.g., team name, email).
        session: An optional boto3 Session object. If not provided, a new session will be created.
        
    Returns:
        The newly created role as a dictionary.
        
    Raises:
        Exception: If the role cannot be created.
    """
    try:
        # Initialize the session and clients
        session = session or boto3.Session()
        role_manager = RoleManager(session=session)
        policy_manager = PolicyManager(session=session)
        validation = ValidationUtils()
        tagging = TaggingUtils(session=session)
        logging_utils = LoggingUtils(session=session)
        
        # Create standard tags
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
            },
            {
                "Key": "RoleType",
                "Value": "LambdaExecution"
            }
        ]
        
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
        
        # Validate the trust policy
        trust_policy_findings = validation.validate_trust_relationship(trust_policy)
        if trust_policy_findings:
            logger.warning(f"Trust policy has {len(trust_policy_findings)} validation findings:")
            for finding in trust_policy_findings:
                logger.warning(f"  {finding['severity']}: {finding['message']} at {finding['location']}")
        
        # Create the role
        role = role_manager.create_role(
            role_name=role_name,
            trust_policy=trust_policy,
            description=f"Lambda execution role for {application} in {environment}",
            path="/service-role/",
            tags=tags,
            max_session_duration=3600
        )
        
        # Start with basic Lambda execution permissions (CloudWatch Logs)
        permissions = [PermissionSets.basic_lambda_execution()]
        
        # Add S3 permissions if needed
        if s3_buckets:
            for bucket in s3_buckets:
                permissions.append(PermissionSets.s3_read_write(bucket_name=bucket))
        
        # Add DynamoDB permissions if needed
        if dynamodb_tables:
            for table in dynamodb_tables:
                permissions.append(PermissionSets.dynamodb_read_write(table_name=table))
        
        # Add SQS permissions if needed
        if sqs_queues:
            for queue in sqs_queues:
                queue_arn = f"arn:aws:sqs:*:*:{queue}"
                permissions.append(PermissionSets.sqs_consumer(queue_arn=queue_arn))
                permissions.append(PermissionSets.sqs_producer(queue_arn=queue_arn))
        
        # Add SNS permissions if needed
        if sns_topics:
            for topic in sns_topics:
                topic_arn = f"arn:aws:sns:*:*:{topic}"
                permissions.append(PermissionSets.sns_publisher(topic_arn=topic_arn))
        
        # Add KMS permissions if needed
        if kms_keys:
            for key_id in kms_keys:
                permissions.append(PermissionSets.kms_encrypt_decrypt(key_id=key_id))
        
        # Add VPC permissions if needed
        if vpc_access:
            permissions.append(PermissionSets.vpc_execution())
        
        # Add X-Ray permissions if needed
        if xray_tracing:
            permissions.append(PermissionSets.xray_write())
        
        # Validate all permission sets
        for i, permission in enumerate(permissions):
            findings = validation.validate_policy_least_privilege(permission)
            if findings:
                logger.warning(f"Permission set {i+1} has {len(findings)} validation findings:")
                for finding in findings:
                    logger.warning(f"  {finding['severity']}: {finding['message']} at {finding['location']}")
        
        # Create and attach inline policies for each permission set
        for i, permission in enumerate(permissions):
            policy_name = f"{role_name}-policy-{i+1}"
            role_manager.put_role_policy(
                role_name=role_name,
                policy_name=policy_name,
                policy_document=permission
            )
        
        # Log the role creation
        logging_utils.log_role_change(
            action="create",
            role_name=role_name,
            user="aws_iam_management",
            details={
                "s3_buckets": s3_buckets,
                "dynamodb_tables": dynamodb_tables,
                "sqs_queues": sqs_queues,
                "sns_topics": sns_topics,
                "kms_keys": kms_keys,
                "vpc_access": vpc_access,
                "xray_tracing": xray_tracing,
                "environment": environment,
                "application": application,
                "owner": owner
            }
        )
        
        logger.info(f"Created Lambda execution role: {role_name}")
        return role
    except Exception as e:
        logger.error(f"Error creating Lambda execution role: {e}")
        raise

def create_lambda_execution_role_with_templates(
    role_name: str,
    s3_access: bool = False,
    dynamodb_access: bool = False,
    sqs_access: bool = False,
    sns_access: bool = False,
    kms_access: bool = False,
    vpc_access: bool = False,
    xray_access: bool = False,
    environment: str = "production",
    application: str = "lambda-app",
    owner: str = "platform-team",
    session: Optional[boto3.Session] = None
) -> Dict[str, Any]:
    """
    Create a Lambda execution role using the RoleTemplates class.
    
    This function demonstrates how to use the RoleTemplates class to create
    a Lambda execution role with appropriate permissions.
    
    Args:
        role_name: The name of the role to create.
        s3_access: Whether the Lambda function needs access to S3.
        dynamodb_access: Whether the Lambda function needs access to DynamoDB.
        sqs_access: Whether the Lambda function needs access to SQS.
        sns_access: Whether the Lambda function needs access to SNS.
        kms_access: Whether the Lambda function needs access to KMS.
        vpc_access: Whether the Lambda function needs access to VPC resources.
        xray_access: Whether the Lambda function uses X-Ray tracing.
        environment: The environment (e.g., 'dev', 'test', 'production').
        application: The application or service that the Lambda function belongs to.
        owner: The owner of the Lambda function (e.g., team name, email).
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
        logging_utils = LoggingUtils(session=session)
        
        # Create standard tags
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
            },
            {
                "Key": "RoleType",
                "Value": "LambdaExecution"
            }
        ]
        
        # Create the role using the template
        role = templates.create_enhanced_lambda_role(
            role_name=role_name,
            description=f"Lambda execution role for {application} in {environment}",
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
        logging_utils.log_role_change(
            action="create",
            role_name=role_name,
            user="aws_iam_management",
            details={
                "s3_access": s3_access,
                "dynamodb_access": dynamodb_access,
                "sqs_access": sqs_access,
                "sns_access": sns_access,
                "kms_access": kms_access,
                "vpc_access": vpc_access,
                "xray_access": xray_access,
                "environment": environment,
                "application": application,
                "owner": owner
            }
        )
        
        logger.info(f"Created Lambda execution role using templates: {role_name}")
        return role
    except Exception as e:
        logger.error(f"Error creating Lambda execution role with templates: {e}")
        raise

def main():
    """
    Main function to demonstrate the creation of Lambda execution roles.
    """
    # Example 1: Create a basic Lambda execution role
    basic_role_name = "basic-lambda-execution-role"
    basic_role = create_lambda_execution_role(
        role_name=basic_role_name,
        environment="dev",
        application="example-app",
        owner="example-team"
    )
    print(f"Created basic Lambda execution role: {basic_role['RoleName']}")
    
    # Example 2: Create a Lambda execution role with S3 and DynamoDB access
    s3_dynamodb_role_name = "s3-dynamodb-lambda-role"
    s3_dynamodb_role = create_lambda_execution_role(
        role_name=s3_dynamodb_role_name,
        s3_buckets=["my-app-bucket", "my-logs-bucket"],
        dynamodb_tables=["my-app-table"],
        environment="dev",
        application="example-app",
        owner="example-team"
    )
    print(f"Created Lambda execution role with S3 and DynamoDB access: {s3_dynamodb_role['RoleName']}")
    
    # Example 3: Create a Lambda execution role with VPC access and X-Ray tracing
    vpc_xray_role_name = "vpc-xray-lambda-role"
    vpc_xray_role = create_lambda_execution_role(
        role_name=vpc_xray_role_name,
        vpc_access=True,
        xray_tracing=True,
        environment="dev",
        application="example-app",
        owner="example-team"
    )
    print(f"Created Lambda execution role with VPC access and X-Ray tracing: {vpc_xray_role['RoleName']}")
    
    # Example 4: Create a Lambda execution role using templates
    template_role_name = "template-lambda-role"
    template_role = create_lambda_execution_role_with_templates(
        role_name=template_role_name,
        s3_access=True,
        dynamodb_access=True,
        vpc_access=True,
        environment="dev",
        application="example-app",
        owner="example-team"
    )
    print(f"Created Lambda execution role using templates: {template_role['RoleName']}")

if __name__ == "__main__":
    main()
