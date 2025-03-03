# Lambda Execution Role Implementation Guide

This document provides a detailed explanation of the Lambda execution role implementation in the `aws_iam_management` package.

## Overview

Lambda execution roles are IAM roles that grant Lambda functions permission to access AWS services and resources. The `aws_iam_management` package provides several ways to create and manage Lambda execution roles, from basic roles with minimal permissions to enhanced roles with access to multiple AWS services.

## Basic Lambda Execution Role

A basic Lambda execution role grants the Lambda function permission to write logs to CloudWatch Logs, which is the minimum permission required for a Lambda function to operate properly.

### Implementation

The basic Lambda execution role is implemented in the `RoleTemplates` class in the `templates.py` module:

```python
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
```

The basic Lambda execution permission set is defined in the `PermissionSets` class in the `permission_sets.py` module:

```python
@staticmethod
def basic_lambda_execution() -> Dict[str, Any]:
    """
    Get the basic Lambda execution permission set.
    
    This permission set allows Lambda functions to write logs to CloudWatch Logs.
    
    Returns:
        A policy document as a dictionary.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }
```

### Usage

To create a basic Lambda execution role:

```python
from aws_iam_management.core.templates import RoleTemplates

templates = RoleTemplates()
role = templates.create_lambda_execution_role(
    role_name="my-lambda-function-role",
    description="Role for my Lambda function"
)
```

## Enhanced Lambda Execution Role

An enhanced Lambda execution role grants the Lambda function permission to access multiple AWS services, such as S3, DynamoDB, SQS, SNS, KMS, VPC, and X-Ray.

### Implementation

The enhanced Lambda execution role is implemented in the `RoleTemplates` class in the `templates.py` module:

```python
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
```

### Usage

To create an enhanced Lambda execution role:

```python
from aws_iam_management.core.templates import RoleTemplates

templates = RoleTemplates()
role = templates.create_enhanced_lambda_role(
    role_name="my-complex-lambda-function-role",
    description="Role for my complex Lambda function",
    s3_access=True,
    dynamodb_access=True,
    sqs_access=True,
    vpc_access=True
)
```

## Custom Lambda Execution Role

A custom Lambda execution role allows you to define your own trust policy and permissions for a Lambda function.

### Implementation

The custom Lambda execution role is implemented in the `RoleTemplates` class in the `templates.py` module:

```python
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
```

### Usage

To create a custom Lambda execution role:

```python
from aws_iam_management.core.templates import RoleTemplates
from aws_iam_management.core.permission_sets import PermissionSets

templates = RoleTemplates()
role = templates.create_custom_role(
    role_name="my-custom-lambda-function-role",
    trust_policy={
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
    },
    description="Custom role for my Lambda function",
    permissions=[
        PermissionSets.basic_lambda_execution(),
        PermissionSets.s3_read_only(bucket_name="my-bucket"),
        PermissionSets.dynamodb_read_only(table_name="my-table")
    ]
)
```

## Lambda Execution Role Example

The `lambda_execution_role_example.py` module provides a more comprehensive example of creating Lambda execution roles with specific permissions for various AWS services.

### Implementation

The `create_lambda_execution_role` function in the `lambda_execution_role_example.py` module:

```python
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
```

### Usage

To create a Lambda execution role with specific permissions:

```python
from aws_iam_management.examples.lambda_execution_role_example import create_lambda_execution_role

role = create_lambda_execution_role(
    role_name="my-lambda-function-role",
    s3_buckets=["my-bucket"],
    dynamodb_tables=["my-table"],
    vpc_access=True,
    xray_tracing=True,
    environment="production",
    application="my-app",
    owner="my-team"
)
```

## Conclusion

The Lambda execution role implementation in the `aws_iam_management` package provides a flexible and secure way to create and manage IAM roles for Lambda functions.
