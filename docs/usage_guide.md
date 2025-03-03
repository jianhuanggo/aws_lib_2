# Usage Guide

This guide provides examples of how to use the `aws_iam_management` package to manage IAM roles and policies.

## Basic Usage

### Creating a Basic Lambda Execution Role

To create a basic Lambda execution role with the minimum permissions required for a Lambda function to operate properly:

```python
from aws_iam_management.core.templates import RoleTemplates

# Initialize the RoleTemplates class
templates = RoleTemplates()

# Create a basic Lambda execution role
role = templates.create_lambda_execution_role(
    role_name="my-lambda-function-role",
    description="Role for my Lambda function"
)

print(f"Created role: {role['RoleName']}")
```

### Creating an Enhanced Lambda Execution Role

To create an enhanced Lambda execution role with access to multiple AWS services:

```python
from aws_iam_management.core.templates import RoleTemplates

# Initialize the RoleTemplates class
templates = RoleTemplates()

# Create an enhanced Lambda execution role
role = templates.create_enhanced_lambda_role(
    role_name="my-complex-lambda-function-role",
    description="Role for my complex Lambda function",
    s3_access=True,
    dynamodb_access=True,
    sqs_access=True,
    vpc_access=True
)

print(f"Created role: {role['RoleName']}")
```

### Creating a Custom Role

To create a custom IAM role with specific permissions:

```python
from aws_iam_management.core.templates import RoleTemplates
from aws_iam_management.core.permission_sets import PermissionSets

# Initialize the RoleTemplates class
templates = RoleTemplates()

# Define a trust policy for Lambda
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

# Create a custom role
role = templates.create_custom_role(
    role_name="my-custom-role",
    trust_policy=trust_policy,
    description="My custom role",
    permissions=[
        PermissionSets.basic_lambda_execution(),
        PermissionSets.s3_read_only(bucket_name="my-bucket"),
        PermissionSets.dynamodb_read_only(table_name="my-table")
    ]
)

print(f"Created role: {role['RoleName']}")
```

## Advanced Usage

### Using the Lambda Execution Role Example

The `lambda_execution_role_example` module provides a more comprehensive example of creating Lambda execution roles with specific permissions for various AWS services:

```python
from aws_iam_management.examples.lambda_execution_role_example import create_lambda_execution_role

# Create a Lambda execution role with specific permissions
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

print(f"Created role: {role['RoleName']}")
```

### Managing IAM Policies

To create and manage IAM policies:

```python
from aws_iam_management.core.policy_manager import PolicyManager

# Initialize the PolicyManager class
policy_manager = PolicyManager()

# Create a policy
policy = policy_manager.create_policy(
    policy_name="my-policy",
    policy_document={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::my-bucket",
                    "arn:aws:s3:::my-bucket/*"
                ]
            }
        ]
    },
    description="My policy",
    tags=[
        {
            "Key": "Environment",
            "Value": "production"
        },
        {
            "Key": "Application",
            "Value": "my-app"
        }
    ]
)

print(f"Created policy: {policy['PolicyName']}")

# Create a new version of the policy
policy_version = policy_manager.create_policy_version(
    policy_arn=policy["Arn"],
    policy_document={
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:PutObject"
                ],
                "Resource": [
                    "arn:aws:s3:::my-bucket",
                    "arn:aws:s3:::my-bucket/*"
                ]
            }
        ]
    },
    set_as_default=True
)

print(f"Created policy version: {policy_version['VersionId']}")
```

### Using Permission Sets

To use predefined permission sets:

```python
from aws_iam_management.core.permission_sets import PermissionSets

# Get the S3 read-only permission set for a specific bucket
s3_read_only = PermissionSets.s3_read_only(bucket_name="my-bucket")

# Get the DynamoDB read-write permission set for a specific table
dynamodb_read_write = PermissionSets.dynamodb_read_write(table_name="my-table")

# Get the SQS consumer permission set for a specific queue
sqs_consumer = PermissionSets.sqs_consumer(queue_arn="arn:aws:sqs:us-east-1:123456789012:my-queue")

# Get the VPC execution permission set
vpc_execution = PermissionSets.vpc_execution()
```

### Validating IAM Policies

To validate IAM policies against best practices:

```python
from aws_iam_management.utils.validation import ValidationUtils

# Initialize the ValidationUtils class
validation = ValidationUtils()

# Define a policy document
policy_document = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        }
    ]
}

# Validate the policy against the principle of least privilege
findings = validation.validate_policy_least_privilege(policy_document)

# Print the validation findings
for finding in findings:
    print(f"{finding['severity']}: {finding['message']} at {finding['location']}")
```

### Tagging IAM Resources

To apply standard tags to IAM resources:

```python
from aws_iam_management.utils.tagging import TaggingUtils

# Initialize the TaggingUtils class
tagging = TaggingUtils()

# Apply standard tags to a role
tags = tagging.apply_standard_tags(
    resource_arn="arn:aws:iam::123456789012:role/my-role",
    resource_type="role",
    environment="production",
    application="my-app",
    owner="my-team",
    additional_tags={
        "CostCenter": "123456",
        "Project": "my-project"
    }
)

print(f"Applied tags: {tags}")

# Get resources by tag
roles = tagging.get_resources_by_tag(
    resource_type="role",
    tag_key="Environment",
    tag_value="production"
)

print(f"Found {len(roles)} roles with Environment=production")
```

### Logging IAM Operations

To log IAM operations for audit and compliance purposes:

```python
from aws_iam_management.utils.logging import LoggingUtils

# Initialize the LoggingUtils class
logging_utils = LoggingUtils()

# Log a role change
logging_utils.log_role_change(
    action="update",
    role_name="my-role",
    user="my-user",
    details={
        "permissions_added": ["s3:GetObject"],
        "permissions_removed": ["s3:DeleteObject"]
    }
)

# Get the audit trail for a role
audit_trail = logging_utils.get_audit_trail(
    resource_type="role",
    resource_name="my-role",
    start_time="2023-01-01T00:00:00Z",
    end_time="2023-01-02T00:00:00Z"
)

print(f"Found {len(audit_trail)} audit trail entries")
```

## Best Practices

For best practices on managing IAM roles and policies, see the [Best Practices Guide](best_practices.md).

## API Reference

For a detailed reference of the classes and functions in the `aws_iam_management` package, see the [API Reference](api_reference.md).
