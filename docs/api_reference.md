# API Reference

This document provides a detailed reference for the classes and functions in the `aws_iam_management` package.

## Core Modules

### RoleManager

The `RoleManager` class provides methods for managing IAM roles.

#### Constructor

```python
RoleManager(session: Optional[boto3.Session] = None)
```

- `session`: An optional boto3 Session object. If not provided, a new session will be created.

#### Methods

##### create_role

```python
create_role(
    role_name: str, 
    trust_policy: Dict[str, Any], 
    description: str = "",
    max_session_duration: int = 3600,
    path: str = "/",
    permissions_boundary: Optional[str] = None,
    tags: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]
```

Creates an IAM role with the specified parameters.

- `role_name`: The name of the role to create.
- `trust_policy`: The trust policy that grants an entity permission to assume the role.
- `description`: A description of the role.
- `max_session_duration`: The maximum session duration (in seconds) for the role.
- `path`: The path to the role.
- `permissions_boundary`: The ARN of the policy that is used to set the permissions boundary for the role.
- `tags`: A list of tags to attach to the role.

Returns the newly created role as a dictionary.

##### delete_role

```python
delete_role(role_name: str) -> bool
```

Deletes an IAM role.

- `role_name`: The name of the role to delete.

Returns `True` if the role was deleted successfully, `False` otherwise.

##### get_role

```python
get_role(role_name: str) -> Dict[str, Any]
```

Gets information about an IAM role.

- `role_name`: The name of the role to get information about.

Returns the role information as a dictionary.

##### list_roles

```python
list_roles(path_prefix: str = "/", max_items: int = 100) -> List[Dict[str, Any]]
```

Lists IAM roles.

- `path_prefix`: The path prefix for filtering the results.
- `max_items`: The maximum number of items to return.

Returns a list of IAM roles.

##### update_role

```python
update_role(
    role_name: str, 
    description: Optional[str] = None,
    max_session_duration: Optional[int] = None
) -> Dict[str, Any]
```

Updates an IAM role.

- `role_name`: The name of the role to update.
- `description`: A new description for the role.
- `max_session_duration`: The maximum session duration (in seconds) for the role.

Returns the updated role as a dictionary.

##### update_assume_role_policy

```python
update_assume_role_policy(role_name: str, trust_policy: Dict[str, Any]) -> bool
```

Updates the trust policy of an IAM role.

- `role_name`: The name of the role to update.
- `trust_policy`: The new trust policy.

Returns `True` if the trust policy was updated successfully, `False` otherwise.

##### attach_policy

```python
attach_policy(role_name: str, policy_arn: str) -> bool
```

Attaches a managed policy to an IAM role.

- `role_name`: The name of the role to attach the policy to.
- `policy_arn`: The ARN of the policy to attach.

Returns `True` if the policy was attached successfully, `False` otherwise.

##### detach_policy

```python
detach_policy(role_name: str, policy_arn: str) -> bool
```

Detaches a managed policy from an IAM role.

- `role_name`: The name of the role to detach the policy from.
- `policy_arn`: The ARN of the policy to detach.

Returns `True` if the policy was detached successfully, `False` otherwise.

##### put_role_policy

```python
put_role_policy(role_name: str, policy_name: str, policy_document: Dict[str, Any]) -> bool
```

Adds or updates an inline policy for an IAM role.

- `role_name`: The name of the role to add the policy to.
- `policy_name`: The name of the policy.
- `policy_document`: The policy document.

Returns `True` if the policy was added or updated successfully, `False` otherwise.

##### delete_role_policy

```python
delete_role_policy(role_name: str, policy_name: str) -> bool
```

Deletes an inline policy from an IAM role.

- `role_name`: The name of the role to delete the policy from.
- `policy_name`: The name of the policy to delete.

Returns `True` if the policy was deleted successfully, `False` otherwise.

##### list_attached_role_policies

```python
list_attached_role_policies(role_name: str) -> List[Dict[str, str]]
```

Lists the managed policies attached to an IAM role.

- `role_name`: The name of the role to list policies for.

Returns a list of attached policies.

##### list_role_policies

```python
list_role_policies(role_name: str) -> List[str]
```

Lists the names of the inline policies embedded in an IAM role.

- `role_name`: The name of the role to list policies for.

Returns a list of policy names.

##### get_role_policy

```python
get_role_policy(role_name: str, policy_name: str) -> Dict[str, Any]
```

Gets information about an inline policy for an IAM role.

- `role_name`: The name of the role.
- `policy_name`: The name of the policy.

Returns the policy document as a dictionary.

##### tag_role

```python
tag_role(role_name: str, tags: List[Dict[str, str]]) -> bool
```

Adds tags to an IAM role.

- `role_name`: The name of the role to tag.
- `tags`: A list of tags to add to the role.

Returns `True` if the tags were added successfully, `False` otherwise.

##### untag_role

```python
untag_role(role_name: str, tag_keys: List[str]) -> bool
```

Removes tags from an IAM role.

- `role_name`: The name of the role to remove tags from.
- `tag_keys`: A list of tag keys to remove.

Returns `True` if the tags were removed successfully, `False` otherwise.

### PolicyManager

The `PolicyManager` class provides methods for managing IAM policies.

#### Constructor

```python
PolicyManager(session: Optional[boto3.Session] = None)
```

- `session`: An optional boto3 Session object. If not provided, a new session will be created.

#### Methods

##### create_policy

```python
create_policy(
    policy_name: str, 
    policy_document: Dict[str, Any], 
    description: str = "",
    path: str = "/",
    tags: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]
```

Creates an IAM policy with the specified parameters.

- `policy_name`: The name of the policy to create.
- `policy_document`: The policy document.
- `description`: A description of the policy.
- `path`: The path to the policy.
- `tags`: A list of tags to attach to the policy.

Returns the newly created policy as a dictionary.

##### delete_policy

```python
delete_policy(policy_arn: str) -> bool
```

Deletes an IAM policy.

- `policy_arn`: The ARN of the policy to delete.

Returns `True` if the policy was deleted successfully, `False` otherwise.

##### get_policy

```python
get_policy(policy_arn: str) -> Dict[str, Any]
```

Gets information about an IAM policy.

- `policy_arn`: The ARN of the policy to get information about.

Returns the policy information as a dictionary.

##### list_policies

```python
list_policies(
    scope: str = "Local", 
    only_attached: bool = False,
    path_prefix: str = "/",
    max_items: int = 100
) -> List[Dict[str, Any]]
```

Lists IAM policies.

- `scope`: The scope to filter policies by. Valid values: 'All', 'AWS', 'Local'.
- `only_attached`: If True, only attached policies are returned.
- `path_prefix`: The path prefix for filtering the results.
- `max_items`: The maximum number of items to return.

Returns a list of IAM policies.

##### create_policy_version

```python
create_policy_version(
    policy_arn: str, 
    policy_document: Dict[str, Any], 
    set_as_default: bool = True
) -> Dict[str, Any]
```

Creates a new version of an IAM policy.

- `policy_arn`: The ARN of the policy to create a new version for.
- `policy_document`: The policy document.
- `set_as_default`: If True, the new version is set as the default version.

Returns the newly created policy version as a dictionary.

##### get_policy_version

```python
get_policy_version(policy_arn: str, version_id: str) -> Dict[str, Any]
```

Gets information about a version of an IAM policy.

- `policy_arn`: The ARN of the policy.
- `version_id`: The version ID.

Returns the policy version information as a dictionary.

##### list_policy_versions

```python
list_policy_versions(policy_arn: str) -> List[Dict[str, Any]]
```

Lists the versions of an IAM policy.

- `policy_arn`: The ARN of the policy to list versions for.

Returns a list of policy versions.

##### delete_policy_version

```python
delete_policy_version(policy_arn: str, version_id: str) -> bool
```

Deletes a version of an IAM policy.

- `policy_arn`: The ARN of the policy.
- `version_id`: The version ID.

Returns `True` if the policy version was deleted successfully, `False` otherwise.

##### set_default_policy_version

```python
set_default_policy_version(policy_arn: str, version_id: str) -> bool
```

Sets the default version of an IAM policy.

- `policy_arn`: The ARN of the policy.
- `version_id`: The version ID to set as the default.

Returns `True` if the default version was set successfully, `False` otherwise.

##### cleanup_policy_versions

```python
cleanup_policy_versions(policy_arn: str, keep_count: int = 4) -> None
```

Deletes old non-default versions of an IAM policy to make room for new versions.

- `policy_arn`: The ARN of the policy.
- `keep_count`: The number of non-default versions to keep (default is 4, which allows for 1 default version and 4 non-default versions).

##### delete_policy_versions

```python
delete_policy_versions(policy_arn: str) -> None
```

Deletes all non-default versions of an IAM policy.

- `policy_arn`: The ARN of the policy.

##### detach_policy_from_all_entities

```python
detach_policy_from_all_entities(policy_arn: str) -> None
```

Detaches an IAM policy from all entities (users, groups, and roles).

- `policy_arn`: The ARN of the policy to detach.

##### list_entities_for_policy

```python
list_entities_for_policy(
    policy_arn: str, 
    entity_filter: str = "All",
    path_prefix: str = "/",
    max_items: int = 100
) -> Dict[str, List[Dict[str, str]]]
```

Lists all entities (users, groups, and roles) that a policy is attached to.

- `policy_arn`: The ARN of the policy.
- `entity_filter`: The entity type to include. Valid values: 'User', 'Role', 'Group', 'LocalManagedPolicy', 'AWSManagedPolicy', 'All'.
- `path_prefix`: The path prefix for filtering the results.
- `max_items`: The maximum number of items to return.

Returns a dictionary containing lists of users, groups, and roles that the policy is attached to.

##### tag_policy

```python
tag_policy(policy_arn: str, tags: List[Dict[str, str]]) -> bool
```

Adds tags to an IAM policy.

- `policy_arn`: The ARN of the policy to tag.
- `tags`: A list of tags to add to the policy.

Returns `True` if the tags were added successfully, `False` otherwise.

##### untag_policy

```python
untag_policy(policy_arn: str, tag_keys: List[str]) -> bool
```

Removes tags from an IAM policy.

- `policy_arn`: The ARN of the policy to remove tags from.
- `tag_keys`: A list of tag keys to remove.

Returns `True` if the tags were removed successfully, `False` otherwise.

##### validate_policy

```python
validate_policy(policy_document: Dict[str, Any]) -> Dict[str, Any]
```

Validates an IAM policy document.

- `policy_document`: The policy document to validate.

Returns the validation results as a dictionary.

### PermissionSets

The `PermissionSets` class provides predefined permission sets for common AWS service access patterns.

#### Methods

##### basic_lambda_execution

```python
@staticmethod
basic_lambda_execution() -> Dict[str, Any]
```

Gets the basic Lambda execution permission set.

Returns a policy document as a dictionary.

##### s3_read_only

```python
@staticmethod
s3_read_only(bucket_name: str = "*", object_prefix: str = "*") -> Dict[str, Any]
```

Gets the S3 read-only permission set.

- `bucket_name`: The name of the S3 bucket. Default is "*" (all buckets).
- `object_prefix`: The prefix for S3 objects. Default is "*" (all objects).

Returns a policy document as a dictionary.

##### s3_read_write

```python
@staticmethod
s3_read_write(bucket_name: str = "*", object_prefix: str = "*") -> Dict[str, Any]
```

Gets the S3 read-write permission set.

- `bucket_name`: The name of the S3 bucket. Default is "*" (all buckets).
- `object_prefix`: The prefix for S3 objects. Default is "*" (all objects).

Returns a policy document as a dictionary.

##### dynamodb_read_only

```python
@staticmethod
dynamodb_read_only(table_name: str = "*") -> Dict[str, Any]
```

Gets the DynamoDB read-only permission set.

- `table_name`: The name of the DynamoDB table. Default is "*" (all tables).

Returns a policy document as a dictionary.

##### dynamodb_read_write

```python
@staticmethod
dynamodb_read_write(table_name: str = "*") -> Dict[str, Any]
```

Gets the DynamoDB read-write permission set.

- `table_name`: The name of the DynamoDB table. Default is "*" (all tables).

Returns a policy document as a dictionary.

##### sqs_consumer

```python
@staticmethod
sqs_consumer(queue_arn: str = "*") -> Dict[str, Any]
```

Gets the SQS consumer permission set.

- `queue_arn`: The ARN of the SQS queue. Default is "*" (all queues).

Returns a policy document as a dictionary.

##### sqs_producer

```python
@staticmethod
sqs_producer(queue_arn: str = "*") -> Dict[str, Any]
```

Gets the SQS producer permission set.

- `queue_arn`: The ARN of the SQS queue. Default is "*" (all queues).

Returns a policy document as a dictionary.

##### sns_publisher

```python
@staticmethod
sns_publisher(topic_arn: str = "*") -> Dict[str, Any]
```

Gets the SNS publisher permission set.

- `topic_arn`: The ARN of the SNS topic. Default is "*" (all topics).

Returns a policy document as a dictionary.

##### cloudwatch_logs

```python
@staticmethod
cloudwatch_logs(log_group_name: str = "*") -> Dict[str, Any]
```

Gets the CloudWatch Logs permission set.

- `log_group_name`: The name of the log group. Default is "*" (all log groups).

Returns a policy document as a dictionary.

##### kms_decrypt

```python
@staticmethod
kms_decrypt(key_id: str = "*") -> Dict[str, Any]
```

Gets the KMS decrypt permission set.

- `key_id`: The ID of the KMS key. Default is "*" (all keys).

Returns a policy document as a dictionary.

##### kms_encrypt_decrypt

```python
@staticmethod
kms_encrypt_decrypt(key_id: str = "*") -> Dict[str, Any]
```

Gets the KMS encrypt/decrypt permission set.

- `key_id`: The ID of the KMS key. Default is "*" (all keys).

Returns a policy document as a dictionary.

##### xray_write

```python
@staticmethod
xray_write() -> Dict[str, Any]
```

Gets the X-Ray write permission set.

Returns a policy document as a dictionary.

##### vpc_execution

```python
@staticmethod
vpc_execution() -> Dict[str, Any]
```

Gets the VPC execution permission set.

Returns a policy document as a dictionary.

### RoleTemplates

The `RoleTemplates` class provides templates for creating common IAM roles.

#### Constructor

```python
RoleTemplates(session: Optional[boto3.Session] = None)
```

- `session`: An optional boto3 Session object. If not provided, a new session will be created.

#### Methods

##### create_lambda_execution_role

```python
create_lambda_execution_role(
    role_name: str,
    description: str = "Lambda execution role",
    path: str = "/service-role/",
    permissions: List[Dict[str, Any]] = None,
    managed_policy_arns: List[str] = None,
    tags: Optional[List[Dict[str, str]]] = None,
    max_session_duration: int = 3600
) -> Dict[str, Any]
```

Creates a Lambda execution role.

- `role_name`: The name of the role to create.
- `description`: A description of the role.
- `path`: The path to the role.
- `permissions`: A list of permission sets to include in the role.
- `managed_policy_arns`: A list of managed policy ARNs to attach to the role.
- `tags`: A list of tags to attach to the role.
- `max_session_duration`: The maximum session duration (in seconds) for the role.

Returns the newly created role as a dictionary.

##### create_ec2_instance_role

```python
create_ec2_instance_role(
    role_name: str,
    description: str = "EC2 instance role",
    path: str = "/service-role/",
    permissions: List[Dict[str, Any]] = None,
    managed_policy_arns: List[str] = None,
    tags: Optional[List[Dict[str, str]]] = None,
    max_session_duration: int = 3600
) -> Dict[str, Any]
```

Creates an EC2 instance role.

- `role_name`: The name of the role to create.
- `description`: A description of the role.
- `path`: The path to the role.
- `permissions`: A list of permission sets to include in the role.
- `managed_policy_arns`: A list of managed policy ARNs to attach to the role.
- `tags`: A list of tags to attach to the role.
- `max_session_duration`: The maximum session duration (in seconds) for the role.

Returns the newly created role as a dictionary.

##### create_custom_role

```python
create_custom_role(
    role_name: str,
    trust_policy: Dict[str, Any],
    description: str = "Custom role",
    path: str = "/",
    permissions: List[Dict[str, Any]] = None,
    managed_policy_arns: List[str] = None,
    tags: Optional[List[Dict[str, str]]] = None,
    max_session_duration: int = 3600
) -> Dict[str, Any]
```

Creates a custom IAM role.

- `role_name`: The name of the role to create.
- `trust_policy`: The trust policy that grants an entity permission to assume the role.
- `description`: A description of the role.
- `path`: The path to the role.
- `permissions`: A list of permission sets to include in the role.
- `managed_policy_arns`: A list of managed policy ARNs to attach to the role.
- `tags`: A list of tags to attach to the role.
- `max_session_duration`: The maximum session duration (in seconds) for the role.

Returns the newly created role as a dictionary.

##### create_enhanced_lambda_role

```python
create_enhanced_lambda_role(
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
) -> Dict[str, Any]
```

Creates an enhanced Lambda execution role with access to multiple AWS services.

- `role_name`: The name of the role to create.
- `description`: A description of the role.
- `path`: The path to the role.
- `s3_access`: Whether to include S3 access permissions.
- `dynamodb_access`: Whether to include DynamoDB access permissions.
- `sqs_access`: Whether to include SQS access permissions.
- `sns_access`: Whether to include SNS access permissions.
- `kms_access`: Whether to include KMS access permissions.
- `vpc_access`: Whether to include VPC access permissions.
- `xray_access`: Whether to include X-Ray access permissions.
- `additional_permissions`: Additional permission sets to include in the role.
- `managed_policy_arns`: A list of managed policy ARNs to attach to the role.
- `tags`: A list of tags to attach to the role.
- `max_session_duration`: The maximum session duration (in seconds) for the role.

Returns the newly created role as a dictionary.

## Utility Modules

### ValidationUtils

The `ValidationUtils` class provides methods for validating IAM policies and trust relationships.

#### Constructor

```python
ValidationUtils()
```

#### Methods

##### validate_policy_least_privilege

```python
validate_policy_least_privilege(policy_document: Dict[str, Any]) -> List[Dict[str, Any]]
```

Validates an IAM policy document against the principle of least privilege.

- `policy_document`: The policy document to validate.

Returns a list of validation findings.

##### validate_trust_relationship

```python
validate_trust_relationship(trust_policy: Dict[str, Any]) -> List[Dict[str, Any]]
```

Validates a trust policy document.

- `trust_policy`: The trust policy document to validate.

Returns a list of validation findings.

##### validate_policy_complexity

```python
validate_policy_complexity(policy_document: Dict[str, Any]) -> List[Dict[str, Any]]
```

Validates the complexity of an IAM policy document.

- `policy_document`: The policy document to validate.

Returns a list of validation findings.

##### validate_policy_size

```python
validate_policy_size(policy_document: Dict[str, Any]) -> List[Dict[str, Any]]
```

Validates the size of an IAM policy document.

- `policy_document`: The policy document to validate.

Returns a list of validation findings.

### TaggingUtils

The `TaggingUtils` class provides methods for tagging IAM resources.

#### Constructor

```python
TaggingUtils(session: Optional[boto3.Session] = None)
```

- `session`: An optional boto3 Session object. If not provided, a new session will be created.

#### Methods

##### apply_standard_tags

```python
apply_standard_tags(
    resource_arn: str,
    resource_type: str,
    environment: str,
    application: str,
    owner: str,
    additional_tags: Optional[Dict[str, str]] = None
) -> List[Dict[str, str]]
```

Applies standard tags to an IAM resource.

- `resource_arn`: The ARN of the resource to tag.
- `resource_type`: The type of the resource. Valid values: 'role', 'policy'.
- `environment`: The environment tag value.
- `application`: The application tag value.
- `owner`: The owner tag value.
- `additional_tags`: Additional tags to apply to the resource.

Returns a list of tags that were applied to the resource.

##### get_resources_by_tag

```python
get_resources_by_tag(
    resource_type: str,
    tag_key: str,
    tag_value: str
) -> List[Dict[str, Any]]
```

Gets IAM resources by tag.

- `resource_type`: The type of the resource. Valid values: 'role', 'policy'.
- `tag_key`: The tag key to filter by.
- `tag_value`: The tag value to filter by.

Returns a list of resources that match the tag.

##### get_all_tagged_resources

```python
get_all_tagged_resources() -> Dict[str, List[Dict[str, Any]]]
```

Gets all tagged IAM resources.

Returns a dictionary containing lists of roles and policies with their tags.

### LoggingUtils

The `LoggingUtils` class provides methods for logging IAM operations.

#### Constructor

```python
LoggingUtils(session: Optional[boto3.Session] = None)
```

- `session`: An optional boto3 Session object. If not provided, a new session will be created.

#### Methods

##### log_role_change

```python
log_role_change(
    action: str,
    role_name: str,
    user: str,
    details: Optional[Dict[str, Any]] = None
) -> bool
```

Logs a change to an IAM role.

- `action`: The action that was performed. Valid values: 'create', 'update', 'delete'.
- `role_name`: The name of the role that was changed.
- `user`: The user who performed the action.
- `details`: Additional details about the change.

Returns `True` if the change was logged successfully, `False` otherwise.

##### log_policy_change

```python
log_policy_change(
    action: str,
    policy_arn: str,
    user: str,
    details: Optional[Dict[str, Any]] = None
) -> bool
```

Logs a change to an IAM policy.

- `action`: The action that was performed. Valid values: 'create', 'update', 'delete'.
- `policy_arn`: The ARN of the policy that was changed.
- `user`: The user who performed the action.
- `details`: Additional details about the change.

Returns `True` if the change was logged successfully, `False` otherwise.

##### get_audit_trail

```python
get_audit_trail(
    resource_type: str,
    resource_name: str,
    start_time: str,
    end_time: str
) -> List[Dict[str, Any]]
```

Gets the audit trail for an IAM resource.

- `resource_type`: The type of the resource. Valid values: 'role', 'policy'.
- `resource_name`: The name of the resource.
- `start_time`: The start time for the audit trail.
- `end_time`: The end time for the audit trail.

Returns a list of audit trail entries.

## Example Modules

### lambda_execution_role_example

The `lambda_execution_role_example` module provides examples of creating Lambda execution roles.

#### Functions

##### create_lambda_execution_role

```python
create_lambda_execution_role(
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
) -> Dict[str, Any]
```

Creates a Lambda execution role with specific permissions.

- `role_name`: The name of the role to create.
- `s3_buckets`: A list of S3 bucket names to grant access to.
- `dynamodb_tables`: A list of DynamoDB table names to grant access to.
- `sqs_queues`: A list of SQS queue names to grant access to.
- `sns_topics`: A list of SNS topic names to grant access to.
- `kms_keys`: A list of KMS key IDs to grant access to.
- `vpc_access`: Whether to include VPC access permissions.
- `xray_tracing`: Whether to include X-Ray tracing permissions.
- `environment`: The environment tag value.
- `application`: The application tag value.
- `owner`: The owner tag value.
- `session`: An optional boto3 Session object. If not provided, a new session will be created.

Returns the newly created role as a dictionary.

##### create_lambda_execution_role_with_templates

```python
create_lambda_execution_role_with_templates(
    role_name: str,
    s3_access: bool = False,
    dynamodb_access: bool = False,
    vpc_access: bool = False,
    environment: str = "production",
    application: str = "lambda-app",
    owner: str = "platform-team",
    session: Optional[boto3.Session] = None
) -> Dict[str, Any]
```

Creates a Lambda execution role using the RoleTemplates class.

- `role_name`: The name of the role to create.
- `s3_access`: Whether to include S3 access permissions.
- `dynamodb_access`: Whether to include DynamoDB access permissions.
- `vpc_access`: Whether to include VPC access permissions.
- `environment`: The environment tag value.
- `application`: The application tag value.
- `owner`: The owner tag value.
- `session`: An optional boto3 Session object. If not provided, a new session will be created.

Returns the newly created role as a dictionary.
