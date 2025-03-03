# AWS IAM Best Practices for Lambda Functions

## Principle of Least Privilege
- Grant only the permissions required for the Lambda function to perform its tasks
- Avoid using wildcard permissions (*) in IAM policies
- Regularly review and audit permissions to ensure they remain appropriate

## Use IAM Roles Instead of IAM Users
- Always use IAM roles for Lambda functions instead of embedding credentials
- Roles provide temporary credentials and are more secure than long-term access keys

## Resource-Based Policies
- Use resource-based policies when appropriate to control access to specific resources
- Combine identity-based and resource-based policies for defense in depth

## Policy Conditions
- Use policy conditions to restrict permissions based on specific criteria
- Examples include source IP restrictions, time-based permissions, and MFA requirements

## Managed Policies vs. Inline Policies
- Use AWS managed policies when possible for common use cases
- Create custom managed policies for reusable permission sets
- Use inline policies for one-off or highly specific permission sets

## Permission Boundaries
- Implement permission boundaries to set the maximum permissions for a role
- Useful for delegating permissions management while maintaining control

## Tagging Strategy
- Implement a consistent tagging strategy for IAM resources
- Use attribute-based access control (ABAC) with tags when appropriate

## Monitoring and Logging
- Enable AWS CloudTrail for IAM activity logging
- Set up alerts for suspicious IAM activity
- Regularly review IAM access advisor to identify unused permissions

## Common Lambda Execution Role Permissions
- CloudWatch Logs: For Lambda function logging
- S3: If the function needs to access S3 buckets
- DynamoDB: If the function interacts with DynamoDB tables
- SQS/SNS: If the function processes messages or publishes notifications
- KMS: If the function needs to encrypt/decrypt data
- X-Ray: For tracing and debugging

## Automation and Infrastructure as Code
- Use infrastructure as code (IaC) tools like AWS CDK, CloudFormation, or Terraform to manage IAM resources
- Automate the creation and management of IAM roles and policies
- Version control IAM configurations

## Security Best Practices
- Implement a regular rotation schedule for IAM credentials
- Use IAM Access Analyzer to identify resources shared with external entities
- Implement a process for regular IAM policy reviews
