"""
IAM Permission Sets Module

This module provides predefined permission sets for common AWS service access patterns.
These permission sets follow the principle of least privilege and can be used to create IAM policies.
"""

from typing import Dict, Any

class PermissionSets:
    """
    A class containing predefined permission sets for common AWS service access patterns.
    
    These permission sets can be used to create IAM policies that follow the principle of least privilege.
    """
    
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
        
    @staticmethod
    def s3_read_only(bucket_name: str = "*", object_prefix: str = "*") -> Dict[str, Any]:
        """
        Get the S3 read-only permission set.
        
        This permission set allows read-only access to S3 buckets and objects.
        
        Args:
            bucket_name: The name of the S3 bucket. Default is "*" (all buckets).
            object_prefix: The prefix for S3 objects. Default is "*" (all objects).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/{object_prefix}"
                    ]
                }
            ]
        }
        
    @staticmethod
    def s3_read_write(bucket_name: str = "*", object_prefix: str = "*") -> Dict[str, Any]:
        """
        Get the S3 read-write permission set.
        
        This permission set allows read and write access to S3 buckets and objects.
        
        Args:
            bucket_name: The name of the S3 bucket. Default is "*" (all buckets).
            object_prefix: The prefix for S3 objects. Default is "*" (all objects).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:PutObject",
                        "s3:PutObjectAcl",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/{object_prefix}"
                    ]
                }
            ]
        }
        
    @staticmethod
    def dynamodb_read_only(table_name: str = "*") -> Dict[str, Any]:
        """
        Get the DynamoDB read-only permission set.
        
        This permission set allows read-only access to DynamoDB tables.
        
        Args:
            table_name: The name of the DynamoDB table. Default is "*" (all tables).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:GetItem",
                        "dynamodb:BatchGetItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:DescribeTable"
                    ],
                    "Resource": f"arn:aws:dynamodb:*:*:table/{table_name}"
                }
            ]
        }
        
    @staticmethod
    def dynamodb_read_write(table_name: str = "*") -> Dict[str, Any]:
        """
        Get the DynamoDB read-write permission set.
        
        This permission set allows read and write access to DynamoDB tables.
        
        Args:
            table_name: The name of the DynamoDB table. Default is "*" (all tables).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:GetItem",
                        "dynamodb:BatchGetItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:BatchWriteItem",
                        "dynamodb:DescribeTable"
                    ],
                    "Resource": f"arn:aws:dynamodb:*:*:table/{table_name}"
                }
            ]
        }
        
    @staticmethod
    def sqs_consumer(queue_arn: str = "*") -> Dict[str, Any]:
        """
        Get the SQS consumer permission set.
        
        This permission set allows consuming messages from SQS queues.
        
        Args:
            queue_arn: The ARN of the SQS queue. Default is "*" (all queues).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:ChangeMessageVisibility"
                    ],
                    "Resource": queue_arn
                }
            ]
        }
        
    @staticmethod
    def sqs_producer(queue_arn: str = "*") -> Dict[str, Any]:
        """
        Get the SQS producer permission set.
        
        This permission set allows producing messages to SQS queues.
        
        Args:
            queue_arn: The ARN of the SQS queue. Default is "*" (all queues).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:SendMessage",
                        "sqs:SendMessageBatch",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl"
                    ],
                    "Resource": queue_arn
                }
            ]
        }
        
    @staticmethod
    def sns_publisher(topic_arn: str = "*") -> Dict[str, Any]:
        """
        Get the SNS publisher permission set.
        
        This permission set allows publishing messages to SNS topics.
        
        Args:
            topic_arn: The ARN of the SNS topic. Default is "*" (all topics).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "sns:Publish",
                        "sns:GetTopicAttributes"
                    ],
                    "Resource": topic_arn
                }
            ]
        }
        
    @staticmethod
    def cloudwatch_logs(log_group_name: str = "*") -> Dict[str, Any]:
        """
        Get the CloudWatch Logs permission set.
        
        This permission set allows writing logs to CloudWatch Logs.
        
        Args:
            log_group_name: The name of the log group. Default is "*" (all log groups).
            
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
                        "logs:PutLogEvents",
                        "logs:DescribeLogStreams"
                    ],
                    "Resource": [
                        f"arn:aws:logs:*:*:log-group:{log_group_name}",
                        f"arn:aws:logs:*:*:log-group:{log_group_name}:log-stream:*"
                    ]
                }
            ]
        }
        
    @staticmethod
    def kms_decrypt(key_id: str = "*") -> Dict[str, Any]:
        """
        Get the KMS decrypt permission set.
        
        This permission set allows decrypting data using KMS keys.
        
        Args:
            key_id: The ID of the KMS key. Default is "*" (all keys).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "kms:Decrypt",
                        "kms:DescribeKey"
                    ],
                    "Resource": f"arn:aws:kms:*:*:key/{key_id}"
                }
            ]
        }
        
    @staticmethod
    def kms_encrypt_decrypt(key_id: str = "*") -> Dict[str, Any]:
        """
        Get the KMS encrypt/decrypt permission set.
        
        This permission set allows encrypting and decrypting data using KMS keys.
        
        Args:
            key_id: The ID of the KMS key. Default is "*" (all keys).
            
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey"
                    ],
                    "Resource": f"arn:aws:kms:*:*:key/{key_id}"
                }
            ]
        }
        
    @staticmethod
    def xray_write() -> Dict[str, Any]:
        """
        Get the X-Ray write permission set.
        
        This permission set allows writing trace data to X-Ray.
        
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "xray:PutTraceSegments",
                        "xray:PutTelemetryRecords",
                        "xray:GetSamplingRules",
                        "xray:GetSamplingTargets",
                        "xray:GetSamplingStatisticSummaries"
                    ],
                    "Resource": "*"
                }
            ]
        }
        
    @staticmethod
    def vpc_execution() -> Dict[str, Any]:
        """
        Get the VPC execution permission set.
        
        This permission set allows Lambda functions to access resources in a VPC.
        
        Returns:
            A policy document as a dictionary.
        """
        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "ec2:CreateNetworkInterface",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:DeleteNetworkInterface",
                        "ec2:AssignPrivateIpAddresses",
                        "ec2:UnassignPrivateIpAddresses"
                    ],
                    "Resource": "*"
                }
            ]
        }
