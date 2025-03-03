"""
Unit tests for the PermissionSets class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json

from aws_iam_management.core.permission_sets import PermissionSets

class TestPermissionSets(unittest.TestCase):
    """
    Test cases for the PermissionSets class.
    """
    
    def test_basic_lambda_execution(self):
        """
        Test the basic Lambda execution permission set.
        """
        # Call the method under test
        result = PermissionSets.basic_lambda_execution()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 3)
        self.assertIn("logs:CreateLogGroup", result["Statement"][0]["Action"])
        self.assertIn("logs:CreateLogStream", result["Statement"][0]["Action"])
        self.assertIn("logs:PutLogEvents", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "arn:aws:logs:*:*:*")
        
    def test_s3_read_only(self):
        """
        Test the S3 read-only permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.s3_read_only()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("s3:GetObject", result["Statement"][0]["Action"])
        self.assertIn("s3:GetObjectVersion", result["Statement"][0]["Action"])
        self.assertIn("s3:ListBucket", result["Statement"][0]["Action"])
        self.assertIn("s3:GetBucketLocation", result["Statement"][0]["Action"])
        self.assertEqual(len(result["Statement"][0]["Resource"]), 2)
        self.assertEqual(result["Statement"][0]["Resource"][0], "arn:aws:s3:::*")
        self.assertEqual(result["Statement"][0]["Resource"][1], "arn:aws:s3:::*/*")
        
        # Call the method under test with specific parameters
        bucket_name = "test-bucket"
        object_prefix = "test-prefix"
        result = PermissionSets.s3_read_only(bucket_name=bucket_name, object_prefix=object_prefix)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("s3:GetObject", result["Statement"][0]["Action"])
        self.assertIn("s3:GetObjectVersion", result["Statement"][0]["Action"])
        self.assertIn("s3:ListBucket", result["Statement"][0]["Action"])
        self.assertIn("s3:GetBucketLocation", result["Statement"][0]["Action"])
        self.assertEqual(len(result["Statement"][0]["Resource"]), 2)
        self.assertEqual(result["Statement"][0]["Resource"][0], f"arn:aws:s3:::{bucket_name}")
        self.assertEqual(result["Statement"][0]["Resource"][1], f"arn:aws:s3:::{bucket_name}/{object_prefix}")
        
    def test_s3_read_write(self):
        """
        Test the S3 read-write permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.s3_read_write()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 7)
        self.assertIn("s3:GetObject", result["Statement"][0]["Action"])
        self.assertIn("s3:GetObjectVersion", result["Statement"][0]["Action"])
        self.assertIn("s3:ListBucket", result["Statement"][0]["Action"])
        self.assertIn("s3:GetBucketLocation", result["Statement"][0]["Action"])
        self.assertIn("s3:PutObject", result["Statement"][0]["Action"])
        self.assertIn("s3:PutObjectAcl", result["Statement"][0]["Action"])
        self.assertIn("s3:DeleteObject", result["Statement"][0]["Action"])
        self.assertEqual(len(result["Statement"][0]["Resource"]), 2)
        self.assertEqual(result["Statement"][0]["Resource"][0], "arn:aws:s3:::*")
        self.assertEqual(result["Statement"][0]["Resource"][1], "arn:aws:s3:::*/*")
        
        # Call the method under test with specific parameters
        bucket_name = "test-bucket"
        object_prefix = "test-prefix"
        result = PermissionSets.s3_read_write(bucket_name=bucket_name, object_prefix=object_prefix)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 7)
        self.assertIn("s3:GetObject", result["Statement"][0]["Action"])
        self.assertIn("s3:GetObjectVersion", result["Statement"][0]["Action"])
        self.assertIn("s3:ListBucket", result["Statement"][0]["Action"])
        self.assertIn("s3:GetBucketLocation", result["Statement"][0]["Action"])
        self.assertIn("s3:PutObject", result["Statement"][0]["Action"])
        self.assertIn("s3:PutObjectAcl", result["Statement"][0]["Action"])
        self.assertIn("s3:DeleteObject", result["Statement"][0]["Action"])
        self.assertEqual(len(result["Statement"][0]["Resource"]), 2)
        self.assertEqual(result["Statement"][0]["Resource"][0], f"arn:aws:s3:::{bucket_name}")
        self.assertEqual(result["Statement"][0]["Resource"][1], f"arn:aws:s3:::{bucket_name}/{object_prefix}")
        
    def test_dynamodb_read_only(self):
        """
        Test the DynamoDB read-only permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.dynamodb_read_only()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 5)
        self.assertIn("dynamodb:GetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:BatchGetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Query", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Scan", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:DescribeTable", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "arn:aws:dynamodb:*:*:table/*")
        
        # Call the method under test with specific parameters
        table_name = "test-table"
        result = PermissionSets.dynamodb_read_only(table_name=table_name)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 5)
        self.assertIn("dynamodb:GetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:BatchGetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Query", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Scan", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:DescribeTable", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], f"arn:aws:dynamodb:*:*:table/{table_name}")
        
    def test_dynamodb_read_write(self):
        """
        Test the DynamoDB read-write permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.dynamodb_read_write()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 9)
        self.assertIn("dynamodb:GetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:BatchGetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Query", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Scan", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:PutItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:UpdateItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:DeleteItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:BatchWriteItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:DescribeTable", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "arn:aws:dynamodb:*:*:table/*")
        
        # Call the method under test with specific parameters
        table_name = "test-table"
        result = PermissionSets.dynamodb_read_write(table_name=table_name)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 9)
        self.assertIn("dynamodb:GetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:BatchGetItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Query", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:Scan", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:PutItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:UpdateItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:DeleteItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:BatchWriteItem", result["Statement"][0]["Action"])
        self.assertIn("dynamodb:DescribeTable", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], f"arn:aws:dynamodb:*:*:table/{table_name}")
        
    def test_sqs_consumer(self):
        """
        Test the SQS consumer permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.sqs_consumer()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("sqs:ReceiveMessage", result["Statement"][0]["Action"])
        self.assertIn("sqs:DeleteMessage", result["Statement"][0]["Action"])
        self.assertIn("sqs:GetQueueAttributes", result["Statement"][0]["Action"])
        self.assertIn("sqs:ChangeMessageVisibility", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "*")
        
        # Call the method under test with specific parameters
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:test-queue"
        result = PermissionSets.sqs_consumer(queue_arn=queue_arn)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("sqs:ReceiveMessage", result["Statement"][0]["Action"])
        self.assertIn("sqs:DeleteMessage", result["Statement"][0]["Action"])
        self.assertIn("sqs:GetQueueAttributes", result["Statement"][0]["Action"])
        self.assertIn("sqs:ChangeMessageVisibility", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], queue_arn)
        
    def test_sqs_producer(self):
        """
        Test the SQS producer permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.sqs_producer()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("sqs:SendMessage", result["Statement"][0]["Action"])
        self.assertIn("sqs:SendMessageBatch", result["Statement"][0]["Action"])
        self.assertIn("sqs:GetQueueAttributes", result["Statement"][0]["Action"])
        self.assertIn("sqs:GetQueueUrl", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "*")
        
        # Call the method under test with specific parameters
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:test-queue"
        result = PermissionSets.sqs_producer(queue_arn=queue_arn)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("sqs:SendMessage", result["Statement"][0]["Action"])
        self.assertIn("sqs:SendMessageBatch", result["Statement"][0]["Action"])
        self.assertIn("sqs:GetQueueAttributes", result["Statement"][0]["Action"])
        self.assertIn("sqs:GetQueueUrl", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], queue_arn)
        
    def test_sns_publisher(self):
        """
        Test the SNS publisher permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.sns_publisher()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 2)
        self.assertIn("sns:Publish", result["Statement"][0]["Action"])
        self.assertIn("sns:GetTopicAttributes", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "*")
        
        # Call the method under test with specific parameters
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        result = PermissionSets.sns_publisher(topic_arn=topic_arn)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 2)
        self.assertIn("sns:Publish", result["Statement"][0]["Action"])
        self.assertIn("sns:GetTopicAttributes", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], topic_arn)
        
    def test_cloudwatch_logs(self):
        """
        Test the CloudWatch Logs permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.cloudwatch_logs()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("logs:CreateLogGroup", result["Statement"][0]["Action"])
        self.assertIn("logs:CreateLogStream", result["Statement"][0]["Action"])
        self.assertIn("logs:PutLogEvents", result["Statement"][0]["Action"])
        self.assertIn("logs:DescribeLogStreams", result["Statement"][0]["Action"])
        self.assertEqual(len(result["Statement"][0]["Resource"]), 2)
        self.assertEqual(result["Statement"][0]["Resource"][0], "arn:aws:logs:*:*:log-group:*")
        self.assertEqual(result["Statement"][0]["Resource"][1], "arn:aws:logs:*:*:log-group:*:log-stream:*")
        
        # Call the method under test with specific parameters
        log_group_name = "test-log-group"
        result = PermissionSets.cloudwatch_logs(log_group_name=log_group_name)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 4)
        self.assertIn("logs:CreateLogGroup", result["Statement"][0]["Action"])
        self.assertIn("logs:CreateLogStream", result["Statement"][0]["Action"])
        self.assertIn("logs:PutLogEvents", result["Statement"][0]["Action"])
        self.assertIn("logs:DescribeLogStreams", result["Statement"][0]["Action"])
        self.assertEqual(len(result["Statement"][0]["Resource"]), 2)
        self.assertEqual(result["Statement"][0]["Resource"][0], f"arn:aws:logs:*:*:log-group:{log_group_name}")
        self.assertEqual(result["Statement"][0]["Resource"][1], f"arn:aws:logs:*:*:log-group:{log_group_name}:log-stream:*")
        
    def test_kms_decrypt(self):
        """
        Test the KMS decrypt permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.kms_decrypt()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 2)
        self.assertIn("kms:Decrypt", result["Statement"][0]["Action"])
        self.assertIn("kms:DescribeKey", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "arn:aws:kms:*:*:key/*")
        
        # Call the method under test with specific parameters
        key_id = "12345678-1234-1234-1234-123456789012"
        result = PermissionSets.kms_decrypt(key_id=key_id)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 2)
        self.assertIn("kms:Decrypt", result["Statement"][0]["Action"])
        self.assertIn("kms:DescribeKey", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], f"arn:aws:kms:*:*:key/{key_id}")
        
    def test_kms_encrypt_decrypt(self):
        """
        Test the KMS encrypt/decrypt permission set.
        """
        # Call the method under test with default parameters
        result = PermissionSets.kms_encrypt_decrypt()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 5)
        self.assertIn("kms:Encrypt", result["Statement"][0]["Action"])
        self.assertIn("kms:Decrypt", result["Statement"][0]["Action"])
        self.assertIn("kms:ReEncrypt*", result["Statement"][0]["Action"])
        self.assertIn("kms:GenerateDataKey*", result["Statement"][0]["Action"])
        self.assertIn("kms:DescribeKey", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "arn:aws:kms:*:*:key/*")
        
        # Call the method under test with specific parameters
        key_id = "12345678-1234-1234-1234-123456789012"
        result = PermissionSets.kms_encrypt_decrypt(key_id=key_id)
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 5)
        self.assertIn("kms:Encrypt", result["Statement"][0]["Action"])
        self.assertIn("kms:Decrypt", result["Statement"][0]["Action"])
        self.assertIn("kms:ReEncrypt*", result["Statement"][0]["Action"])
        self.assertIn("kms:GenerateDataKey*", result["Statement"][0]["Action"])
        self.assertIn("kms:DescribeKey", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], f"arn:aws:kms:*:*:key/{key_id}")
        
    def test_xray_write(self):
        """
        Test the X-Ray write permission set.
        """
        # Call the method under test
        result = PermissionSets.xray_write()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 5)
        self.assertIn("xray:PutTraceSegments", result["Statement"][0]["Action"])
        self.assertIn("xray:PutTelemetryRecords", result["Statement"][0]["Action"])
        self.assertIn("xray:GetSamplingRules", result["Statement"][0]["Action"])
        self.assertIn("xray:GetSamplingTargets", result["Statement"][0]["Action"])
        self.assertIn("xray:GetSamplingStatisticSummaries", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "*")
        
    def test_vpc_execution(self):
        """
        Test the VPC execution permission set.
        """
        # Call the method under test
        result = PermissionSets.vpc_execution()
        
        # Verify the result
        self.assertEqual(result["Version"], "2012-10-17")
        self.assertEqual(len(result["Statement"]), 1)
        self.assertEqual(result["Statement"][0]["Effect"], "Allow")
        self.assertEqual(len(result["Statement"][0]["Action"]), 5)
        self.assertIn("ec2:CreateNetworkInterface", result["Statement"][0]["Action"])
        self.assertIn("ec2:DescribeNetworkInterfaces", result["Statement"][0]["Action"])
        self.assertIn("ec2:DeleteNetworkInterface", result["Statement"][0]["Action"])
        self.assertIn("ec2:AssignPrivateIpAddresses", result["Statement"][0]["Action"])
        self.assertIn("ec2:UnassignPrivateIpAddresses", result["Statement"][0]["Action"])
        self.assertEqual(result["Statement"][0]["Resource"], "*")

if __name__ == "__main__":
    unittest.main()
