"""
Unit tests for the ValidationUtils class.
"""

import unittest
from unittest.mock import MagicMock, patch
import json
import boto3
from botocore.exceptions import ClientError

from aws_iam_management.utils.validation import ValidationUtils

class TestValidationUtils(unittest.TestCase):
    """
    Test cases for the ValidationUtils class.
    """
    
    def setUp(self):
        """
        Set up the test environment.
        """
        # Create a ValidationUtils instance
        self.validation = ValidationUtils()
        
        # Define test data
        self.test_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::test-bucket"
                }
            ]
        }
        
        self.test_policy_document_with_wildcard = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "*",
                    "Resource": "*"
                }
            ]
        }
        
        self.test_trust_policy = {
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
        
        self.test_trust_policy_with_wildcard = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "*"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        
    def test_validate_policy_least_privilege(self):
        """
        Test validating a policy against the principle of least privilege.
        """
        # Call the method under test
        findings = self.validation.validate_policy_least_privilege(self.test_policy_document)
        
        # Verify the result
        self.assertEqual(len(findings), 0)
        
    def test_validate_policy_least_privilege_with_wildcard(self):
        """
        Test validating a policy with wildcards against the principle of least privilege.
        """
        # Call the method under test
        findings = self.validation.validate_policy_least_privilege(self.test_policy_document_with_wildcard)
        
        # Verify the result
        self.assertEqual(len(findings), 2)
        self.assertEqual(findings[0]['severity'], 'WARNING')
        self.assertEqual(findings[1]['severity'], 'WARNING')
        
    def test_check_for_wildcards(self):
        """
        Test checking for wildcards in a policy.
        """
        # Call the method under test
        findings = self.validation.check_for_wildcards(self.test_policy_document)
        
        # Verify the result
        self.assertEqual(len(findings), 0)
        
    def test_check_for_wildcards_with_wildcard(self):
        """
        Test checking for wildcards in a policy with wildcards.
        """
        # Call the method under test
        findings = self.validation.check_for_wildcards(self.test_policy_document_with_wildcard)
        
        # Verify the result
        self.assertEqual(len(findings), 2)
        self.assertEqual(findings[0]['severity'], 'WARNING')
        self.assertEqual(findings[1]['severity'], 'HIGH')
        
    def test_validate_trust_relationship(self):
        """
        Test validating a trust relationship policy.
        """
        # Call the method under test
        findings = self.validation.validate_trust_relationship(self.test_trust_policy)
        
        # Verify the result
        self.assertEqual(len(findings), 0)
        
    def test_validate_trust_relationship_with_wildcard(self):
        """
        Test validating a trust relationship policy with wildcards.
        """
        # Call the method under test
        findings = self.validation.validate_trust_relationship(self.test_trust_policy_with_wildcard)
        
        # Verify the result
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0]['severity'], 'HIGH')
        
    def test_check_policy_size(self):
        """
        Test checking the size of a policy.
        """
        # Call the method under test
        result = self.validation.check_policy_size(self.test_policy_document)
        
        # Verify the result
        self.assertFalse(result['exceeds_limit'])
        self.assertLess(result['size_bytes'], result['max_size_bytes'])
        
    def test_check_policy_statement_count(self):
        """
        Test checking the number of statements in a policy.
        """
        # Call the method under test
        result = self.validation.check_policy_statement_count(self.test_policy_document)
        
        # Verify the result
        self.assertEqual(result['statement_count'], 1)
        self.assertFalse(result['exceeds_limit'])
        
    def test_analyze_policy_complexity(self):
        """
        Test analyzing the complexity of a policy.
        """
        # Call the method under test
        result = self.validation.analyze_policy_complexity(self.test_policy_document)
        
        # Verify the result
        self.assertEqual(result['statement_count'], 1)
        self.assertEqual(result['unique_action_count'], 1)
        self.assertEqual(result['unique_resource_count'], 1)
        self.assertEqual(result['condition_count'], 0)
        self.assertEqual(result['complexity_score'], 3)

if __name__ == "__main__":
    unittest.main()
