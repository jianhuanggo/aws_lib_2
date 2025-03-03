"""
IAM Validation Utilities Module

This module provides utilities for validating IAM policies and roles.
It helps ensure that IAM policies follow best practices and the principle of least privilege.
"""

import json
import logging
import re
from typing import Dict, List, Optional, Any, Tuple, Set

logger = logging.getLogger(__name__)

class ValidationUtils:
    """
    A class for validating IAM policies and roles.
    
    This class provides methods for checking IAM policies against best practices
    and the principle of least privilege.
    """
    
    @staticmethod
    def validate_policy_least_privilege(policy_document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Validate that an IAM policy follows the principle of least privilege.
        
        Args:
            policy_document: The policy document to validate.
            
        Returns:
            A list of validation findings, each as a dictionary with 'severity', 'message', and 'location' keys.
        """
        findings = []
        
        # Check for policy version
        if policy_document.get('Version') != '2012-10-17':
            findings.append({
                'severity': 'WARNING',
                'message': 'Policy should use version 2012-10-17',
                'location': 'Version'
            })
        
        # Check statements
        statements = policy_document.get('Statement', [])
        if not isinstance(statements, list):
            statements = [statements]
        
        for i, statement in enumerate(statements):
            # Check for wildcard resources
            resources = statement.get('Resource', [])
            if not isinstance(resources, list):
                resources = [resources]
            
            for resource in resources:
                if resource == '*':
                    findings.append({
                        'severity': 'WARNING',
                        'message': f'Statement {i} uses wildcard resource "*"',
                        'location': f'Statement[{i}].Resource'
                    })
            
            # Check for wildcard actions
            actions = statement.get('Action', [])
            if not isinstance(actions, list):
                actions = [actions]
            
            for action in actions:
                if action == '*' or action.endswith('*'):
                    findings.append({
                        'severity': 'WARNING',
                        'message': f'Statement {i} uses wildcard action "{action}"',
                        'location': f'Statement[{i}].Action'
                    })
            
            # Check for NotAction
            if 'NotAction' in statement:
                findings.append({
                    'severity': 'WARNING',
                    'message': f'Statement {i} uses NotAction, which can be overly permissive',
                    'location': f'Statement[{i}].NotAction'
                })
            
            # Check for NotResource
            if 'NotResource' in statement:
                findings.append({
                    'severity': 'WARNING',
                    'message': f'Statement {i} uses NotResource, which can be overly permissive',
                    'location': f'Statement[{i}].NotResource'
                })
            
            # Check for missing conditions on powerful actions
            powerful_actions = [
                'iam:*', 'iam:Create*', 'iam:Delete*', 'iam:Update*', 'iam:Put*',
                's3:*', 's3:Delete*', 's3:Put*',
                'dynamodb:*', 'dynamodb:Delete*', 'dynamodb:Update*', 'dynamodb:Put*',
                'kms:*', 'kms:Delete*', 'kms:Update*', 'kms:Put*',
                'ec2:*', 'ec2:TerminateInstances', 'ec2:StopInstances',
                'rds:*', 'rds:Delete*', 'rds:Stop*',
                'lambda:*', 'lambda:Delete*', 'lambda:Update*'
            ]
            
            if statement.get('Effect') == 'Allow':
                for action in actions:
                    if any(re.match(pattern.replace('*', '.*'), action) for pattern in powerful_actions):
                        if 'Condition' not in statement:
                            findings.append({
                                'severity': 'WARNING',
                                'message': f'Statement {i} allows powerful action "{action}" without conditions',
                                'location': f'Statement[{i}].Action'
                            })
        
        return findings
    
    @staticmethod
    def check_for_wildcards(policy_document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Check for wildcard permissions in an IAM policy.
        
        Args:
            policy_document: The policy document to check.
            
        Returns:
            A list of wildcard findings, each as a dictionary with 'severity', 'message', and 'location' keys.
        """
        findings = []
        
        # Check statements
        statements = policy_document.get('Statement', [])
        if not isinstance(statements, list):
            statements = [statements]
        
        for i, statement in enumerate(statements):
            if statement.get('Effect') == 'Allow':
                # Check for wildcard resources
                resources = statement.get('Resource', [])
                if not isinstance(resources, list):
                    resources = [resources]
                
                for resource in resources:
                    if resource == '*':
                        findings.append({
                            'severity': 'WARNING',
                            'message': f'Statement {i} uses wildcard resource "*"',
                            'location': f'Statement[{i}].Resource'
                        })
                
                # Check for wildcard actions
                actions = statement.get('Action', [])
                if not isinstance(actions, list):
                    actions = [actions]
                
                for action in actions:
                    if action == '*':
                        findings.append({
                            'severity': 'HIGH',
                            'message': f'Statement {i} uses wildcard action "*"',
                            'location': f'Statement[{i}].Action'
                        })
                    elif action.endswith('*'):
                        findings.append({
                            'severity': 'MEDIUM',
                            'message': f'Statement {i} uses wildcard action "{action}"',
                            'location': f'Statement[{i}].Action'
                        })
        
        return findings
    
    @staticmethod
    def validate_trust_relationship(trust_policy: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Validate a trust relationship policy.
        
        Args:
            trust_policy: The trust policy to validate.
            
        Returns:
            A list of validation findings, each as a dictionary with 'severity', 'message', and 'location' keys.
        """
        findings = []
        
        # Check for policy version
        if trust_policy.get('Version') != '2012-10-17':
            findings.append({
                'severity': 'WARNING',
                'message': 'Trust policy should use version 2012-10-17',
                'location': 'Version'
            })
        
        # Check statements
        statements = trust_policy.get('Statement', [])
        if not isinstance(statements, list):
            statements = [statements]
        
        for i, statement in enumerate(statements):
            # Check for wildcard principals
            principals = statement.get('Principal', {})
            if isinstance(principals, dict):
                for principal_type, principal_value in principals.items():
                    if principal_value == '*':
                        findings.append({
                            'severity': 'HIGH',
                            'message': f'Statement {i} uses wildcard principal "{principal_type}": "*"',
                            'location': f'Statement[{i}].Principal.{principal_type}'
                        })
            
            # Check for NotPrincipal
            if 'NotPrincipal' in statement:
                findings.append({
                    'severity': 'WARNING',
                    'message': f'Statement {i} uses NotPrincipal, which can be overly permissive',
                    'location': f'Statement[{i}].NotPrincipal'
                })
        
        return findings
    
    @staticmethod
    def check_policy_size(policy_document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if an IAM policy exceeds AWS size limits.
        
        Args:
            policy_document: The policy document to check.
            
        Returns:
            A dictionary with 'size_bytes', 'max_size_bytes', and 'exceeds_limit' keys.
        """
        policy_json = json.dumps(policy_document)
        size_bytes = len(policy_json.encode('utf-8'))
        max_size_bytes = 6144  # AWS limit is 6,144 bytes
        
        return {
            'size_bytes': size_bytes,
            'max_size_bytes': max_size_bytes,
            'exceeds_limit': size_bytes > max_size_bytes
        }
    
    @staticmethod
    def check_policy_statement_count(policy_document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if an IAM policy exceeds the maximum number of statements.
        
        Args:
            policy_document: The policy document to check.
            
        Returns:
            A dictionary with 'statement_count', 'max_statements', and 'exceeds_limit' keys.
        """
        statements = policy_document.get('Statement', [])
        if not isinstance(statements, list):
            statements = [statements]
        
        statement_count = len(statements)
        max_statements = 100  # AWS recommended limit
        
        return {
            'statement_count': statement_count,
            'max_statements': max_statements,
            'exceeds_limit': statement_count > max_statements
        }
    
    @staticmethod
    def analyze_policy_complexity(policy_document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze the complexity of an IAM policy.
        
        Args:
            policy_document: The policy document to analyze.
            
        Returns:
            A dictionary with complexity metrics.
        """
        statements = policy_document.get('Statement', [])
        if not isinstance(statements, list):
            statements = [statements]
        
        # Count unique actions, resources, and conditions
        unique_actions = set()
        unique_resources = set()
        condition_count = 0
        
        for statement in statements:
            # Count actions
            actions = statement.get('Action', [])
            if not isinstance(actions, list):
                actions = [actions]
            unique_actions.update(actions)
            
            # Count resources
            resources = statement.get('Resource', [])
            if not isinstance(resources, list):
                resources = [resources]
            unique_resources.update(resources)
            
            # Count conditions
            if 'Condition' in statement:
                condition_count += 1
        
        return {
            'statement_count': len(statements),
            'unique_action_count': len(unique_actions),
            'unique_resource_count': len(unique_resources),
            'condition_count': condition_count,
            'complexity_score': len(statements) + len(unique_actions) + len(unique_resources) + condition_count
        }
