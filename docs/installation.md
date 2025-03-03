# Installation Guide

This guide provides instructions for installing the `aws_iam_management` package.

## Prerequisites

Before installing the `aws_iam_management` package, ensure you have the following prerequisites:

1. Python 3.6 or higher
2. pip (Python package installer)
3. AWS account with appropriate permissions
4. AWS credentials configured on your system

## Installation Methods

### From PyPI (Recommended)

The easiest way to install the `aws_iam_management` package is from PyPI:

```bash
pip install aws-iam-management
```

### From Source

To install the package from source:

1. Clone the repository:

```bash
git clone https://github.com/yourusername/aws-iam-management.git
```

2. Navigate to the project directory:

```bash
cd aws-iam-management
```

3. Install the package:

```bash
pip install -e .
```

## AWS Credentials Configuration

The `aws_iam_management` package requires AWS credentials to interact with AWS services. You can configure your AWS credentials in several ways:

### Using AWS CLI

If you have the AWS CLI installed, you can configure your credentials by running:

```bash
aws configure
```

This will prompt you to enter your AWS Access Key ID, Secret Access Key, default region, and output format.

### Using Environment Variables

You can set the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=your_region
```

### Using Credentials File

You can create a credentials file at `~/.aws/credentials` with the following content:

```
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
```

And a config file at `~/.aws/config` with the following content:

```
[default]
region = your_region
```

## Verifying Installation

To verify that the `aws_iam_management` package is installed correctly, you can run the following Python code:

```python
import aws_iam_management
print(aws_iam_management.__version__)
```

This should print the version number of the installed package.

## Troubleshooting

If you encounter any issues during installation, try the following:

1. Ensure you have the latest version of pip:

```bash
pip install --upgrade pip
```

2. If you're installing from source, ensure you have the required build tools:

```bash
pip install wheel setuptools
```

3. If you're having issues with AWS credentials, verify that your credentials are correctly configured:

```python
import boto3
session = boto3.Session()
print(session.get_credentials().access_key)
```

This should print your AWS access key. If it doesn't, your credentials are not correctly configured.

## Next Steps

After installing the `aws_iam_management` package, you can start using it to manage IAM roles and policies. See the [Usage Guide](usage.md) for examples of how to use the package.
