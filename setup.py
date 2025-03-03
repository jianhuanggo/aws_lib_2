"""
Setup script for the aws_iam_management package.
"""

from setuptools import setup, find_packages

setup(
    name="aws_iam_management",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.20.0",
        "botocore>=1.23.0",
    ],
    author="AWS IAM Management Team",
    author_email="example@example.com",
    description="A production-grade Python package for managing AWS IAM roles and policies",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/example/aws_iam_management",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
)
