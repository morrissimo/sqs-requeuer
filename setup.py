#!/usr/bin/env python


import setuptools

setuptools.setup(
    name="sqstoolkit",
    version="1.1.0",
    url='https://github.com/morrissimo/sqs-toolkit',
    author="Robert Morris",
    author_email="robert@emthree.com",
    description="A collection of plumbing tools for working with AWS SQS queues",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "boto3",
    ],
)
