"""
# resources
https://github.com/cloudtools/troposphere/blob/master/troposphere/firehose.py
https://github.com/cloudtools/troposphere/blob/master/examples/Firehose_with_Redshift.py
http://docs.aws.amazon.com/firehose/latest/dev/create-configure.html
http://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#using-iam-s3

template to allow firehose to read from the kinesis and stream and write data to s3

"""
from awacs.aws import Allow, Statement, Policy, Action
from troposphere import Ref, Template, GetAtt, Output, iam
from utils import create_or_update_stack, stack_info
import ruamel_yaml as yaml
from troposphere.firehose import (
    DeliveryStream,
    EncryptionConfiguration,
    S3Configuration,
)

with open('../config/firehose_config.yml') as f:
    cfg = yaml.load(f)

STACK_NAME = cfg['firehose']['stack_name']
ACCOUNT_ID = cfg['firehose']['account_id']
BUCKET_NAME = cfg['firehose']['bucket_name']
REGION = cfg['firehose']['region']
STREAM_NAME = stack_info(stack_name='ScraperStreamStack')['StreamName']

t = Template()
description = 'Stack for kinesis firehose stream to deliver to s3 from kinesis'
t.add_description(description)
t.add_version('2010-09-09')


firehose_policy_doc = Policy(
    Statement=[
        Statement(
            Sid='FirehoseS3Statement',
            Effect=Allow,
            Action=[
                Action("s3", "AbortMultipartUpload"),
                Action("s3", "GetBucketLocation"),
                Action("s3", "GetObject"),
                Action("s3", "ListBucket"),
                Action("s3", "ListBucketMultipartUploads"),
                Action("s3", 'PutObject')
            ],
            Resource=[
                f"arn:aws:s3:::{BUCKET_NAME}",
                f"arn:aws:s3:::{BUCKET_NAME}/*"
            ]
        ),
        Statement(
            Sid='FirehoseKinesisStatement',
            Effect=Allow,
            Action=[
                Action("kinesis", "DescribeStream"),
                Action("kinesis", "GetRecords"),
                Action("kinesis", "GetShardIterator")
            ],
            Resource=[f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{STREAM_NAME}"]
        )
    ],
)

FirehoseExecutionRole = t.add_resource(
    iam.Role(
        "FirehoseExecutionRole",
        Path="/",
        AssumeRolePolicyDocument={
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": ["firehose.amazonaws.com"]},
                "Action": ["sts:AssumeRole"],
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": ACCOUNT_ID ## restrict to only my account id to trigger this
                    }
                }
            }]
        },

        Policies=[
            iam.Policy(
                PolicyName='FirehosePolicy',
                PolicyDocument=firehose_policy_doc,
            ),
        ]
))