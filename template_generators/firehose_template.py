"""
# resources
https://github.com/cloudtools/troposphere/blob/master/troposphere/firehose.py
https://github.com/cloudtools/troposphere/blob/master/examples/Firehose_with_Redshift.py
http://docs.aws.amazon.com/firehose/latest/dev/create-configure.html
http://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#using-iam-s3

template to allow firehose to read from the kinesis and stream and write data to s3

"""
import boto3
from awacs.aws import Allow, Statement, Policy, Action
from troposphere import Ref, Template, GetAtt, Output, iam
from utils import create_or_update_stack, stack_info
import ruamel_yaml as yaml
from troposphere.firehose import (
    KinesisStreamSourceConfiguration,
    DeliveryStream,
    BufferingHints,
    S3DestinationConfiguration
)

with open('../config/firehose_config.yml') as f:
    cfg = yaml.load(f)

STACK_NAME = cfg['firehose']['stack_name']
ACCOUNT_ID = cfg['firehose']['account_id']
BUCKET_NAME = cfg['firehose']['bucket_name']
REGION = cfg['firehose']['region']
STREAM_NAME = stack_info(stack_name='ScraperStreamStack')['StreamName']
STREAM_ARN = stack_info(stack_name='ScraperStreamStack')['StreamARN']

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

# need ARN of this FirehoseExecutionRole
KinesisSource = KinesisStreamSourceConfiguration(
    KinesisStreamARN=STREAM_ARN,
    RoleARN=GetAtt(FirehoseExecutionRole, 'Arn')
)

FirehoseBuffer = BufferingHints(
    IntervalInSeconds=120,
    SizeInMBs=1
)

S3Destination = S3DestinationConfiguration(
    BucketARN=f'arn:aws:s3:::{BUCKET_NAME}',
    BufferingHints=FirehoseBuffer,
    CompressionFormat='GZIP',
    Prefix='firehose/',
    RoleARN=GetAtt(FirehoseExecutionRole, 'Arn')

)

Firehose = t.add_resource(DeliveryStream(
    'KinesisToS3Stream',
    DeliveryStreamName='KinesisToS3Stream',
    DeliveryStreamType='KinesisStreamAsSource',
    KinesisStreamSourceConfiguration=KinesisSource,
    S3DestinationConfiguration=S3Destination
))

t_json = t.to_json(indent=4)

print(t_json)

stack_args = {
    'StackName': STACK_NAME,
    'TemplateBody': t_json,
    'Tags': [
        {
            'Key': 'Purpose',
            'Value': 'ScraperFirehose'
        }
    ],
    'Capabilities': [
        'CAPABILITY_IAM',
    ]
}

cfn = boto3.client('cloudformation')
cfn.validate_template(TemplateBody=t_json)

create_or_update_stack(**stack_args)
