"""
useful http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-events-rule.html
"""

import boto3
from troposphere import Template, Ref, Parameter, Tags, Output, GetAtt
from awacs.aws import Allow, Statement, Policy, Action, AWSPrincipal, Role
from cfn_flip import to_yaml
from troposphere import Template, Parameter, iam
from awacs.s3 import ARN as S3_ARN
from troposphere.awslambda import Function, Code
import ruamel_yaml as yaml
from utils import create_or_update_stack

with open('../config/lambda_writer_config.yml') as f:
    cfg = yaml.load(f)


STACK_NAME = cfg['LambdaWriter']['stack_name']
S3_BUCKET = cfg['LambdaWriter']['s3_bucket']
S3_REGION = cfg['LambdaWriter']['s3_region']

t = Template()
description = 'Stack for writing from lambda to rds'
t.add_description(description)

t.add_version('2010-09-09')

LambdaExecutionRole = t.add_resource(Role(
    "LambdaExecutionRole",
    Path="/",
    Policies=[Policy(
        PolicyName="root",
        PolicyDocument={
            "Version": "2012-10-17",
            "Statement": [{
                "Action": ["logs:*"],
                "Resource": "arn:aws:logs:*:*:*",
                "Effect": "Allow"
            }]
        })],
    AssumeRolePolicyDocument={
        "Version": "2012-10-17",
        "Statement": [{
            "Action": ["sts:AssumeRole"],
            "Effect": "Allow",
            "Principal": {
                "Service": ["lambda.amazonaws.com"]
            }
        }]
    },
))

t_json = t.to_json(indent=4)

print(t_json)

stack_args = {
    'StackName': STACK_NAME,
    'TemplateBody': t_json,
    'Tags': [
        {
            'Key': 'Purpose',
            'Value': 'Scraper RDS'
        }
    ],
    'Capabilities': [
        'CAPABILITY_IAM',
    ]
}

cfn = boto3.client('cloudformation')
#print(t_json)
cfn.validate_template(TemplateBody=t_json)

create_or_update_stack(**stack_args)
