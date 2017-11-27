"""
useful http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-events-rule.html
"""

#TODO: clean up imports
import boto3
from troposphere import Template, Ref, Parameter, Tags, Output, GetAtt

from awacs.aws import Allow, Statement, Policy, Action, AWSPrincipal
from troposphere import Template, Parameter, iam
from troposphere.iam import Role
from awacs.s3 import ARN as S3_ARN
from troposphere.awslambda import Function, Code, VPCConfig, Environment, EventSourceMapping
import ruamel_yaml as yaml
from utils import create_or_update_stack, stack_info

with open('../config/lambda_scraper_config.yml') as f:
    cfg = yaml.load(f)


STACK_NAME = cfg['LambdaScraper']['stack_name']
S3_BUCKET = cfg['LambdaScraper']['s3_bucket']
S3_REGION = cfg['LambdaScraper']['s3_region']
SECURITY_GROUP_ID = cfg['LambdaScraper']['security_group']
S3_KEY = cfg['LambdaScraper']['s3_key']
SUBNET_ID = cfg['LambdaScraper']['subnet_id']
STREAM_ARN = stack_info(stack_name='ScraperStreamStack')['StreamARN']



t = Template()
description = 'Stack for scraping and putting records to kinesis'
t.add_description(description)

t.add_version('2010-09-09')


ExistingSecurityGroups = t.add_parameter(Parameter(
    "ExistingSecurityGroups",
    Type="List<AWS::EC2::SecurityGroup::Id>"
))

ExistingSubnets = t.add_parameter(Parameter(
    "ExistingSubnets",
    Type="List<AWS::EC2::Subnet::Id>",
    Description='My VPC subnets'
))



lambda_policy_doc = Policy(
    Statement=[
        Statement(
            Sid='Logs',
            Effect=Allow,
            Action=[Action('logs', 'CreateLogGroup'),
                    Action('logs', 'CreateLogStream'),
                    Action('logs', 'PutLogEvents')],
            Resource=["arn:aws:logs:*:*:*"]
        ),
        # inside vpc there is ENI stuff need to be able to do this
        Statement(
            Sid='ENIs',
            Effect=Allow,
            Action=[
                Action('ec2', 'DescribeNetworkInterfaces'),
                Action('ec2', 'CreateNetworkInterface'),
                Action('ec2', 'DeleteNetworkInterface'),
                Action("kinesis", "DescribeStream"),
                Action("kinesis", "GetRecords"),
                Action("kinesis", "GetShardIterator"),
                Action("kinesis", "ListStreams"),
                Action("kinesis", "PutRecord")
            ],
            # should specify the stream this is for to restrict more
            Resource=["*"]
        ),
    ],
)


ScraperLambdaExecutionRole = t.add_resource(
    iam.Role(
        "ScraperLambdaExecutionRole",
        Path="/",
        AssumeRolePolicyDocument={
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": ["lambda.amazonaws.com"]},
                "Action": ["sts:AssumeRole"]
            }]
        },

        Policies=[
            iam.Policy(
                PolicyName='ScraperLambdaPolicy',
                PolicyDocument=lambda_policy_doc,
            ),
        ]
))


vpc_config = VPCConfig(
    "LambdaVPCConfig",
    SecurityGroupIds=Ref(ExistingSecurityGroups),
    SubnetIds=Ref(ExistingSubnets)
)

LambdaCode = Code(
    "LambdaScraperCode",
    S3Bucket=S3_BUCKET,
    S3Key=S3_KEY
)
# VpcConfig=vpc_config

# Function
WriteToRDSFunction = t.add_resource(Function(
    "WriteToRDSFunction",
    Code=LambdaCode,
    Description="Write data to postgres RDS",
    DependsOn="ScraperLambdaExecutionRole",
    FunctionName="ScraperIndex",
    Handler="lambda_function.lambda_handler",
    Role=GetAtt("ScraperLambdaExecutionRole", "Arn"),
    Runtime="python3.6",
    MemorySize=512,
    Timeout=30
))



t_json = t.to_json(indent=4)

print(t_json)

stack_args = {
    'StackName': STACK_NAME,
    'Parameters': [
        {
            'ParameterKey': 'ExistingSecurityGroups',
            'ParameterValue': SECURITY_GROUP_ID
        },
        {
            'ParameterKey': 'ExistingSubnets',
            'ParameterValue': SUBNET_ID
        }
    ],
    'TemplateBody': t_json,
    'Tags': [
        {
            'Key': 'Purpose',
            'Value': 'Scraping'
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
