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
from troposphere.awslambda import Function, Code, VPCConfig, Environment
import ruamel_yaml as yaml
from utils import create_or_update_stack, stack_info

with open('../config/lambda_writer_config.yml') as f:
    cfg = yaml.load(f)


STACK_NAME = cfg['LambdaWriter']['stack_name']
S3_BUCKET = cfg['LambdaWriter']['s3_bucket']
S3_REGION = cfg['LambdaWriter']['s3_region']
SECURITY_GROUP_ID = cfg['LambdaWriter']['security_group']
S3_KEY = cfg['LambdaWriter']['s3_key']
SUBNET_ID = cfg['LambdaWriter']['subnet_id']
RDS_ENDPOINT = stack_info(stack_name=cfg['LambdaWriter']['rds_stack'])['DBEndpoint']

with open('../config/rds_config.yml') as f:
    cfg = yaml.load(f)
RDS_USER_NAME = cfg['ScraperDatabase']['master_user_name']
RDS_USER_PASSWORD = cfg['ScraperDatabase']['master_user_password']


t = Template()
description = 'Stack for writing from lambda to rds'
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
                Action('ec2', 'DeleteNetworkInterface')
            ],
            Resource=["*"]
        )
    ],
)

LambdaExecutionRole = t.add_resource(
    iam.Role(
        "LambdaExecutionRole",
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
                PolicyName='lambdaPolicy',
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
    "LambdaWriterCode",
    S3Bucket=S3_BUCKET,
    S3Key=S3_KEY
)

# or should these be params?
EnvironmentVars = Environment(
    "LambdaEnvs",
    Variables={
        "db_user": RDS_USER_NAME,
        "db_pass": RDS_USER_PASSWORD,
        "db_endpoint": RDS_ENDPOINT
    }

)

# Function
WriteToRDSFunction = t.add_resource(Function(
    "WriteToRDSFunction",
    Code=LambdaCode,
    Description="Write data to postgres RDS",
    DependsOn="LambdaExecutionRole",
    Environment=EnvironmentVars,
    FunctionName="WriteToRDSFunction",
    Handler="lambda_function.lambda_handler",
    Role=GetAtt("LambdaExecutionRole", "Arn"),
    Runtime="python3.6",
    MemorySize=512,
    Timeout=30,
    VpcConfig=vpc_config
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
