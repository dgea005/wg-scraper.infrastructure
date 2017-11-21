from troposphere import GetAtt, Join, Output, Ref
from troposphere import Template, ec2, Parameter
from troposphere.rds import DBInstance, DBSecurityGroup
import ruamel_yaml as yaml
import boto3
import requests
from utils import create_or_update_stack

with open('../config/rds_config.yml') as f:
    cfg = yaml.load(f)

STACK_NAME = 'ScraperRDS'
INSTANCE_CLASS = cfg['ScraperDatabase']['instance_class']
MASTER_USER_NAME = cfg['ScraperDatabase']['master_user_name']
MASTER_USER_PASSWORD = cfg['ScraperDatabase']['master_user_password']
DB_NAME = cfg['ScraperDatabase']['db_name']
VPC_ID = cfg['ScraperDatabase']['vpc_id']


t = Template()
t.add_version("2010-09-09")
t.add_description("CFN template for scraper db")

r = requests.get('http://checkip.amazonaws.com/')
result = [x for x in r.text if x is not '\n']
my_ip = ''.join(result)

db_security_group = t.add_resource(ec2.SecurityGroup(
    'DatabaseSecurityGroup',
    GroupDescription="Database security group.",
    VpcId=VPC_ID,
    SecurityGroupIngress=[
        ec2.SecurityGroupRule(
            IpProtocol="tcp",
            FromPort="5432",
            ToPort="5432",
            CidrIp=f"{my_ip}/32"
        ),
    ],
))


MyDB = t.add_resource(DBInstance(
    "MyDB",
    Engine="postgres",
    EngineVersion="9.6.5",
    MasterUsername=MASTER_USER_NAME,
    MasterUserPassword=MASTER_USER_PASSWORD,
    AllocatedStorage="5",
    DBInstanceClass=INSTANCE_CLASS,
    DBName=DB_NAME,
    VPCSecurityGroups=[Ref(db_security_group)]
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



print(t.to_json())