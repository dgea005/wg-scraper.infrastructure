from troposphere import GetAtt, Join, Output
from troposphere import Template
from troposphere.rds import DBInstance
import ruamel_yaml as yaml
import boto3
from utils import create_or_update_stack

with open('../config/rds_config.yml') as f:
    cfg = yaml.load(f)

STACK_NAME = 'ScraperRDS'
INSTANCE_CLASS = cfg['ScraperDatabase']['instance_class']
MASTER_USER_NAME = cfg['ScraperDatabase']['master_user_name']
MASTER_USER_PASSWORD = cfg['ScraperDatabase']['master_user_password']
DB_NAME = cfg['ScraperDatabase']['db_name']


t = Template()
t.add_version("2010-09-09")
t.add_description("CFN template for scraper db")
MyDB = t.add_resource(DBInstance(
    "MyDB",
    Engine="postgres",
    EngineVersion="9.6.5",
    MasterUsername=MASTER_USER_NAME,
    MasterUserPassword=MASTER_USER_PASSWORD,
    AllocatedStorage="5",
    DBInstanceClass=INSTANCE_CLASS,
    DBName=DB_NAME,
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