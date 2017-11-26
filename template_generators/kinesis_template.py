import boto3
from troposphere import Output
from troposphere import Ref, Template, GetAtt
import troposphere.kinesis as kinesis
from utils import create_or_update_stack

STACK_NAME = 'ScraperStreamStack'

t = Template()
description = 'Stack for kinesis stream for scraper data'
t.add_description(description)

kinesis_stream = t.add_resource(kinesis.Stream(
    "ScraperStream",
    Name="ScraperStream",
    ShardCount=1,
    RetentionPeriodHours=24
))

t.add_output([
    Output(
        "StreamName",
        Description="Stream Name (Physical ID)",
        Value=Ref(kinesis_stream),
    ),
])

t.add_output([
    Output(
        "StreamARN",
        Description="Stream Name (Physical ID)",
        Value=GetAtt(kinesis_stream, "Arn"),
    ),
])

t_json = t.to_json(indent=4)

print(t_json)

stack_args = {
    'StackName': STACK_NAME,
    'TemplateBody': t_json,
    'Tags': [
        {
            'Key': 'Purpose',
            'Value': 'ScrapedDataStreaming'
        }
    ]
}

cfn = boto3.client('cloudformation')
cfn.validate_template(TemplateBody=t_json)

create_or_update_stack(**stack_args)