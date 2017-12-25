import boto3
import botocore.exceptions
import logging
logger = logging.getLogger(__name__)

cfn = boto3.client('cloudformation')

def cfn_stack_info(*, stack_name):
    stack_info = {}
    for stack in cfn.describe_stacks(StackName=stack_name)['Stacks']:
        stack_info = {output.get('OutputKey'): output.get('OutputValue') for output in stack['Outputs']}
        stack_info['StackId'] = stack['StackId']
        if 'Parameters' in stack.keys():
            stack_info['Parameters'] = {param.get('ParameterKey'): param.get('ParameterValue') for param in stack['Parameters']}
        stack_info['StackId'] = stack['StackId']
        stack_info['StackName'] = stack['StackName']
    return stack_info

def describe_cfn_stack(*, stack_name):
    return cfn.describe_stacks(StackName=stack_name)

def cfn_stack_events(*, stack_name):
    return cfn.describe_stack_events(StackName=stack_name).get('StackEvents')[0]

def update_cfn_stack(**stack_args):
    cfn.validate_template(TemplateBody=stack_args['TemplateBody'])
    json_output_path = '{}.json'.format(stack_args['StackName'])
    with open(json_output_path, 'wt') as f:
        f.write(stack_args['TemplateBody'])

    try:
        cfn.update_stack(**stack_args)
    except botocore.exceptions.ClientError as err:
        message = err.response['Error']['Message']
        print(message)
        if message == 'No updates are to be performed.':
            pass
        else:
            raise err

def create_cfn_stack(**stack_args):
    cfn.validate_template(TemplateBody=stack_args['TemplateBody'])
    json_output_path = '{}.json'.format(stack_args['StackName'])
    with open(json_output_path, 'wt') as f:
        f.write(stack_args['TemplateBody'])

    try:
        cfn.create_stack(**stack_args)
    except botocore.exceptions.ClientError as err:
        message = err.response['Error']['Message']
        print(message)
        raise err


def create_or_update_cfn_stack(**stack_args):
    cfn.validate_template(TemplateBody=stack_args['TemplateBody'])
    try:
        create_cfn_stack(**stack_args)
    except cfn.exceptions.AlreadyExistsException:
        update_cfn_stack(**stack_args)
