import boto3
import botocore.exceptions
import requests
import ruamel_yaml as yaml
from pkg_resources import resource_string
import logging
logger = logging.getLogger(__name__)

cfn = boto3.client('cloudformation')


def stack_info(*, stack_name):
    """Read cfn_templates from existing stacks

    Examples
    --------
    g = stack_info(STACK_NAME).get('AutoScalingGroupName')
    """
    info = {}
    for stack in cfn.describe_stacks(StackName=stack_name)['Stacks']:
        info = {o.get('OutputKey'): o.get('OutputValue') for o in stack['Outputs']}
        info['StackId'] = stack['StackId']
        # sometime we have any parameters in the stack
        if 'Parameters' in stack.keys():
            params = {p.get('ParameterKey'): p.get('ParameterValue') for p in stack['Parameters']}
            info['Parameters'] = params
        info['StackId'] = stack['StackId']
        info['StackName'] = stack['StackName']
    return info


def describe_stack(*, stack_name):
    return cfn.describe_stacks(StackName=stack_name)


def stack_events(*, stack_name):
    return cfn.describe_stack_events(StackName=stack_name).get('StackEvents')[0]


def update_stack(**stack_args):
    """Validate template, write json cfn_templates, update stack

    Parameters
    ----------
    stack_args : dict{'StackName':str,
                      'TemplateBody': str,
                      'Tags': list('Key': str,
                      'Parameters': list(dict)}


    """
    cfn.validate_template(TemplateBody=stack_args['TemplateBody'])
    json_output_path = 'templates/json/{}.json'.format(stack_args['StackName'])
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


def create_stack(**stack_args):
    """Validate template, write json cfn_templates, create stack

    Parameters
    ----------
    stack_args : dict{'StackName':str,
                      'TemplateBody': str,
                      'Tags': list('Key': str,
                      'Parameters': list(dict)}


    """
    cfn.validate_template(TemplateBody=stack_args['TemplateBody'])
    json_output_path = 'templates/json/{}.json'.format(stack_args['StackName'])
    with open(json_output_path, 'wt') as f:
        f.write(stack_args['TemplateBody'])

    try:
        cfn.create_stack(**stack_args)
    except botocore.exceptions.ClientError as err:
        message = err.response['Error']['Message']
        print(message)
        raise err


def create_or_update_stack(**stack_args):
    """Validate template, write json cfn_templates, create or update stack

    Parameters
    ----------
    stack_args : dict{'StackName':str,
                      'TemplateBody': str,
                      'Tags': list('Key': str,
                      'Parameters': list(dict)}
    """
    cfn.validate_template(TemplateBody=stack_args['TemplateBody'])
    try:
        create_stack(**stack_args)
    except cfn.exceptions.AlreadyExistsException:
        update_stack(**stack_args)


