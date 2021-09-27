import boto3, json
from types import MappingProxyType

'''This file contains code reused in all our lambdas'''

def get_ssm_params(logger,path,enc):
    '''This function will pull a parameter from SSM
    Path is either a list or a string for a single return
    :params path=a list or string of the variable path
    :params enc=either true or false depending on if encryption is used
    '''
    logger.debug(f'Starting parameter pull')
    logger.debug(f'Getting parameter(s) {path} with encryption set to {enc}')
    a = {}
    ssm_client = boto3.client('ssm')
    if isinstance(path, str):
        param1 = ssm_client.get_parameter(Name=path, WithDecryption=enc)
        a[path] = param1['Parameter']['Value']
        logger.debug(f'Sucessfully pulled {a[path]} from {path}')
    else:
        for i in path:
            param1 = ssm_client.get_parameter(Name=i, WithDecryption=enc)
            logger.debug(f'Sucessfully pulled value from {i}')
            a[i] = param1['Parameter']['Value']
    logger.debug(f'SSM parameter(s) done')

    frozen_dict = MappingProxyType(a)
    return frozen_dict

def send_sqs_message(logger,sqs_queue_name, msg_att, msg_body):
    """
    This function creates our SQS message
    :param sqs_queue_name: Name of existing SQS URL
    :param masg_att: String message attributes
    :param msg_body: String message body
    :return: Dictionary containing information about the sent message. If
        error, returns None.
    """

    # Send the SQS message
    #sqs_client = boto3.client('sqs')
    logger.debug(type(msg_att))   
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(
                    QueueName=sqs_queue_name)['QueueUrl'] 
    logger.debug(sqs_queue_url)
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageAttributes=msg_att,
                                      MessageBody=json.dumps(msg_body))
    except boto3.botocore.ClientError as e:
        logger.error(e) 
        return None
    return msg

def send_sns_message(logger,topic_arn,message,m_struct,m_attr):
    '''
    This function publishes an SNS message to the SNS topic
    :param1: logger=the debugger log handle
    :param2: topic_arn=
    :param3: message=
    :param4: m_struct=
    :param5: m_attr=
    :return: repsonse=either the dict of messageId and SequenceNumber
    '''

    logger.info(f'Calling {topic_arn} for message')
    logger.debug(f'Message: {message}')
    logger.debug(f'MessageStructure: {m_struct}')
    logger.debug(f'MessageAttributes: {m_attr}')
    sns_client = boto3.client('sns')
    try:
        response = sns_client.publish(
            TopicArn = topic_arn,
            Message = message,
            MessageStructure = m_struct,
            MessageAttributes = m_attr
        )
        logger.info(f'Message sent to {topic_arn}')
    except Exception as e:
        logger.info(f'Got Exception {e}')
        response = f'Exception trying to publish message to {topic_arn}'
    
    return response

def read_s3_file(logger,bucket,key,tp):
    '''
    This function takes a bucket and a key and returns the data from the s3 file
    :param1: bucket=the bucket name that the file resides in
    :param2: key=the key to the file, which in s3 is the path and filename in one
    :param3: type=thsi is the file type
    :returns: contents=the output of the s3 read command
    '''
    # Get s3 object contents based on bucket name and object key; in bytes and convert to string
    try:
        data = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logger.info(f'Error reading {bucket}/{key}: {e.response["Error"]["Code"]}')
    contents = data['Body'].read().decode("utf-8")    

    return contents