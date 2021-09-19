import boto3, json
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
    return a

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
    print(type(msg_att))   
    sqs_client = boto3.client('sqs')
    sqs_queue_url = sqs_client.get_queue_url(
                    QueueName=sqs_queue_name)['QueueUrl'] 
    print(sqs_queue_url)
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageAttributes=msg_att,
                                      MessageBody=json.dumps(msg_body))
    except boto3.botocore.ClientError as e:
        logger.error(e) 
        return None
    return msg
