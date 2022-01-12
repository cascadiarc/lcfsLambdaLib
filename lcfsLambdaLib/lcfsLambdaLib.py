import boto3, json, io, dropbox, urllib3,logging,os
from urllib.error import HTTPError
from types import MappingProxyType
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from tabulate import tabulate

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

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
        a[path] = param1['Parameter']['Value'].rstrip()
        logger.debug(f'Sucessfully pulled {a[path]} from {path}')
    else:
        for i in path:
            logger.debug(f'Lookup: {i}')
            param1 = ssm_client.get_parameter(Name=i, WithDecryption=enc)
            logger.debug(f'Sucessfully pulled value from {i}')
            a[i] = param1['Parameter']['Value'].rstrip()
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
                                      MessageBody=msg_body)
    except ClientError as e:
        logger.debug(f'Gor error: {e}')
        logger.error(e) 
        return None
    return msg

def send_sqs_fifo_message(logger,sqs_queue_name, msg_att, msg_body,gid):
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
                                      MessageBody=json.dumps(msg_body),
                                      MessageGroupId=gid)
    except ClientError as e:
        logger.error(e) 
        return None
    return msg

def send_sns_message(logger,topic_arn,subject,message,m_struct,m_attr):
    '''
    This function publishes an SNS message to the SNS topic
    :param1: logger=the debugger log handle
    :param2: topic_arn=this is the aws arn of the topic
    :param3: message=this is string of the message to be sent 
    :param4: subject=this is the subject line
    :param5: m_struct=this is the message format typically json
    :param6: m_attr=this is a dict of message attributes
    :return: repsonse=either the dict of messageId and SequenceNumber
    '''

    logger.info(f'Calling {topic_arn} for message')
    logger.debug(f'Message: {message}')
    logger.debug(f'MessageStructure: {m_struct}')
    logger.debug(f'MessageAttributes: {m_attr}')
    sns_client = boto3.client('sns')
    if m_struct == 'json':
        message = json.dumps({'default': json.dumps(message)})

    try:
        response = sns_client.publish(
            TopicArn = topic_arn,
            Subject= subject,
            Message = message,
            MessageStructure = m_struct,
            MessageAttributes = m_attr
        )
        logger.info(f'Message sucessfully sent to {topic_arn}')
    except Exception as e:
        logger.debug(f'Got Exception {e}')
        response = f'Check Logs. Exception trying to publish message to {topic_arn}'
    
    return response

def read_s3_file(logger,bucket,key,encoding):
    '''
    This function takes a bucket and a key and returns the data from the s3 file
    :param1: bucket=the bucket name that the file resides in
    :param2: key=the key to the file, which in s3 is the path and filename in one
    :param3: type=this is the file type
    :returns: contents=the output of the s3 read command
    '''
    s3_client = boto3.client('s3')
    # Get s3 object contents based on bucket name and object key; in bytes and convert to string
    try:
        data = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logger.info(f'Error reading {bucket}/{key}: {e.data["Error"]["Code"]}')
    #io.BytesIO(data.get('Body').read())
    contents = data['Body'].read().decode(encoding)    

    return contents

def pns_msg(logger,subject,msg):
    '''
    This function gets called by process_s3. It preps and sends the SNS
    message
    :param1: subject=this is the subject of the notification
    :param2: msg=this is the message in json format
    :param3: ***msgAtt=this is a kwags array of message attributes
    :returns: no return
    '''
    #set our MessageAttributes so we can filter off the SNS message
    logger.info(f'Setting up our Message')
    m = json.dumps(msg)
    n = ['default', m]
    msg = json.dumps(n)
    ms = 'json'
    ma ={
            "application" : {
                "DataType" : "String",
                "StringValue" : "print-checks"
            },
            "error" : {
                "DataType" : "String",
                "StringValue" : "Billpay json has unexpected data"
            }      
    }

    ms = json.dumps(ms)
    #resp = lcfsLambdaLib.send_sns_message(logger,tp_arn,msg,ms,ma)
    #logger.debug(dir(lcfsLambdaLib))
    tp_arn='arn:aws:sns:us-west-2:386461531385:application_error'
    resp = send_sns_message(logger,tp_arn,subject,msg,ms,ma)
    if resp['MessageId']:
        logger.info(f'{resp["MessageId"]} published to recievers.')
    else:
        logger.debug(f'Recieved Error {resp["MessageId"]}')

def ssm_params(**kw):
    '''This function will CRU parameters from SSM
    :param: type=whether read, create, update
    :param: kw
            path=a list or string of the variable path
            enc=either true or false depending on if encryption is used
            name,desc,value,type= values necessary for create and update
    :returns: nothing if an error, otherwise the standard SMS response
    '''
    t = kw['t']
    logger = kw['logger']
    logger.debug(f'Starting parameter pull for parm type {t}')
    logger.debug(f'Getting parameter(s) {kw}')
    a = {}
    ssm_client = boto3.client('ssm')
    if t == 'r':
        path = kw['name']
        enc = kw['enc']
        if isinstance(path, str):
            param1 = ssm_client.get_parameter(Name=path, WithDecryption=enc)
            a = param1['Parameter']['Value']
            logger.debug(f'Sucessfully pulled value from {a}')
            return a
        else:
            for i in path:
                param1 = ssm_client.get_parameter(Name=i, WithDecryption=enc)
                logger.debug(f'Sucessfully pulled value from {param1}')
                a[i] = param1['Parameter']['Value']
                logger.debug(f'Sucessfully pulled value from {a[i]}')
                return a
    elif t == 'c':
        name = kw['name']
        desc = kw['desc']
        v = kw['v']
        pt = kw['parm_type']
        resp = ssm_client.put_parameter(
                    Name=name,
                    Description=desc,
                    Value=v,
                    Type=pt,
                    Tags=[
                        {
                            'Key' : 'cost_center',
                            'Value': 'left coast'
                        }]
                )
        logger.debug(f'SSM parameter(s) done: {resp}')
        return resp
    elif t == 'u':
        name = kw['name']
        v = kw['v']
        resp = ssm_client.put_parameter(
                    Name=name,
                    Value=v,
                    Overwrite=True
                )
        logger.debug(f'SSM parameter(s) done: {resp}')
        return resp
    else:
        logger.debug(f'Error, no SSM type provided: {type}')

def dau_create_folder(logger,dbx_as_user,folder_path,folder_name):
    '''This function calls dropbox to create a folder
    :param: logger=the logging handle
    :param: dbx_as_user=the dropbox session
    :param: fodler_path=the path the folder will be created in
    :param: folder_name=the name of the folder to create
    :returns: the path to the new folder
    '''
    logger.info(f'Got a dropbox folder create request')
    fn = f'{folder_path}{folder_name}'
    logger.info(f'Creating new folder {folder_name} in {folder_path}')
    a = dbx_as_user.files_create_folder_v2(fn)
    logger.info(f'Folder creation complete')
    logger.debug(f'Folder created {a.path_display}')
    return a.path_display

def dau_copy_to(logger,bn,bk,dbx_as_user,dbx_path):
    '''This function takes s3 file and copies it to dropbox
    :param: logger=our logging handle
    :param: bn=s3 bucket name to read from
    :param: bk=s3 bucket key to read from
    :param: dbx_as_user=the dropbox session
    :param: dbx_path=the path that the file will be copied to and filename
    :returns: the path and fielname in dropbox
    '''
    logger.info(f'Got a request to copy files from s3 to dropbox')
    s3_client = boto3.client('s3')
    try:
        data = s3_client.get_object(Bucket=bn, Key=bk)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        logger.debug(f'Was not able to read s3://{bn}{bk}')
        logger.debug(f'Recived error code {error_code}')
        return
    logger.info(f'sucessfully read in s3://{bn}{bk}')
    try:
        a = dbx_as_user.files_upload(data['Body'].read(), dbx_path)
        logger.debug(f'Sucessfully uploaded {a.path_display}')
    except dropbox.exceptions.ApiError as e:
        logger.debug(f'Recived error {e}')
        return
    logger.info(f'sucessfully read in s3://{bn}{bk}')
    return a.path_display

def dau_to_s3(logger,dbx_as_user,dbx_path,dbx_filename,bk,bn):
    '''This function takes a dropbox file and copies it to s3
    :param: logger=the logging handle
    :param: bn=s3 bucket name to read from
    :param: bk=s3 bucket key to read from
    :param: dbx_as_user=the dropbox session
    :param: dbx_path=the path that the file will be copied to
    :param: dbx_file_name=the naem of the file in dbx
    :returns: the bucket and key in s3
    '''
    logger.info(f'Got a request to copy files from dropbox to s3')
    s3_client = boto3.client('s3')
    #s3_resource = boto3.resource('s3')
    DBX_PATH = f'{dbx_path}{dbx_filename}'
    #grab our file from the dropbox side
    try:
        meta, res = dbx_as_user.files_download(DBX_PATH)
        logger.debug(f'Sucessfully pulled down {meta.name} with response {res}')
    except dropbox.exceptions.ApiError as e:
        logger.debug(f'Recived error {e}')
        return
    logger.info(f'sucessfully downloaded {meta.name}')

    #getting a bucket object
    s3_client.upload_fileobj(io.BytesIO(res.content), bn, bk)
    logger.info(f'sucessfully read in s3://{bn}{bk}')
    return bk

def get_token(logger,url,apiKey,secret):
    '''
    This function calls the Left Coast encryption service to either return the token
    for the given hash value, or create a new token from the plaintext value and return
    that
    :param:     url=this is the encryption service URL
    :param:     json=this is the plaintext parameter of the secret in json format
    :returns:   a token that represents the secret
    '''
    logger.info(f'Calling our encryption service to get a token')
    encoded_body = json.dumps({"plaintext_item": secret})
    try:
        http = urllib3.PoolManager()
        h={'x-api-key':apiKey}
        response = http.request('POST',url,
                                headers=h,
                                body=encoded_body)
    except HTTPError as e:
        logger.info(f'Http request threw error {e}')
        logger.debug(f'URL called: {url}')
        logger.debug(f'Header passed: {h}')
        return f'ERROR: {e}'
    
    item = json.loads(response.data)
    logger.info(f'This is the item returned: {item}')
    return item["token"]

def get_tv(logger,url,apiKey,hash):
    '''
    This function calls the Left Coast decryption service to get the plaintext value from a hash
    :param:     url=this is the encryption service URL
    :param:     apiKey=this is the apiKey used to access the API gateway endpoint
    :param:     hash=this is hte hash of the vault entry
    :returns:   the plaintext value
    '''
    logger.info(f'Calling our encryption service to get a token')
    unencoded_body = json.dumps({"hash": hash})
    try:
        http = urllib3.PoolManager()
        h={'x-api-key':apiKey}
        response = http.request('POST',url,
                                headers=h,
                                body=unencoded_body)
    except HTTPError as e:
        logger.info(f'Http request threw error {e}')
        logger.debug(f'URL called: {url}')
        logger.debug(f'Header passed: {h}')
        return f'ERROR: {e}'
    
    item = json.loads(response.data)
    logger.info(f'This is the item returned: {item}')
    return item["plaintext_value"]

def get_hash(logger,url,apiKey,token):
    '''
    This function calls the Left Coast encryption service to get the hash value for
    a provided token
    :param:     url=this is the encryption service URL
    :param:     apiKey=this is the apiKey used to access the API gateway endpoint
    :param:     token=this is the token used to get the hash value
    :returns:   a hash value that is the key in the vault for the plaintext value
    '''
    logger.info(f'Calling our encryption service to get a hash')
    t = json.dumps({"token": token})
    try:
        http = urllib3.PoolManager()
        h={'x-api-key':apiKey}
        response = http.request('POST',url,
                                headers=h,
                                body=t)
    except HTTPError as e:
        logger.info(f'Http request threw error {e}')
        logger.debug(f'URL called: {url}')
        logger.debug(f'Header passed: {h}')
        return f'ERROR: {e}'
    
    item = json.loads(response.data)
    logger.info(f'This is the item returned: {item}')
    return item["hash"]

def create_multipart_message(
        sender: str, recipients: list, title: str, cc: list=None, text: str=None, html: str=None, bcc: list=None, attachments: list=None)\
        -> MIMEMultipart:
    """
    Creates a MIME multipart message object.
    Uses only the Python `email` standard library.
    Emails, both sender and recipients, can be just the email string or have the format 'The Name <the_email@host.com>'.

    :param sender: The sender.
    :param recipients: List of recipients. Needs to be a list, even if only one recipient.
    :param title: The title of the email.
    :param text: The text version of the email body (optional).
    :param html: The html version of the email body (optional).
    :param attachments: List of files to attach in the email.
    :return: A `MIMEMultipart` to be used to send the email.
    """
    logger.info(f'Creating multipart MIME message')
    logger.debug(f'cc list: {type(cc)}')
    multipart_content_subtype = 'alternative' #if text and html else 'mixed'
    msg = MIMEMultipart(multipart_content_subtype)
    msg['Subject'] = title
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['CC'] = ', '.join(cc)
    if bcc:
        msg['BCC'] = ', '.join(bcc)

    # Record the MIME types of both parts - text/plain and text/html.
    # According to RFC 2046, the last part of a multipart message, in this case the HTML message, is best and preferred.
    if text:
        part = MIMEText(text, 'plain')
        msg.attach(part)
    if html:
        part = MIMEText(html, 'html')
        msg.attach(part)

    # Add attachments
    for attachment in attachments or []:
        with open(attachment, 'rb') as f:
            part = MIMEApplication(f.read())
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            msg.attach(part)

    return msg

def send_mail(
        sender: str, recipients: list, title: str, cc: list=None, text: str=None, html: str=None, bcc: list=None, attachments: list=None) -> dict:
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    msg = create_multipart_message(sender, recipients, title, cc, text, html, bcc, attachments)
    ses_client = boto3.client('ses')  # Use your settings here
    return ses_client.send_raw_email(
        Source=sender,
        Destinations=recipients,
        RawMessage={'Data': msg.as_string()}
    )