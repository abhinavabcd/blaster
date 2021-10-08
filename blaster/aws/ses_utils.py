'''
Created on 05-Jun-2019

@author: abhinav
'''
from ..connection_pool import use_connection_pool
from ..logging import LOG_WARN
from ..common_funcs_and_datastructures import retry, background_task

@background_task
@retry(3)
@use_connection_pool(ses_client="ses")
def send_email(sender, to_list, subject, body_text=None, body_html=None, ses_client=None):
    """
    Send email.
    Note: The emails of sender and receiver should be verified.
    PARAMS
    @sender: sender's email, string
    @to: list of receipient emails eg ['a@b.com', 'c@d.com']
    @subject: subject of the email
    @body: body of the email
    """
    try:
        body_data = {}
        if(body_text):
            body_data['Text'] = {
                'Data': body_text,
                'Charset': 'UTF-8'
            }
        if(body_html):
            body_data['Html'] = {
                'Data': body_html,
                'Charset': 'UTF-8'
            }
        
        response = ses_client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': to_list
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': body_data
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return response
        else:
            return None
    except Exception as ex:
        LOG_WARN('ses_error', data=str(ex))
        return None
