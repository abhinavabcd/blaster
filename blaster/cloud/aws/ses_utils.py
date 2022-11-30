'''
Created on 05-Jun-2019

@author: abhinav
'''
from ...connection_pool import use_connection_pool
from ...logging import LOG_WARN
from ...tools import retry, background_task


@background_task
@retry(3)
@use_connection_pool(ses_client="ses")
def send_email(
    sender, to_list, subject,
    body_text=None, body_html=None, cc_list=None, bcc_list=None, ses_client=None
):
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
                "ToAddresses": to_list or [sender],
                "CcAddresses": cc_list or [],
                "BccAddresses": bcc_list or []
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
