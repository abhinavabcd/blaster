'''
Created on 05-Jun-2019

@author: abhinav
'''
from ...connection_pool import use_connection_pool
from ...logging import LOG_APP_INFO
from ...tools import retry, background_task
from blaster.config import SENDGRID_API_KEY, PREFERED_EMAIL_SERVICE


if(SENDGRID_API_KEY and (not PREFERED_EMAIL_SERVICE or PREFERED_EMAIL_SERVICE == "sendgrid")):
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    @background_task
    @retry(2)   
    def send_email(
        sender, to_list, subject, 
        body_text=None, body_html=None, cc_list=None, bcc_list=None,
    ):
        message = Mail(
            from_email=sender,
            to_emails=to_list,
            subject=subject,
            html_content=body_html
        )
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)    
        LOG_APP_INFO("sendgrid_send_email", response=str(response))

elif(not PREFERED_EMAIL_SERVICE or PREFERED_EMAIL_SERVICE == "ses"):
    # DEFAULT SES
    @background_task
    @retry(2)
    @use_connection_pool(ses_client="ses")
    def send_email(
        sender, to_list, subject,
        body_text=None, body_html=None, cc_list=None, bcc_list=None,
        ses_client=None
    ):
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
        LOG_APP_INFO("ses_send_email", response=str(response))
