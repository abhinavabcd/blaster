'''
Created on 05-Jun-2019

@author: abhinav
'''
from ...connection_pool import use_connection_pool
from ...logging import LOG_APP_INFO
from ...tools import retry, background_task


send_email = None

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
    from blaster.config import SENDGRID_API_KEY

    @background_task
    @retry(2)
    def send_via_sendgrid(
        from_email, to_list, subject,
        body_text=None, body_html=None, cc_list=None, bcc_list=None,
        sendgrid_api_key=None
    ):
        message = Mail(
            from_email=from_email,
            to_emails=to_list,
            subject=subject,
            html_content=body_html
        )
        sg = SendGridAPIClient(sendgrid_api_key or SENDGRID_API_KEY)
        response = sg.send(message)
        LOG_APP_INFO("sendgrid_send_email", response=str(response))

    send_email = send_via_sendgrid
except ImportError:
    pass


# DEFAULT SES
@background_task
@retry(2)
@use_connection_pool(ses_client="ses")
def send_via_ses(
    from_email, to_list, subject,
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
        Source=from_email,
        Destination={
            "ToAddresses": to_list or [from_email],
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


if(send_email is None):  # default to SES
    send_email = send_via_ses
