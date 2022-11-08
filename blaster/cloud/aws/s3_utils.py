'''
Created on 05-Jun-2019

@author: abhinav
'''
import os
from ...connection_pool import get_from_pool, release_to_pool
from ...utils.data_utils import FILE_EXTENSION_TO_MIME_TYPE


def generate_s3_upload_url(
    file_path, s3_bucket, s3_client_pool_name, redirect_url=None, mime_type=None
):
    if(not mime_type):
        mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]

    fields = {
        "acl": "public-read",
        "Content-Type": mime_type,
        "success_action_status": "200"
    }

    conditions = [
        {"acl": "public-read"},
        {"Content-Type": mime_type},
        {"success_action_status": "200"}
    ]
    if(redirect_url):
        conditions.append({"redirect": redirect_url})

    s3_client = get_from_pool(s3_client_pool_name)
    post = s3_client.generate_presigned_post(
        Bucket=s3_bucket,
        Key=file_path,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=604800
    )
    release_to_pool(s3_client, s3_client_pool_name)

    return post
