'''
Created on 05-Jun-2019

@author: abhinav
'''
import os
from ..connection_pool import get_from_pool, release_to_pool
from ..utils.data_utils import FILE_EXTENSION_TO_MIME_TYPE
from ..config import UPLOADS_S3_BUCKET, UPLOADS_S3_CLIENT_POOL

def generate_upload_url(
    file_path, 
    s3_connection_pool_name=UPLOADS_S3_CLIENT_POOL, bucket=UPLOADS_S3_BUCKET,
    redirect_url=None, mime_type=None
):
    if(not mime_type):
        # just in case
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
    
    s3 = get_from_pool(s3_connection_pool_name)
    post = s3.meta.client.generate_presigned_post(
        Bucket=bucket,
        Key=file_path,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=604800
    )
    release_to_pool(s3, s3_connection_pool_name)

    return post
