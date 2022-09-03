'''
Created on 05-Jun-2019

@author: abhinav
'''
import os
from ..connection_pool import get_from_pool, release_to_pool
from ..tools import MIME_TYPE_MAP


def generate_upload_url(
    file_name, base_folder, s3_connection_pool_name="s3",
    bucket=None, redirect_url=None, mime_type=None
):
    if(not mime_type):
        # just in case
        extension = os.path.splitext(file_name)[1][1:]
        mime_type = MIME_TYPE_MAP.get(extension)

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

    # Generate the POST attributes
    s3_key = base_folder + "/" + file_name

    s3 = get_from_pool(s3_connection_pool_name)
    post = s3.meta.client.generate_presigned_post(
        Bucket=bucket,
        Key=s3_key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=604800
    )
    release_to_pool(s3, s3_connection_pool_name)

    return post
