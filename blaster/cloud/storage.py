'''
Created on 05-Jun-2019

@author: abhinav
'''

import os
from datetime import timedelta
from ..connection_pool import use_connection_pool
from ..utils.data_utils import FILE_EXTENSION_TO_MIME_TYPE
from ..config import UPLOADS_S3_BUCKET, UPLOADS_S3_CLIENT_POOL_NAME,\
    UPLOADS_S3_BUCKET_REGION, \
    UPLOADS_GCLOUD_BUCKET


if(UPLOADS_GCLOUD_BUCKET):
    @use_connection_pool(gcloud_storage="google_cloudstorage")
    def generate_upload_url(file_path, mime_type=None, gcloud_storage=None):
        if(not mime_type):
            mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]

        bucket = gcloud_storage.bucket(UPLOADS_GCLOUD_BUCKET)
        blob = bucket.blob(file_path)  # name of file to be saved/uploaded to storage
        url = blob.generate_signed_url(
            version='v4',
            expiration=timedelta(minutes=30),
            method='PUT',
            content_type="application/octet-stream",
            headers={"x-goog-acl": "public-read"}
        )
        return {
            "url": url,
            "method": "PUT",
            # after shit ton of trial and error, this started working
            # if you wish to change, be careful to test it
            "headers": {
                "x-goog-acl": "public-read",
                "content-type": "application/octet-stream"
            },
        }, f"https://storage.googleapis.com/{UPLOADS_GCLOUD_BUCKET}/{file_path}"


# decide uploading via s3 or gcloud
elif(UPLOADS_S3_CLIENT_POOL_NAME and UPLOADS_S3_BUCKET):
    @use_connection_pool(s3_client=UPLOADS_S3_CLIENT_POOL_NAME)
    def generate_upload_url(
        file_path, mime_type=None, redirect_url=None, s3_client=None
    ):
        if(not mime_type):
            mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]

        fields = {
            "acl": "public-read",
            "Content-Type": mime_type,
            "success_action_status": "200"
        }

        # must also add fields to conditions
        conditions = [
            {"acl": "public-read"},
            {"Content-Type": mime_type},
            {"success_action_status": "200"}
        ]

        if(redirect_url):
            conditions.append({"redirect": redirect_url})

        post = s3_client.generate_presigned_post(
            Bucket=UPLOADS_S3_BUCKET,
            Key=file_path,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=30 * 60
        )

        return (
            post,  # {url, fields}
            f'https://{UPLOADS_S3_BUCKET}.s3.{UPLOADS_S3_BUCKET_REGION}.amazonaws.com/{file_path}'
        )
