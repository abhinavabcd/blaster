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
            content_type=mime_type,
            headers={
                "x-goog-acl": "public-read",
                "Content-Type": mime_type
            }
        )
        # bucket.cors = [{
        #   'origin': ['http://localhost:3000', 'https://localhost'...],
        #   'method': ['PUT', 'GET', 'HEAD'],
        #   'responseHeader': ['Content-Type', 'x-goog-resumable', 'x-goog-acl'],
        #   'maxAgeSeconds': 3600
        # }]

        return {
            "url": url,
            "method": "PUT",
            # after shit ton of trial and error, this started working
            # if you wish to change, be careful to test it
            "headers": {
                "x-goog-acl": "public-read",
                "Content-Type": mime_type
            },
        }, f"https://storage.googleapis.com/{UPLOADS_GCLOUD_BUCKET}/{file_path}"

    @use_connection_pool(gcloud_storage="google_cloudstorage")
    def upload_file_obj(file_path, file_obj, mime_type=None, gcloud_storage=None):
        if(not mime_type):
            mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]

        bucket = gcloud_storage.bucket(UPLOADS_GCLOUD_BUCKET)
        blob = bucket.blob(file_path)
        blob.upload_from_file(file_obj, content_type=mime_type)
        blob.make_public()
        return blob.public_url

    @use_connection_pool(gcloud_storage="google_cloudstorage")
    def upload_file_obj_private(
        file_path, file_obj, mime_type=None,
        expires_in=8 * 3600, gcloud_storage=None  # seconds
    ):
        if(not mime_type):
            mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]
        bucket = gcloud_storage.bucket(UPLOADS_GCLOUD_BUCKET)
        blob = bucket.blob(file_path)
        blob.upload_from_file(file_obj, content_type=mime_type)
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(seconds=expires_in),
            method="GET"
        )

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
            ExpiresIn=30 * 60  # 30 minutes
        )

        return (
            post,  # {url, fields}
            f'https://{UPLOADS_S3_BUCKET}.s3.{UPLOADS_S3_BUCKET_REGION}.amazonaws.com/{file_path}'
        )

    @use_connection_pool(s3_client=UPLOADS_S3_CLIENT_POOL_NAME)
    def upload_file_obj(file_path, file_obj, mime_type=None, s3_client=None):
        if(not mime_type):
            mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]
        s3_client.upload_fileobj(
            file_obj,
            UPLOADS_S3_BUCKET,
            file_path,
            ExtraArgs={'ACL': 'public-read', "ContentType": mime_type}
        )
        return f'https://{UPLOADS_S3_BUCKET}.s3.{UPLOADS_S3_BUCKET_REGION}.amazonaws.com/{file_path}'

    @use_connection_pool(s3_client=UPLOADS_S3_CLIENT_POOL_NAME)
    def upload_file_obj_private(
        file_path, file_obj, mime_type=None,
        expires_in=8 * 3600, s3_client=None
    ):
        if(not mime_type):
            mime_type = FILE_EXTENSION_TO_MIME_TYPE[os.path.splitext(file_path)[1]]
        s3_client.upload_fileobj(
            file_obj,
            UPLOADS_S3_BUCKET,
            file_path,
            ExtraArgs={"ContentType": mime_type}
        )

        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': UPLOADS_S3_BUCKET, 'Key': file_path},
            ExpiresIn=expires_in  # Expires in 1 hour
        )
