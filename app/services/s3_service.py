import uuid
import mimetypes
import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException
from app.config import settings

ALLOWED_CONTENT_TYPES = {
    "image/jpeg", "image/jpg", "image/png", "image/webp",
    "image/gif", "image/bmp", "image/tiff",
}

COSH2_MEDIA_PREFIX = "cosh2/media"


def _get_client():
    return boto3.client(
        "s3",
        region_name=settings.s3_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


def upload_image_to_s3(file_bytes: bytes, original_filename: str, core_id: str) -> str:
    """
    Upload an image to S3 under cosh2/media/{core_id}/{uuid}.{ext}.
    Returns the public HTTPS URL.
    """
    content_type, _ = mimetypes.guess_type(original_filename)
    if not content_type:
        content_type = "image/jpeg"

    if content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"File type '{content_type}' is not allowed. Accepted: JPEG, PNG, WEBP, GIF."
        )

    ext = original_filename.rsplit(".", 1)[-1].lower() if "." in original_filename else "jpg"
    key = f"{COSH2_MEDIA_PREFIX}/{core_id}/{uuid.uuid4().hex}.{ext}"

    client = _get_client()
    try:
        client.put_object(
            Bucket=settings.s3_bucket_media,
            Key=key,
            Body=file_bytes,
            ContentType=content_type,
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e.response['Error']['Message']}")

    return f"https://{settings.s3_bucket_url}/{key}"


def delete_from_s3(s3_url: str):
    """Best-effort delete of an existing S3 object given its full URL. Never raises."""
    try:
        prefix = f"https://{settings.s3_bucket_url}/"
        if not s3_url.startswith(prefix):
            return
        key = s3_url[len(prefix):]
        _get_client().delete_object(Bucket=settings.s3_bucket_media, Key=key)
    except Exception:
        pass
