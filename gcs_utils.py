from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound
import tempfile
import os
import logging
from config import RAW_BUCKET, CONVERTED_UPLOADS_FOLDER

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = storage.Client()


class GCSError(Exception):
    """Custom exception for GCS operations"""
    pass


def download_from_gcs(gs_uri: str) -> str:
    """
    Download a file from Google Cloud Storage to a temporary file
    Args:
        gs_uri: GCS URI in format gs://bucket-name/path/to/file
    Returns:
        str: Path to downloaded temporary file

    Raises:
        GCSError: If download fails
        ValueError: If URI format is invalid
    """
    if not gs_uri.startswith("gs://"):
        error_msg = f"Invalid GCS URI format: {gs_uri}. Must start with gs://"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Parse GCS URI
        uri_without_prefix = gs_uri.replace("gs://", "")
        if "/" not in uri_without_prefix:
            error_msg = f"Invalid GCS URI format: {gs_uri}. Missing path after bucket name"
            logger.error(error_msg)
            raise ValueError(error_msg)

        bucket_name, blob_path = uri_without_prefix.split("/", 1)

        logger.info(f"Downloading from GCS: {gs_uri}")
        logger.info(f"Bucket: {bucket_name}, Path: {blob_path}")

        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Create temporary file
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(blob_path)[1])

        try:
            # Try to download - will raise exception if file doesn't exist
            blob.download_to_filename(tmp.name)
            logger.info(f"Successfully downloaded to: {tmp.name}")

            # Verify download succeeded
            if not os.path.exists(tmp.name):
                error_msg = "Download completed but temp file not found"
                logger.error(error_msg)
                raise GCSError(error_msg)

            downloaded_size = os.path.getsize(tmp.name)
            if downloaded_size == 0:
                error_msg = "Downloaded file is empty"
                logger.error(error_msg)
                raise GCSError(error_msg)

            logger.info(f"Downloaded file size: {downloaded_size / 1024 / 1024:.2f} MB")
            return tmp.name

        except NotFound as e:
            # Clean up temp file on error
            if os.path.exists(tmp.name):
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
            error_msg = f"File not found in GCS: {gs_uri}"
            logger.error(error_msg)
            raise GCSError(error_msg) from e

        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(tmp.name):
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
            raise

    except NotFound as e:
        error_msg = f"GCS file not found: {gs_uri}"
        logger.error(error_msg)
        raise GCSError(error_msg) from e

    except GoogleCloudError as e:
        error_msg = f"GCS download failed for {gs_uri}: {str(e)}"
        logger.error(error_msg)
        raise GCSError(error_msg) from e

    except Exception as e:
        error_msg = f"Unexpected error downloading from GCS: {str(e)}"
        logger.error(error_msg)
        raise GCSError(error_msg) from e




def upload_to_gcs(local_path: str, filename: str):
    # Validate local file exists
    if not os.path.exists(local_path):
        error_msg = f"Local file not found: {local_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    file_size = os.path.getsize(local_path)
    if file_size == 0:
        error_msg = f"Local file is empty: {local_path}"
        logger.error(error_msg)
        raise GCSError(error_msg)

    try:
        # Construct the blob path within converted_uploads folder
        blob_path = f"{CONVERTED_UPLOADS_FOLDER}/{filename}"
        gs_uri = f"gs://{RAW_BUCKET}/{blob_path}"

        logger.info(f"Uploading to GCS: {gs_uri}")
        logger.info(f"Local file: {local_path} ({file_size / 1024 / 1024:.2f} MB)")
        logger.info(f"Bucket: {RAW_BUCKET}, Path: {blob_path}")

        bucket = client.bucket(RAW_BUCKET)
        blob = bucket.blob(blob_path)

        # Upload with content type detection
        content_type = "video/mp4" if local_path.endswith(".mp4") else None
        blob.upload_from_filename(local_path, content_type=content_type)

        logger.info(f"Successfully uploaded to: {gs_uri}")
        return gs_uri

    except GoogleCloudError as e:
        error_msg = f"GCS upload failed for {gs_uri}: {str(e)}"
        logger.error(error_msg)
        raise GCSError(error_msg) from e

    except Exception as e:
        error_msg = f"Unexpected error uploading to GCS: {str(e)}"
        logger.error(error_msg)
        raise GCSError(error_msg) from e
