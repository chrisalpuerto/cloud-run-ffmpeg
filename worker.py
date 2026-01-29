import json
import logging
import time
import os
from typing import Optional
from google.cloud import pubsub_v1
from google.cloud import firestore
from gcs_utils import (download_from_gcs, upload_to_gcs, GCSError, publish_other_worker_message)
from ffmpeg_functions import encode, FFmpegError, VideoAlreadyOptimized
from config import PROJECT_ID

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RetryableError(Exception):
    pass


class NonRetryableError(Exception):
    pass


MAX_RETRY_COUNT = 3
SUBSCRIPTION_ID = "video-encode-pull-sub"

db = firestore.Client(project=PROJECT_ID)


def cleanup_temp_files(*file_paths):
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.unlink(file_path)
                logger.info(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up {file_path}: {e}")


def update_job_status(job_id: str, status: str, error: Optional[str] = None, **kwargs):
    try:
        job_ref = db.collection("jobs").document(job_id)
        update_data = {"status": status}
        if error:
            update_data["error"] = error
            update_data["failedAt"] = firestore.SERVER_TIMESTAMP
        update_data.update(kwargs)
        job_ref.update(update_data)
        logger.info(f"Job {job_id} status updated to: {status}")
    except Exception as e:
        logger.error(f"Failed to update job {job_id} status: {e}")


def update_video_gcs_uri(job_id: str, gcs_uri: str):
    try:
        job_ref = db.collection("jobs").document(job_id)
        job_ref.update({"videoGcsUri": gcs_uri})
        logger.info(f"Job {job_id} video GCS URI updated.")
    except Exception as e:
        logger.error(f"Failed to update job {job_id} video GCS URI: {e}")


def update_job_encoded(job_id: str, output_uri: str, duration_sec: int):
    try:
        job_ref = db.collection("jobs").document(job_id)
        job_ref.update({
            "encoded_uri": output_uri,
            "encodedCompletedAt": firestore.SERVER_TIMESTAMP,
        })
        logger.info(f"Job {job_id} marked as done.")
    except Exception as e:
        logger.error(f"Failed to mark job {job_id} as done: {e}")


def check_job_idempotency(job_id: str) -> tuple[bool, str, int]:
    TERMINAL_STATUSES = {"done", "error", "cancelled", "failed"}
    IN_PROGRESS_STATUSES = {"queued", "encoding"}
    try:
        job_ref = db.collection("jobs").document(job_id)
        job_doc = job_ref.get()
        if not job_doc.exists:
            logger.warning(f"Job {job_id} not found in Firestore")
            return False, "not_found", 0
        job_data = job_doc.to_dict()
        status = job_data.get("status", "unknown")
        retry_count = job_data.get("retryCount", 0)
        if status in TERMINAL_STATUSES:
            logger.info(f"Job {job_id} already in terminal status: {status}")
            return True, status, retry_count
        if status in IN_PROGRESS_STATUSES:
            logger.info(f"Job {job_id} already in progress: {status}")
            return True, status, retry_count
        return False, status, retry_count
    except Exception as e:
        logger.error(f"Failed to check job {job_id} status: {e}")
        return False, "unknown", 0


def increment_retry_count(job_id: str) -> int:
    try:
        job_ref = db.collection("jobs").document(job_id)
        job_ref.update({"retryCount": firestore.Increment(1)})
        job_doc = job_ref.get()
        if job_doc.exists:
            return job_doc.to_dict().get("retryCount", 1)
        return 1
    except Exception as e:
        logger.error(f"Failed to increment retry count for job {job_id}: {e}")
        return 0


def mark_job_permanently_failed(job_id: str, error: str):
    try:
        job_ref = db.collection("jobs").document(job_id)
        job_ref.update({
            "status": "failed",
            "error": f"Exceeded max retries ({MAX_RETRY_COUNT}): {error}",
            "failedAt": firestore.SERVER_TIMESTAMP,
            "permanentFailure": True
        })
        logger.info(f"Job {job_id} marked as permanently failed")
    except Exception as e:
        logger.error(f"Failed to mark job {job_id} as permanently failed: {e}")


def job_ref_dict(job_id: str):
    return db.collection("jobs").document(job_id).get().to_dict()


def process_encoding_job(data: dict):
    job_id = None
    input_file = None
    output_file = None
    start_time = time.time()

    try:
        required_fields = ["jobId", "input_uri"]
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            raise NonRetryableError(f"Missing required fields: {missing_fields}")

        job_id = data["jobId"]
        input_uri = data["input_uri"]
        curr_username = data.get("curr_username", "unknown_user")
        output_filename = data.get("output_filename", f"{job_id}_encoded.mp4")

        if not input_uri.startswith("gs://"):
            raise NonRetryableError(f"Invalid input_uri format: {input_uri}")

        logger.info(f"Starting encoding job: {job_id}")
        logger.info(f"Input: {input_uri}")
        logger.info(f"Output filename: {output_filename}")

        update_job_status(job_id, "encoding", encodingStartedAt=firestore.SERVER_TIMESTAMP)

        logger.info(f"{curr_username}: starting encoding job {job_id}")
        logger.info(f"[{job_id}] Step 1/3: Downloading from GCS...")
        try:
            input_file = download_from_gcs(input_uri)
            logger.info(f"[{job_id}] Downloaded to: {input_file}")
        except ValueError as e:
            raise NonRetryableError(f"Invalid GCS URI: {str(e)}")
        except GCSError as e:
            error_str = str(e).lower()
            if "not found" in error_str or "404" in error_str:
                raise NonRetryableError(f"Source file not found: {str(e)}")
            raise RetryableError(f"GCS download failed (transient): {str(e)}")

        logger.info(f"{curr_username}: starting step 2/3: ")
        logger.info(f"[{job_id}] Step 2/3: Encoding video...")
        output_file = f"/tmp/{job_id}_encoded.mp4"
        try:
            encode(input_file, output_file)
            logger.info(f"[{job_id}] Encoded to: {output_file}")
        except VideoAlreadyOptimized as e:
            logger.info(f"[{job_id}] {str(e)}")
            logger.info(f"[{job_id}] Using original input file instead of encoding")
            output_file = input_file
        except FileNotFoundError as e:
            raise NonRetryableError(f"Input file not found during encoding: {str(e)}")
        except FFmpegError as e:
            raise NonRetryableError(f"FFmpeg encoding failed: {str(e)}")

        logger.info(f"[{job_id}] Step 3/3: Uploading to GCS converted_uploads folder...")
        try:
            output_uri = upload_to_gcs(output_file, output_filename)
            logger.info(f"[{job_id}] Uploaded to: {output_uri}")
        except FileNotFoundError as e:
            raise NonRetryableError(f"Output file not found for upload: {str(e)}")
        except GCSError as e:
            raise RetryableError(f"GCS upload failed (transient): {str(e)}")

        duration = time.time() - start_time
        update_job_status(
            job_id,
            "processing",
            encodedAt=firestore.SERVER_TIMESTAMP,
            encodingDurationSec=int(duration)
        )
        update_job_encoded(job_id, output_uri, int(duration))
        logger.info(f"[{job_id}] Encoding completed successfully in {duration:.2f}s")
        logger.info(f"{curr_username}: encoding successful for this user.")
        try:
            update_video_gcs_uri(job_id, output_uri)
        except Exception as e:
            logger.error(f"[{job_id}] Failed to update video GCS URI: {e}", exc_info=True)
        try:
            publish_other_worker_message(job_id, output_uri)
        except Exception as e:
            logger.error(f"[{job_id}] Failed to publish to next worker: {e}", exc_info=True)
            logger.error(f"{curr_username}: failed to publish to next worker.")
            raise RetryableError(f"Failed to publish to next worker: {str(e)}")

    except (RetryableError, NonRetryableError):
        raise
    except Exception as e:
        error_msg = str(e)
        logger.error(f"[{job_id}] Unexpected error: {error_msg}", exc_info=True)
        raise NonRetryableError(f"Unexpected error: {error_msg}")
    finally:
        try:
            if output_file and output_file == input_file:
                cleanup_temp_files(input_file)
            else:
                cleanup_temp_files(input_file, output_file)
        except Exception as cleanup_error:
            logger.warning(f"[{job_id}] Cleanup failed: {cleanup_error}")


def process_message(message):
    job_id = None
    try:
        message_data = message.data.decode('utf-8')
        data = json.loads(message_data)
        logger.info(f"Received encoding job: {data} (messageId: {message.message_id})")

        job_id = data.get("jobId")
        if not job_id:
            logger.error("Missing jobId in message data")
            message.ack()
            return

        job = job_ref_dict(job_id)
        if job and job.get("encoded_uri"):
            logger.warning(f"[{job_id}] Already encoded; skipping")
            message.ack()
            return

        should_skip, current_status, retry_count = check_job_idempotency(job_id)
        if should_skip:
            logger.info(f"Job {job_id} already handled (status: {current_status}). ACKing message.")
            message.ack()
            return

        if retry_count >= MAX_RETRY_COUNT:
            logger.error(f"Job {job_id} exceeded max retries ({MAX_RETRY_COUNT})")
            mark_job_permanently_failed(job_id, "Exceeded max retry attempts")
            message.ack()
            return

        if data.get("status") in ["done", "error", "cancelled", "failed"]:
            logger.info(f"Job {job_id} has terminal status in payload: {data.get('status')}")
            message.ack()
            return

        process_encoding_job(data)
        message.ack()

    except NonRetryableError as e:
        error_msg = str(e)
        logger.error(f"[{job_id}] Non-retryable error: {error_msg}")
        if job_id:
            update_job_status(job_id, "error", error=error_msg)
        message.ack()

    except RetryableError as e:
        error_msg = str(e)
        logger.warning(f"[{job_id}] Retryable error: {error_msg}")
        if job_id:
            new_retry_count = increment_retry_count(job_id)
            logger.info(f"[{job_id}] Retry count: {new_retry_count}/{MAX_RETRY_COUNT}")
            if new_retry_count >= MAX_RETRY_COUNT:
                logger.error(f"[{job_id}] Max retries exceeded, marking as permanently failed")
                mark_job_permanently_failed(job_id, error_msg)
                message.ack()
                return
            update_job_status(job_id, "encoding", error=f"Retry {new_retry_count}: {error_msg}")
        logging.info(f"Received retryable error: {str(e)}")
        message.nack()

    except Exception as e:
        error_msg = str(e)
        logger.error(f"[{job_id}] Unexpected error: {error_msg}", exc_info=True)
        if job_id:
            update_job_status(job_id, "error", error=f"Unexpected: {error_msg}")
        message.ack()


def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    logger.info(f"Cloud Run Job started, pulling messages from {subscription_path}")

    response = subscriber.pull(
        request={
            "subscription": subscription_path,
            "max_messages": 1,  # start with 1
        },
        timeout=60,
    )

    if not response.received_messages:
        logger.info("No messages available, exiting job.")
        return

    for received_message in response.received_messages:
        process_message(received_message)

    logger.info("Job finished processing messages. Exiting.")


if __name__ == "__main__":
    main()
