# Using for encoding on cloud runs

import json
import logging
import time
import base64
import os
from typing import Optional
from flask import Flask, request, jsonify

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Custom exception classes for retry control
class RetryableError(Exception):
    """
    Transient infrastructure failures that should trigger Pub/Sub retry.
    Examples: temporary GCS issues, network timeouts, container resource errors.
    """
    pass


class NonRetryableError(Exception):
    """
    Permanent business-logic failures that should NOT trigger retry.
    Examples: invalid input, corrupt videos, FFmpeg failures, missing fields.
    """
    pass


# Configuration for retry control
MAX_RETRY_COUNT = 3  # Maximum retries before permanent failure

# NOW import GCP libraries and initialize clients (these can be slow)
from google.cloud import firestore
from gcs_utils import (download_from_gcs,
                        upload_to_gcs,
                          GCSError,
                            publish_other_worker_message)
from ffmpeg_functions import encode, FFmpegError, VideoAlreadyOptimized
from config import PROJECT_ID

# Initialize GCP clients
db = firestore.Client(project=PROJECT_ID)

# Initialize Flask app
app = Flask(__name__)


def cleanup_temp_files(*file_paths):
    """Clean up temporary files"""
    for file_path in file_paths:
        if file_path and os.path.exists(file_path):
            try:
                os.unlink(file_path)
                logger.info(f"Cleaned up temp file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up {file_path}: {e}")


def update_job_status(job_id: str, status: str, error: Optional[str] = None, **kwargs):
    """Update job status in Firestore with error handling"""
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

def update_job_encoded(job_id: str, output_uri: str, duration_sec: int):
    """Update job in Firestore for output uri"""
    try:
        job_ref = db.collection("jobs").document(job_id)
        update_data = {
            "encoded_uri": output_uri,
            "encodedCompletedAt": firestore.SERVER_TIMESTAMP,
        }
        job_ref.update(update_data)
        logger.info(f"Job {job_id} marked as done.")
    except Exception as e:
        logger.error(f"Failed to mark job {job_id} as done: {e}")


def check_job_idempotency(job_id: str) -> tuple[bool, str, int]:
    """
    Check if job has already been handled or is in progress.
    Returns (should_skip, current_status, retry_count).

    Args:
        job_id: The job ID to check

    Returns:
        tuple: (should_skip: bool, status: str, retry_count: int)
        - should_skip: True if job should be ACKed without processing
        - status: Current job status from Firestore
        - retry_count: Current retry count from Firestore
    """
    # Statuses that indicate job should not be processed again
    TERMINAL_STATUSES = {"done", "error", "cancelled", "failed"}
    IN_PROGRESS_STATUSES = {"queued"}

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
        # On Firestore read failure, allow processing but log the issue
        return False, "unknown", 0


def increment_retry_count(job_id: str) -> int:
    """
    Increment and return the retry count for a job.
    Returns the new retry count.
    """
    try:
        job_ref = db.collection("jobs").document(job_id)
        job_ref.update({"retryCount": firestore.Increment(1)})

        # Fetch the updated count
        job_doc = job_ref.get()
        if job_doc.exists:
            return job_doc.to_dict().get("retryCount", 1)
        return 1
    except Exception as e:
        logger.error(f"Failed to increment retry count for job {job_id}: {e}")
        return 0


def mark_job_permanently_failed(job_id: str, error: str):
    """Mark a job as permanently failed after exceeding retry limit."""
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

def process_encoding_job(data: dict):
    """
    Process a video encoding job.

    Expected data format:
    {
        "jobId": "job-id-here",
        "input_uri": "gs://bucket/path/to/input.mov",
        "output_filename": "output.mp4"  // Optional, defaults to {jobId}_encoded.mp4
    }
    Output will be uploaded to: gs://hooptuber-raw-1757394912/converted_uploads/{output_filename}

    Raises:
        RetryableError: For transient infrastructure failures (GCS timeouts, network issues)
        NonRetryableError: For permanent failures (invalid input, FFmpeg errors, corrupt videos)
    """
    job_id = None
    input_file = None
    output_file = None
    start_time = time.time()

    try:
        # Validate required fields - missing fields are permanent failures
        required_fields = ["jobId", "input_uri"]
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            logger.error(error_msg)
            raise NonRetryableError(error_msg)

        job_id = data["jobId"]
        input_uri = data["input_uri"]
        output_filename = data.get("output_filename", f"{job_id}_encoded.mp4")

        # Validate input_uri format
        if not input_uri.startswith("gs://"):
            error_msg = f"Invalid input_uri format: {input_uri}"
            logger.error(error_msg)
            raise NonRetryableError(error_msg)

        logger.info(f"Starting encoding job: {job_id}")
        logger.info(f"Input: {input_uri}")
        logger.info(f"Output filename: {output_filename}")

        # Update job status to ENCODING
        update_job_status(job_id, "encoding", encodingStartedAt=firestore.SERVER_TIMESTAMP)

        # Step 1: Download from GCS
        logger.info(f"[{job_id}] Step 1/3: Downloading from GCS...")
        try:
            input_file = download_from_gcs(input_uri)
            logger.info(f"[{job_id}] Downloaded to: {input_file}")
        except ValueError as e:
            # Invalid URI format - permanent failure
            raise NonRetryableError(f"Invalid GCS URI: {str(e)}")
        except GCSError as e:
            error_str = str(e).lower()
            # File not found is permanent; other GCS errors may be transient
            if "not found" in error_str or "404" in error_str:
                raise NonRetryableError(f"Source file not found: {str(e)}")
            # Transient GCS errors (network, timeout, etc.)
            raise RetryableError(f"GCS download failed (transient): {str(e)}")

        # Step 2: Encode video (or skip if already optimized)
        logger.info(f"[{job_id}] Step 2/3: Encoding video...")
        output_file = f"/tmp/{job_id}_encoded.mp4"
        try:
            encode(input_file, output_file)
            logger.info(f"[{job_id}] Encoded to: {output_file}")
        except VideoAlreadyOptimized as e:
            # Video is already optimized - use the original input file
            logger.info(f"[{job_id}] {str(e)}")
            logger.info(f"[{job_id}] Using original input file instead of encoding")
            output_file = input_file  # Use input file as output
        except FileNotFoundError as e:
            # Input file disappeared - permanent failure
            raise NonRetryableError(f"Input file not found during encoding: {str(e)}")
        except FFmpegError as e:
            # FFmpeg failures are permanent - corrupt/unsupported video
            raise NonRetryableError(f"FFmpeg encoding failed: {str(e)}")

        # Step 3: Upload to GCS (to converted_uploads folder)
        logger.info(f"[{job_id}] Step 3/3: Uploading to GCS converted_uploads folder...")
        try:
            output_uri = upload_to_gcs(output_file, output_filename)
            logger.info(f"[{job_id}] Uploaded to: {output_uri}")
        except FileNotFoundError as e:
            # Output file disappeared - permanent failure
            raise NonRetryableError(f"Output file not found for upload: {str(e)}")
        except GCSError as e:
            # GCS upload errors may be transient
            raise RetryableError(f"GCS upload failed (transient): {str(e)}")

        # Success - update job status and queue for further processing
        duration = time.time() - start_time
        update_job_status(
            job_id,
            "queued",
            encodedAt=firestore.SERVER_TIMESTAMP,
            encodingDurationSec=int(duration)
        )
        update_job_encoded(job_id, output_uri, int(duration))
        logger.info(f"[{job_id}] Encoding completed successfully in {duration:.2f}s")
        publish_other_worker_message(job_id, output_uri)

    except (RetryableError, NonRetryableError):
        # Re-raise classified errors for handler to process
        raise

    except Exception as e:
        # Unclassified exceptions - treat as non-retryable to be safe
        error_msg = str(e)
        logger.error(f"[{job_id}] Unexpected error: {error_msg}", exc_info=True)
        raise NonRetryableError(f"Unexpected error: {error_msg}")

    finally:
        # Always clean up temp files safely
        try:
            if output_file and output_file == input_file:
                cleanup_temp_files(input_file)
            else:
                cleanup_temp_files(input_file, output_file)
        except Exception as cleanup_error:
            logger.warning(f"[{job_id}] Cleanup failed: {cleanup_error}")


@app.route('/encode', methods=['POST'])
def handle_pubsub_push():
    """
    Handle Pub/Sub push subscription messages with strict retry control.

    Pub/Sub push format:
    {
        "message": {
            "data": "base64-encoded-json",
            "messageId": "...",
            "publishTime": "..."
        },
        "subscription": "..."
    }

    ACK/NACK BEHAVIOR:
    - HTTP 200-299: Message is ACKed (success or permanent failure)
    - HTTP 400-499: Message is ACKed (invalid message, no retry)
    - HTTP 500: Message is NACKed (transient failure, will retry with backoff)

    This endpoint only returns 500 for explicitly retryable errors.
    All other errors return 200/400 to ACK and prevent retry storms.
    """
    job_id = None

    try:
        # Verify request has Pub/Sub envelope
        logger.info("/encode endpoint hit")
        envelope = request.get_json()
        if not envelope:
            logger.error("No JSON body in request")
            return 'Bad Request: no JSON body', 400

        if 'message' not in envelope:
            logger.error("No message field in request")
            return 'Bad Request: no message field', 400

        # Extract and decode message data
        message = envelope['message']
        message_id = message.get('messageId', 'unknown')

        if 'data' not in message:
            logger.error("No data field in message")
            return 'Bad Request: no data field', 400

        # Decode base64 data
        try:
            message_data = base64.b64decode(message['data']).decode('utf-8')
            data = json.loads(message_data)
            logger.info(f"Received encoding job: {data} (messageId: {message_id})")
        except (json.JSONDecodeError, base64.binascii.Error) as e:
            logger.error(f"Invalid message data: {e}")
            return f'Bad Request: invalid message data - {str(e)}', 400

        job_id = data.get("jobId")

        # Early validation: job_id is required
        if not job_id:
            logger.error("Missing jobId in message data")
            return jsonify({'status': 'error', 'message': 'Missing jobId'}), 400

        # IDEMPOTENCY GUARD: Check if job has already been handled
        should_skip, current_status, retry_count = check_job_idempotency(job_id)

        if should_skip:
            logger.info(f"Job {job_id} already handled (status: {current_status}). ACKing message.")
            return jsonify({
                'status': 'already_handled',
                'jobId': job_id,
                'currentStatus': current_status
            }), 200

        # Check retry limit before processing
        if retry_count >= MAX_RETRY_COUNT:
            error_msg = f"Job {job_id} exceeded max retries ({MAX_RETRY_COUNT})"
            logger.error(error_msg)
            mark_job_permanently_failed(job_id, "Exceeded max retry attempts")
            return jsonify({
                'status': 'permanently_failed',
                'jobId': job_id,
                'message': error_msg
            }), 200  # ACK to stop retries

        # Legacy status checks from message payload (for backwards compatibility)
        if data.get("status") in ["done", "error", "cancelled", "failed"]:
            logger.info(f"Job {job_id} has terminal status in payload: {data.get('status')}")
            return jsonify({
                'status': 'no_action',
                'jobId': job_id,
                'reason': f"payload status is {data.get('status')}"
            }), 200

        # Process the encoding job
        process_encoding_job(data)

        # Success - return 200 to ACK message
        return jsonify({'status': 'queued', 'jobId': job_id}), 200

    except NonRetryableError as e:
        # Permanent failure - ACK message to prevent retries
        error_msg = str(e)
        logger.error(f"[{job_id}] Non-retryable error: {error_msg}")

        if job_id:
            update_job_status(job_id, "error", error=error_msg)

        # Return 200 to ACK - this job will never succeed
        return jsonify({
            'status': 'permanent_error',
            'jobId': job_id,
            'message': error_msg,
            'retryable': False
        }), 200

    except RetryableError as e:
        # Transient failure - check retry count before NACKing
        error_msg = str(e)
        logger.warning(f"[{job_id}] Retryable error: {error_msg}")

        if job_id:
            new_retry_count = increment_retry_count(job_id)
            logger.info(f"[{job_id}] Retry count: {new_retry_count}/{MAX_RETRY_COUNT}")

            if new_retry_count >= MAX_RETRY_COUNT:
                # Exceeded retries - mark as permanent failure and ACK
                logger.error(f"[{job_id}] Max retries exceeded, marking as permanently failed")
                mark_job_permanently_failed(job_id, error_msg)
                return jsonify({
                    'status': 'failed',
                    'jobId': job_id,
                    'message': f'Max retries exceeded: {error_msg}',
                    'retryCount': new_retry_count
                }), 200

            # Update status but keep retrying
            update_job_status(job_id, "encoding", error=f"Retry {new_retry_count}: {error_msg}")

        # Return 500 to NACK - Pub/Sub will retry with backoff
        return jsonify({
            'status': 'transient_error',
            'jobId': job_id,
            'message': error_msg,
            'retryable': True
        }), 500

    except Exception as e:
        # Unexpected error - treat as non-retryable to be safe
        error_msg = str(e)
        logger.error(f"[{job_id}] Unexpected error: {error_msg}", exc_info=True)

        if job_id:
            update_job_status(job_id, "error", error=f"Unexpected: {error_msg}")

        # ACK to prevent retry storms from unknown errors
        return jsonify({
            'status': 'unexpected_error',
            'jobId': job_id,
            'message': error_msg,
            'retryable': False
        }), 200

@app.route('/', methods=['GET'])
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Cloud Run"""
    return 'OK', 200


if __name__ == "__main__":
    """
    Run Flask app for Pub/Sub push subscription

    Cloud Run will route HTTP requests to this Flask app.
    Pub/Sub push subscription will POST to /encode endpoint.
    """
    port = int(os.environ.get("PORT", 8080))

    logger.info("=" * 60)
    logger.info("FFmpeg Encoding Worker Starting (PUSH mode)")
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Port: {port}")
    logger.info("Push endpoint: /encode")
    logger.info("Health check: / or /health")
    logger.info("=" * 60)

    # Run Flask app
    # Cloud Run manages the lifecycle, no need for signal handlers
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,  # Always False in production
        threaded=True  # Handle multiple requests concurrently
    )
