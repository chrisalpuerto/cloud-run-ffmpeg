# Using for encoding on cloud runs

import json
import logging
import time
import base64
import os
from typing import Optional
from flask import Flask, request, jsonify
import threading

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# NOW import GCP libraries and initialize clients (these can be slow)
from google.cloud import firestore
from gcs_utils import download_from_gcs, upload_to_gcs, GCSError
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

def process_encoding_job(data: dict):
    """
    Process a video encoding job

    Expected data format:
    {
        "jobId": "job-id-here",
        "input_uri": "gs://bucket/path/to/input.mov",
        "output_filename": "output.mp4"  // Optional, defaults to {jobId}_encoded.mp4
    }

    Output will be uploaded to: gs://hooptuber-raw-1757394912/converted_uploads/{output_filename}
    """
    job_id = None
    input_file = None
    output_file = None
    start_time = time.time()

    try:
        # Validate required fields
        required_fields = ["jobId", "input_uri"]
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        job_id = data["jobId"]
        input_uri = data["input_uri"]
        output_filename = data.get("output_filename", f"{job_id}_encoded.mp4")

        logger.info(f"Starting encoding job: {job_id}")
        logger.info(f"Input: {input_uri}")
        logger.info(f"Output filename: {output_filename}")
        print(f"hi")
        # Update job status to ENCODING
        update_job_status(job_id, "ENCODING", encodingStartedAt=firestore.SERVER_TIMESTAMP)

        # Step 1: Download from GCS
        logger.info(f"[{job_id}] Step 1/3: Downloading from GCS...")
        try:
            input_file = download_from_gcs(input_uri)
            logger.info(f"[{job_id}] Downloaded to: {input_file}")
        except (GCSError, ValueError) as e:
            raise Exception(f"Download failed: {str(e)}")

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
        except (FFmpegError, FileNotFoundError) as e:
            raise Exception(f"Encoding failed: {str(e)}")
        # Step 3: Upload to GCS (to converted_uploads folder)
        logger.info(f"[{job_id}] Step 3/3: Uploading to GCS converted_uploads folder...")
        try:
            output_uri = upload_to_gcs(output_file, output_filename)
            logger.info(f"[{job_id}] Uploaded to: {output_uri}")
        except (GCSError, FileNotFoundError) as e:
            raise Exception(f"Upload failed: {str(e)}")

        # Success - update job status
        duration = time.time() - start_time
        update_job_status(
            job_id,
            "ENCODED",
            output_uri=output_uri,
            encodedAt=firestore.SERVER_TIMESTAMP,
            encodingDurationSec=int(duration)
        )

        logger.info(f"[{job_id}] Encoding completed successfully in {duration:.2f}s")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"[{job_id}] Encoding job failed: {error_msg}", exc_info=True)

        # Update job status to FAILED in Firestore (application-level failure tracking)
        if job_id:
            update_job_status(job_id, "FAILED", error=error_msg)

        # Re-raise to return 500 to Pub/Sub for retry on transient errors
        raise

    finally:
        # Always clean up temp files
        # If output_file == input_file (video was already optimized), only clean up once
        if output_file and output_file == input_file:
            cleanup_temp_files(input_file)
        else:
            cleanup_temp_files(input_file, output_file)


@app.route('/encode', methods=['POST'])
def handle_pubsub_push():
    """
    Handle Pub/Sub push subscription messages

    Pub/Sub push format:
    {
        "message": {
            "data": "base64-encoded-json",
            "messageId": "...",
            "publishTime": "..."
        },
        "subscription": "..."
    }

    PUSH SUBSCRIPTION ACK BEHAVIOR:
    - HTTP 200-299: Message is ACKed (success)
    - HTTP 400-499: Message is ACKed (permanent failure, no retry)
    - HTTP 500-599: Message is NACKed (transient failure, will retry with backoff)
    """
    try:
        # Verify request has Pub/Sub envelope
        envelope = request.get_json()
        if not envelope:
            logger.error("No JSON body in request")
            return 'Bad Request: no JSON body', 400

        if 'message' not in envelope:
            logger.error("No message field in request")
            return 'Bad Request: no message field', 400

        # Extract and decode message data
        message = envelope['message']

        if 'data' not in message:
            logger.error("No data field in message")
            return 'Bad Request: no data field', 400

        # Decode base64 data
        try:
            message_data = base64.b64decode(message['data']).decode('utf-8')
            data = json.loads(message_data)
            logger.info(f"Received encoding job: {data}")
        except (json.JSONDecodeError, base64.binascii.Error) as e:
            logger.error(f"Invalid message data: {e}")
            # Return 400 to ACK invalid messages (no retry)
            return f'Bad Request: invalid message data - {str(e)}', 400

        # Process the encoding job
        # This will raise exceptions on failure, which Flask will catch
        threading.Thread(
            target=process_encoding_job,
            args=(data,),
            daemon=True
        ).start()

        # Success - return 200 to ACK message
        return jsonify({'status': 'success', 'jobId': data.get('jobId')}), 200

    except ValueError as e:
        # Validation errors (missing fields) - return 400 to ACK (no retry)
        logger.error(f"Validation error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 400

    except Exception as e:
        # Runtime errors (FFmpeg, GCS, etc.) - return 500 to NACK (retry)
        logger.error(f"Processing error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'message': str(e)}), 500


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
