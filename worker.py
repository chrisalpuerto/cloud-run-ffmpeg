# Using for encoding on cloud runs

import json
import logging
import time
import signal
import sys
import threading
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import os

# Configure logging FIRST
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# START HEALTH SERVER IMMEDIATELY - Cloud Run needs this before anything else
def start_health_server():
    """Start HTTP health check server for Cloud Run"""
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Starting health check server on port {port}")

    class HealthCheckHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, _format, *_args):
            # Suppress default HTTP server logs to reduce noise
            pass

    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        logger.info(f"Health check server listening on 0.0.0.0:{port}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"Failed to start health server: {e}")
        raise

# Start health server in daemon thread immediately at module import
health_thread = threading.Thread(target=start_health_server, daemon=True)
health_thread.start()
logger.info("Health check server thread started at module import")

# NOW import GCP libraries and initialize clients (these can be slow)
from google.cloud import pubsub_v1, firestore
from gcs_utils import download_from_gcs, upload_to_gcs, GCSError
from ffmpeg_functions import encode, FFmpegError
from config import PROJECT_ID

# Initialize GCP clients
db = firestore.Client(project=PROJECT_ID)
subscriber = pubsub_v1.SubscriberClient()

# Get subscription path from environment
subscription_name = os.environ.get("ENCODE_PUBSUB")
if not subscription_name:
    logger.error("ENCODE_PUBSUB environment variable not set")
    sys.exit(1)

subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_name)
logger.info(f"Subscription path: {subscription_path}")

# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(sig, _frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    logger.info(f"Received signal {sig}, initiating graceful shutdown...")
    shutdown_requested = True


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


def handle_message(message: pubsub_v1.subscriber.message.Message):
    """
    Process a Pub/Sub message for video encoding

    Expected message format:
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
        # Parse message data
        try:
            data = json.loads(message.data.decode())
            logger.info(f"Received encoding job: {data}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            message.ack()  # Ack invalid messages to prevent retry
            return

        # Validate required fields
        required_fields = ["jobId", "input_uri"]
        missing_fields = [f for f in required_fields if f not in data]
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            logger.error(error_msg)
            message.ack()  # Ack invalid messages
            return

        job_id = data["jobId"]
        input_uri = data["input_uri"]
        output_filename = data.get("output_filename", f"{job_id}_encoded.mp4")

        logger.info(f"Starting encoding job: {job_id}")
        logger.info(f"Input: {input_uri}")
        logger.info(f"Output filename: {output_filename}")

        # Update job status to ENCODING
        update_job_status(job_id, "ENCODING", encodingStartedAt=firestore.SERVER_TIMESTAMP)

        # Step 1: Download from GCS
        logger.info(f"[{job_id}] Step 1/3: Downloading from GCS...")
        try:
            input_file = download_from_gcs(input_uri)
            logger.info(f"[{job_id}] Downloaded to: {input_file}")
        except (GCSError, ValueError) as e:
            raise Exception(f"Download failed: {str(e)}")

        # Step 2: Encode video
        logger.info(f"[{job_id}] Step 2/3: Encoding video...")
        output_file = f"/tmp/{job_id}_encoded.mp4"
        try:
            encode(input_file, output_file)
            logger.info(f"[{job_id}] Encoded to: {output_file}")
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
        message.ack()

    except Exception as e:
        error_msg = str(e)
        logger.error(f"[{job_id}] Encoding job failed: {error_msg}", exc_info=True)

        # Update job status to FAILED
        if job_id:
            update_job_status(job_id, "FAILED", error=error_msg)

        # Nack message to retry (Pub/Sub will handle retry logic)
        message.nack()

    finally:
        # Always clean up temp files
        cleanup_temp_files(input_file, output_file)


def main():
    """Main worker loop"""
    logger.info("=" * 60)
    logger.info("FFmpeg Encoding Worker Starting")
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Subscription: {subscription_path}")
    logger.info("=" * 60)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Subscribe to Pub/Sub
        streaming_pull_future = subscriber.subscribe(
            subscription_path,
            callback=handle_message
        )
        logger.info("Encoder worker listening for messages...")

        # Keep worker alive until shutdown signal
        while not shutdown_requested:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received, shutting down...")
                break

        # Graceful shutdown
        logger.info("Shutting down worker...")
        streaming_pull_future.cancel()
        streaming_pull_future.result(timeout=30)  # Wait up to 30s for ongoing work

    except Exception as e:
        logger.error(f"Fatal error in worker: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Encoder worker stopped")


if __name__ == "__main__":
    main()
