import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "hooptuber-dev-1234")
ENCODE_TOPIC = os.getenv("ENCODE_PUBSUB", "video-encode-topic-sub")
RAW_BUCKET = os.getenv("GCS_RAW_BUCKET", "hooptuber-raw-1757394912")

CONVERTED_UPLOADS_FOLDER = "converted_uploads"

FFMPEG_PRESET = os.getenv("FFMPEG_PRESET", "fast")
CRF = os.getenv("FFMPEG_CRF", "23")

# testing on 1080, will change back to 720p 
TARGET_HEIGHT = int(os.getenv("TARGET_HEIGHT", "1080"))

PROJECT_ID = "hooptuber-dev-1234"
if not PROJECT_ID:
    raise RuntimeError("Project id is not set")