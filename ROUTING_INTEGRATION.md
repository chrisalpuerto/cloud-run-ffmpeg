# FFmpeg Encoding Worker - Routing Integration Guide

This document outlines where and how to integrate the FFmpeg encoding worker with your existing HoopTuber backend infrastructure.

## Overview

The encoding worker is a **separate Cloud Run service** that:
1. Converts HEVC/MOV videos to H.264 MP4 format
2. Downscales videos to 720p resolution
3. Listens to Pub/Sub messages for encoding jobs
4. Updates Firestore with job status

---

## Architecture Flow

```
User Upload → FastAPI Backend → GCS (RAW) → Pub/Sub → Encoding Worker → GCS (PROCESSED) → Analysis Worker
```

---

## Required Changes in Your Backend

### 1. **Add New Environment Variables**

Add these to your FastAPI backend `.env` file:

```env
# Encoding Service
GCS_PROCESSED_BUCKET=your-processed-videos-bucket
ENCODING_TOPIC=encoding-jobs  # New Pub/Sub topic for encoding
```

### 2. **Create Pub/Sub Topic and Subscription**

#### Create Encoding Topic
```bash
gcloud pubsub topics create encoding-jobs --project=YOUR_PROJECT_ID
```

#### Create Subscription for Worker
```bash
gcloud pubsub subscriptions create encoding-worker-sub \
  --topic=encoding-jobs \
  --ack-deadline=600 \
  --message-retention-duration=1h \
  --project=YOUR_PROJECT_ID
```

### 3. **Modify FastAPI Upload Endpoint**

**Location**: `fastapi/routers/vertex_service.py`

#### Current Flow (Line ~50-116):
```python
@router.post("/upload")
async def vertex_upload(...):
    # 1. Upload to RAW_BUCKET
    # 2. Create Firestore job
    # 3. Publish to ANALYSIS worker (existing)
```

#### New Flow (Add Encoding Step):

**Option A: Encode BEFORE Analysis (Recommended)**
```python
@router.post("/upload")
async def vertex_upload(
    request: Request,
    filename: str = Body(...),
    video: UploadFile = File(...),
    userId: Optional[str] = None,
    videoDurationSec: Optional[int] = None,
):
    if not video or not video.filename:
        raise HTTPException(status_code=400, detail="Missing file/filename")

    owner_email = request.headers.get("x-owner-email")

    try:
        # 1. Generate job ID + GCS keys
        curr_datetime = datetime.now().strftime("%Y%m%d-%H%M%S")
        job_id = f"{curr_datetime}--{uuid.uuid4()}"
        blob_name, gcs_uri, original_name = _make_keys(video.filename, job_id)

        # 2. Upload video to GCS RAW bucket
        bucket = storage_client.bucket(RAW_BUCKET)
        video.file.seek(0)
        await run_in_threadpool(
            _upload_filelike_to_gcs,
            bucket,
            blob_name,
            video.file,
            video.content_type or "video/mp4",
        )

        # 3. Determine output URI for encoded video
        encoded_blob_name = f"encoded/{job_id}/{original_name.replace('.MOV', '.mp4').replace('.mov', '.mp4')}"
        encoded_gcs_uri = f"gs://{os.environ['GCS_PROCESSED_BUCKET']}/{encoded_blob_name}"

        # 4. Create Firestore job metadata
        _job_doc(job_id).set(
            {
                "jobId": job_id,
                "userId": userId,
                "ownerEmail": owner_email,
                "status": "queued_encoding",  # Changed status
                "pipeline": "vertex",
                "mode": "vertex",
                "title": original_name,
                "visibility": "private",
                "videoGcsUri": gcs_uri,  # Original upload
                "encodedVideoGcsUri": encoded_gcs_uri,  # Will be set by encoder
                "createdAt": firestore.SERVER_TIMESTAMP,
                "videoDurationSec": videoDurationSec or 0,
                "highlightGcsUri": "",
            },
            merge=True,
        )

        # 5. Publish to ENCODING worker (NEW)
        try:
            _publish_encoding_job(job_id, gcs_uri, encoded_gcs_uri, userId, owner_email)
        except Exception as e:
            _job_doc(job_id).update({"status": "encoding_publish_error", "error": str(e)})
            raise HTTPException(status_code=502, detail=f"Encoding enqueue failed: {e}")

        return {
            "ok": True,
            "jobId": job_id,
            "status": "queued_encoding",
            "videoGcsUri": gcs_uri,
            "encodedVideoGcsUri": encoded_gcs_uri
        }

    except Exception as e:
        print("Vertex upload error:", e)
        raise HTTPException(status_code=500, detail=f"Vertex upload failed: {e}")
```

### 4. **Add Encoding Publisher Function**

**Location**: `fastapi/utils.py` (add after existing `_publish_job`)

```python
def _publish_encoding_job(
    job_id: str,
    input_uri: str,
    output_uri: str,
    user_id: Optional[str] = None,
    owner_email: Optional[str] = None
):
    """
    Publish encoding job to Pub/Sub for FFmpeg worker

    Args:
        job_id: Unique job identifier
        input_uri: GCS URI of uploaded video (gs://bucket/path/to/input.mov)
        output_uri: GCS URI for encoded output (gs://bucket/path/to/output.mp4)
        user_id: Optional user ID
        owner_email: Optional owner email
    """
    ENCODING_TOPIC = os.environ["ENCODING_TOPIC"]
    encoding_topic_path = publisher.topic_path(PROJECT_ID, ENCODING_TOPIC)

    payload = {
        "jobId": job_id,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "userId": user_id,
        "ownerEmail": owner_email
    }

    # Publish to encoding topic
    future = publisher.publish(
        encoding_topic_path,
        json.dumps(payload).encode("utf-8")
    )
    future.result()  # Wait for publish to complete
    print(f"Published encoding job {job_id} to {encoding_topic_path}")
```

### 5. **Add Encoding Completion Webhook (Optional but Recommended)**

**Location**: `fastapi/routers/vertex_service.py` (add new endpoint)

This endpoint will be called by the encoding worker OR monitored via Firestore trigger.

```python
@router.post("/encoding-complete")
async def encoding_complete(request: Request):
    """
    Webhook called when encoding is complete
    Triggers the analysis worker with encoded video
    """
    data = await request.json()
    job_id = data.get("jobId")
    encoded_uri = data.get("output_uri")

    if not job_id or not encoded_uri:
        raise HTTPException(status_code=400, detail="Missing jobId or output_uri")

    # Get job from Firestore
    job_snap = _job_doc(job_id).get()
    if not job_snap.exists:
        raise HTTPException(status_code=404, detail="Job not found")

    job_data = job_snap.to_dict()

    # Update job with encoded URI
    _job_doc(job_id).update({
        "encodedVideoGcsUri": encoded_uri,
        "status": "queued_analysis"
    })

    # NOW publish to analysis worker with ENCODED video
    try:
        _publish_job(
            job_id,
            encoded_uri,  # Use encoded video, not original
            user_id=job_data.get("userId"),
            owner_email=job_data.get("ownerEmail"),
            mode="vertex"
        )
    except Exception as e:
        _job_doc(job_id).update({"status": "analysis_publish_error", "error": str(e)})
        raise HTTPException(status_code=502, detail=f"Analysis enqueue failed: {e}")

    return {"ok": True, "status": "queued_analysis"}
```

### 6. **Update Analysis Worker to Use Encoded Video**

**Location**: `worker/main.py` (your existing analysis worker)

Ensure the analysis worker uses `encodedVideoGcsUri` instead of `videoGcsUri` if encoding is enabled:

```python
# In your analysis worker message handler
def process_message(message):
    data = json.loads(message.data.decode())
    job_id = data["jobId"]

    # Get job from Firestore
    job_doc = db.collection("jobs").document(job_id)
    job_data = job_doc.get().to_dict()

    # Use encoded video if available, otherwise fall back to original
    video_uri = job_data.get("encodedVideoGcsUri") or job_data.get("videoGcsUri")

    # Continue with analysis...
```

---

## Alternative: Cloud Functions Trigger (Simpler)

Instead of a webhook, use a **Firestore trigger** to automatically start analysis when encoding completes:

**Create**: `functions/on-encoding-complete.py`

```python
from google.cloud import firestore, pubsub_v1
import os

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
publisher = pubsub_v1.PublisherClient()

def on_encoding_complete(event, context):
    """Triggered when job.status changes to 'ENCODED'"""

    # Get the updated document
    job_id = context.resource.split('/')[-1]
    db = firestore.Client(project=PROJECT_ID)
    job_ref = db.collection("jobs").document(job_id)
    job_data = job_ref.get().to_dict()

    # Check if status is ENCODED
    if job_data.get("status") != "ENCODED":
        return

    # Publish to analysis worker
    topic_path = publisher.topic_path(PROJECT_ID, "your-analysis-topic")
    payload = {
        "jobId": job_id,
        "videoGcsUri": job_data["encodedVideoGcsUri"],
        "outBucket": job_data.get("outBucket"),
        "userId": job_data.get("userId"),
        "ownerEmail": job_data.get("ownerEmail"),
        "mode": "vertex"
    }

    publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
    job_ref.update({"status": "queued_analysis"})
```

Deploy with:
```bash
gcloud functions deploy on-encoding-complete \
  --runtime python311 \
  --trigger-event providers/cloud.firestore/eventTypes/document.write \
  --trigger-resource "projects/YOUR_PROJECT/databases/(default)/documents/jobs/{jobId}" \
  --project=YOUR_PROJECT_ID
```

---

## Deployment Steps

### 1. Build and Deploy Encoding Worker to Cloud Run

```bash
cd encoding

# Build Docker image
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/ffmpeg-encoder

# Deploy to Cloud Run
gcloud run deploy ffmpeg-encoder \
  --image gcr.io/YOUR_PROJECT_ID/ffmpeg-encoder \
  --platform managed \
  --region us-central1 \
  --memory 4Gi \
  --cpu 2 \
  --timeout 3600 \
  --concurrency 1 \
  --min-instances 0 \
  --max-instances 10 \
  --set-env-vars GCP_PROJECT_ID=YOUR_PROJECT_ID,\
PUBSUB_SUBSCRIPTION=encoding-worker-sub,\
PUBSUB_TOPIC=encoding-jobs,\
GCS_RAW_BUCKET=your-raw-bucket,\
GCS_PROCESSED_BUCKET=your-processed-bucket,\
FFMPEG_PRESET=fast,\
FFMPEG_CRF=23,\
TARGET_HEIGHT=720 \
  --service-account=YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com
```

### 2. Grant Permissions

```bash
# Allow Cloud Run to pull from Pub/Sub
gcloud run services add-iam-policy-binding ffmpeg-encoder \
  --member=serviceAccount:service-YOUR_PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com \
  --role=roles/run.invoker \
  --region=us-central1

# Allow service account to read/write GCS
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member=serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin

# Allow service account to update Firestore
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member=serviceAccount:YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com \
  --role=roles/datastore.user
```

---

## Testing the Integration

### 1. Test Upload Flow

```bash
# Upload a video
curl -X POST "https://your-api.com/vertex/upload" \
  -H "x-owner-email: test@example.com" \
  -F "filename=test.mov" \
  -F "video=@test.mov" \
  -F "userId=test-user"

# Response should include:
# {
#   "ok": true,
#   "jobId": "20260108-123456--uuid",
#   "status": "queued_encoding",
#   "encodedVideoGcsUri": "gs://processed/encoded/..."
# }
```

### 2. Monitor Logs

```bash
# Encoding worker logs
gcloud run services logs read ffmpeg-encoder --region=us-central1

# Look for:
# - "Received encoding job: ..."
# - "Starting encode: ..."
# - "Encoding completed successfully in X.XXs"
```

### 3. Check Firestore

```javascript
// Job document should progress through statuses:
// queued_encoding → ENCODING → ENCODED → queued_analysis → processing → done
```

---

## Summary of Routing Points

| **What** | **Where** | **Action** |
|----------|-----------|------------|
| **Upload Endpoint** | `fastapi/routers/vertex_service.py:50` | Modify to publish to encoding topic |
| **Publisher Function** | `fastapi/utils.py:69` | Add `_publish_encoding_job()` |
| **Encoding Complete Handler** | `fastapi/routers/vertex_service.py` (new endpoint) | Add `/encoding-complete` webhook |
| **Analysis Worker** | `worker/main.py` | Update to use `encodedVideoGcsUri` |
| **Environment Variables** | `.env` files | Add `ENCODING_TOPIC`, `GCS_PROCESSED_BUCKET` |

---

## Job Status Flow

```
1. queued_encoding     (Upload complete, waiting for encoding)
2. ENCODING            (Encoding worker processing)
3. ENCODED             (Encoding complete, encoded video in GCS)
4. queued_analysis     (Waiting for analysis worker)
5. processing          (Analysis in progress)
6. done                (Complete)
```

## Error Handling

The worker handles these errors gracefully:
- ✅ Invalid GCS URIs → `GCSError`
- ✅ Missing files → `FileNotFoundError`
- ✅ FFmpeg failures → `FFmpegError`
- ✅ Upload/download failures → `GCSError`
- ✅ All errors logged with stack traces
- ✅ Firestore updated with error status
- ✅ Pub/Sub messages nacked for retry

---

## Need Help?

Check these logs if issues occur:
1. **Encoding Worker**: `gcloud run services logs read ffmpeg-encoder`
2. **Pub/Sub**: Check dead-letter topic for failed messages
3. **Firestore**: Check job document `status` and `error` fields
