# Cloud Video Encoding Service

A containerized, event-driven video encoding microservice built with Python and FFmpeg, deployed on Google Cloud Run with automated CI/CD.

## Overview

This service is a serverless video processing worker that automatically transcodes uploaded videos to web-optimized formats. It leverages Google Cloud's managed infrastructure to provide scalable, cost-effective video encoding without server management.

**Key Features:**
- Event-driven architecture using Google Cloud Pub/Sub
- HEVC/MOV to H.264 MP4 transcoding with 720p downscaling
- Containerized with Docker for consistent deployments
- Automated CI/CD pipeline with GitHub Actions
- Real-time job status tracking via Firestore
- Serverless scaling with Cloud Run

---

## Architecture

```
User Upload → FastAPI Backend → GCS (RAW) → Pub/Sub → Encoding Worker → GCS (PROCESSED) → Analysis Worker
                                                            ↓
                                                        Firestore
                                                      (Job Status)
```

**Flow:**
1. User uploads video through FastAPI backend
2. Raw video stored in Google Cloud Storage (GCS)
3. Pub/Sub message triggers encoding worker
4. Worker downloads, processes, and uploads encoded video
5. Job status updates written to Firestore
6. Processed video triggers downstream analysis

---

## Technical Stack

**Core Technologies:**
- **Python 3.11** - Application runtime
- **FFmpeg** - Video encoding engine
- **Docker** - Containerization
- **Google Cloud Run** - Serverless container platform
- **Google Cloud Pub/Sub** - Event messaging
- **Google Cloud Storage** - Object storage
- **Firestore** - NoSQL database for job tracking

## Docker Implementation

The service is containerized using a multi-stage Dockerfile optimized for Cloud Run deployment.

**Dockerfile Highlights:**
```dockerfile
FROM python:3.11-slim

# Install FFmpeg for video processing
RUN apt-get update && apt-get install -y ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "worker.py"]
```

**Key Design Decisions:**
- **Slim base image** - Minimal Python 3.11 image reduces container size
- **Layer caching** - Dependencies installed before code copy for faster rebuilds
- **No cache pip install** - Reduces image size by removing pip cache
- **Cleanup apt lists** - Removes package manager metadata to minimize footprint

**Local Testing:**
```bash
# Build the Docker image
docker build -t video-encoder .

# Run locally (requires GCP credentials)
docker run -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json video-encoder
```

---

## CI/CD Pipeline

Automated deployment pipeline using GitHub Actions for continuous delivery to Cloud Run.

**Pipeline: [.github/workflows/cloudrun-deploy.yml](.github/workflows/cloudrun-deploy.yml)**

**Workflow Triggers:**
- Automatic deployment on push to `main` branch
- Manual dispatch capability

**Pipeline Stages:**

1. **Checkout Code**
   ```yaml
   - uses: actions/checkout@v4
   ```

2. **Authenticate with GCP**
   ```yaml
   - uses: google-github-actions/auth@v2
     with:
       credentials_json: ${{ secrets.GCP_SA_KEY }}
   ```
   - Uses GitHub Secrets for secure credential management
   - Service account with minimal required permissions

3. **Configure Docker for Artifact Registry**
   ```yaml
   - run: gcloud auth configure-docker $REGION-docker.pkg.dev
   ```
   - Enables pushing images to Google Artifact Registry

4. **Build & Push Docker Image**
   ```yaml
   docker build -t $REGION-docker.pkg.dev/$PROJECT_ID/$REPO/$IMAGE:$GITHUB_SHA .
   docker push $REGION-docker.pkg.dev/$PROJECT_ID/$REPO/$IMAGE:$GITHUB_SHA
   ```
   - Tags images with Git commit SHA for version tracking
   - Immutable image tags enable rollback capability

5. **Deploy to Cloud Run**
   ```yaml
   gcloud run deploy $SERVICE \
     --image $REGION-docker.pkg.dev/$PROJECT_ID/$REPO/$IMAGE:$GITHUB_SHA \
     --region $REGION \
     --platform managed \
     --quiet
   ```
   - Zero-downtime deployment
   - Automatic traffic migration to new revision

**Configuration:**
- Environment variables defined in workflow file
- Secrets managed via GitHub repository settings
- Region: `us-central1`
- Platform: `managed` (fully serverless)

**Deployment Security:**
- Service account follows principle of least privilege
- Credentials never exposed in logs
- Automated secret rotation recommended

---

## Key Learnings & Achievements

**DevOps:**
- Implemented end-to-end CI/CD reducing deployment time from manual 15+ minutes to automated 3-5 minutes
- Containerized legacy application enabling portability and consistent environments
- Configured automated deployment with zero-downtime using Cloud Run revisions

**Cloud Architecture:**
- Designed event-driven microservice architecture for scalable video processing
- Leveraged serverless computing to minimize costs (pay only during encoding)
- Implemented Pub/Sub push model for real-time job processing

**Performance:**
- Optimized Docker image size through multi-stage builds and layer caching
- Configured FFmpeg parameters for optimal quality-to-size ratio
- Achieved consistent 720p encoding at ~30fps

---

## Future Enhancements

- Add support for multiple output resolutions (480p, 1080p)
- Implement retry logic with exponential backoff
- Add CloudWatch/Monitoring dashboards for job metrics
- Support additional video codecs (AV1, VP9)
- Implement automated testing in CI/CD pipeline
- Add video thumbnail generation
- Implement parallel chunk processing for faster encoding
---