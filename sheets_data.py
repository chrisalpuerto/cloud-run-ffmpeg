# fastapi 
import gspread
import os
from google.cloud import storage, firestore
from google.cloud import pubsub_v1
from zoneinfo import ZoneInfo
#from utils import _job_doc
COLLETION = os.getenv("FIRESTORE_COLLETION")


print("DEBUGGING:")
print(f"GCP_PROJECT_ID: {os.environ.get('GCP_PROJECT_ID')}")
print(f"GOOGLE_APP_CREDS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
print(f"Credentials file exists: {os.path.exists(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', ''))}")

PROJECT_ID   = os.environ["GCP_PROJECT_ID"]
RAW_BUCKET   = os.environ["GCS_RAW_BUCKET"]
OUT_BUCKET   = os.environ["GCS_OUT_BUCKET"]
TOPIC_NAME   = os.environ["PUBSUB_TOPIC"]
COLLECTION   = os.getenv("FIRESTORE_COLLECTION", "jobs")
#HIGHLIGHT_COL = os.getenv("FIRESTORE_HIGHLIGHT_COL", "Highlights")
SERVICE_ACCOUNT = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

storage_client   = storage.Client(project=PROJECT_ID)
firestore_client = firestore.Client(project=PROJECT_ID)
publisher        = pubsub_v1.PublisherClient()
topic_path       = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


def _job_doc():
    pass
def sec_to_time(sec):
    # Convert seconds to minutes and seconds
    minutes, seconds = divmod(sec, 60)
    return f"{minutes}m {seconds}s"

# function that writes data to sheet ones job is done
def write_to_sheet(job_id):
    # sheets creds
    ss_creds = os.getenv('SHEETS_CREDS')
    # open service acount for sheets
    gc = gspread.service_account(filename=ss_creds)
    # open sheet
    sh = gc.open("HoopTuber-JobDurationData")
    sheet = sh.sheet1
    
    # get job doc data here
    doc_ref = _job_doc(job_id)
    snap = doc_ref.get()
    if not snap.exists:
        print("Doc does not exist")
        return
    data = snap.to_dict()
    try:
        if data["status"] == "done":
            # data from doc + how long the job took
            owner = data["userId"]
            start_time = data["createdAt"] # createdAt is a datetime object
            end_time = data["finishedAt"] # finishedAt is a datetime object
            start_time_str = str(start_time)
            end_time_str = str(end_time)

            duration = sec_to_time(data["videoDurationSec"]) # videoDurationSec is in seconds
            print(f"start time: {start_time}")
            print(f"end time: {end_time}")
            print(f"duration: {duration} and type: {type(duration)}")
            how_long = sec_to_time((end_time - start_time).total_seconds())
            add_data = [job_id, owner, duration, how_long, start_time_str, end_time_str]
            sheet.append_row(add_data)
            print(f"Added data for job: {job_id}")
        else:
            print(f"Job is not done yet: {data['status']}")
    except Exception as e:
        print(f"Error writing to sheet: {e}")