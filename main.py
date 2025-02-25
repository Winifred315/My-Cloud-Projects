import json
import os
import logging
import subprocess
from google.cloud import storage, pubsub_v1

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ID = "verse-dev-433901"
LOCATION = "us-east4"
SOURCE_BUCKET_NAME = "vodunprocessedgcp"
DESTINATION_BUCKET_NAME = "vodprocessedgcp"
PUBSUB_TOPIC_NAME = "verse-dev-433901-topic"

def transcoder_handler(request):
    try:
        # Parse incoming request
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"statusCode": 400, "message": "Invalid input. JSON body required."}, 400

        # Initialize storage client and fetch the latest video file
        storage_client = storage.Client()
        latest_blob = get_latest_blob(storage_client, SOURCE_BUCKET_NAME)
        if not latest_blob:
            return {"statusCode": 404, "message": "No files found in source bucket."}, 404

        source_gcs_object = latest_blob.name
        source_gcs_uri = f"gs://{SOURCE_BUCKET_NAME}/{source_gcs_object}"
        base_file_name, _ = os.path.splitext(os.path.basename(source_gcs_object))
        logger.info(f"Selected file for processing: {source_gcs_uri}")

        # Copy original file to destination bucket
        copy_original_file(storage_client, latest_blob, base_file_name)

        # Generate DASH outputs
        dash_output_path = f"{base_file_name}/dash/"
        dash_output_dir = f"/tmp/{base_file_name}_dash"
        generate_dash_files(base_file_name, source_gcs_object, dash_output_path, dash_output_dir)

        # Generate thumbnail
        generate_thumbnail(base_file_name, source_gcs_object)

        # Ensure the TXT output directory exists
        txt_output_dir = f"/tmp/{base_file_name}_txt"
        if not os.path.exists(txt_output_dir):
            os.makedirs(txt_output_dir)
            logger.info(f"Created TXT output directory: {txt_output_dir}")

        # Notify via Pub/Sub
        job_completed_notification(base_file_name)

        # Clear temporary files
        clear_tmp_files(base_file_name)

        return {
            "statusCode": 200,
            "message": f"Transcoding and DASH packaging completed for {source_gcs_object}.",
            "outputPath": dash_output_path,
        }
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        return {"statusCode": 500, "message": "Internal server error.", "error": str(e)}, 500


def get_latest_blob(storage_client, bucket_name):
    """Fetch the most recently uploaded file from the bucket."""
    bucket = storage_client.bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket))
    return max(blobs, key=lambda blob: blob.time_created, default=None)


def copy_original_file(storage_client, source_blob, base_file_name):
    """Copy the original video file to the destination bucket."""
    destination_bucket = storage_client.bucket(DESTINATION_BUCKET_NAME)
    new_blob_name = f"{base_file_name}/original/{source_blob.name}"
    copied_blob = source_blob.bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
    logger.info(f"Original file copied to {copied_blob.name} in destination bucket.")


def generate_dash_files(base_file_name, source_blob_name, output_path, output_dir):
    """Transcode the video and generate DASH output."""
    local_video_path = f"/tmp/{source_blob_name}"
    os.makedirs(output_dir, exist_ok=True)

    # Download the video file locally
    download_blob(SOURCE_BUCKET_NAME, source_blob_name, local_video_path)

    # Transcoding and DASH packaging with FFmpeg
    dash_manifest_path = f"{output_dir}/manifest.mpd"
    ffmpeg_command = [
    "ffmpeg",
    "-loglevel", "error",
    "-i", local_video_path,
    "-filter_complex",
    "[0:v]split=3[vsd][vhd][vuhd];"
    "[vsd]scale=854:480[voutsd];"
    "[vhd]scale=1280:720[vouthd];"
    "[vuhd]scale=1920:1080[voutuhd]",

    # SD Output
    "-map", "[voutsd]", "-map", "0:a",
    "-b:v:0", "2M", "-c:v:0", "libx264", "-g", "120", "-keyint_min", "120",
    "-preset", "fast", "-profile:v:0", "main",

    # HD Output
    "-map", "[vouthd]", "-map", "0:a",
    "-b:v:1", "6M", "-c:v:1", "libx264", "-g", "120", "-keyint_min", "120",
    "-preset", "fast", "-profile:v:1", "main",

    # UHD Output
    "-map", "[voutuhd]", "-map", "0:a",
    "-b:v:2", "10M", "-c:v:2", "libx264", "-g", "120", "-keyint_min", "120",
    "-preset", "fast", "-profile:v:2", "high",

    # DASH Packaging
    "-f", "dash",
    "-use_template", "0",  # Disables templates, enabling explicit segment lists
    "-use_timeline", "0",  # Disables SegmentTimeline
    "-seg_duration", "2",  # Fixed duration for each segment
    "-init_seg_name", "init-$RepresentationID$.mp4",
    "-media_seg_name", "segment-$RepresentationID$-$Number$.m4s",
    "-adaptation_sets", "id=0,streams=v id=1,streams=a",
    dash_manifest_path,
    ]


    subprocess.run(ffmpeg_command, check=True)
    logger.info(f"DASH files generated at: {output_dir}")
    upload_directory(output_dir, DESTINATION_BUCKET_NAME, output_path)

def generate_thumbnail(base_file_name, source_blob_name):
    """Generate a thumbnail for the video."""
    local_video_path = f"/tmp/{source_blob_name}"
    local_thumbnail_path = f"/tmp/{base_file_name}.jpg"

    # FFmpeg command to generate the thumbnail
    ffmpeg_command = [
        "ffmpeg",
        "-loglevel", "error",
        "-y",
        "-i", local_video_path,
        "-ss", "00:00:10",
        "-vframes", "1",
        "-q:v", "2",
        local_thumbnail_path,
    ]
    subprocess.run(ffmpeg_command, check=True)
    logger.info(f"Thumbnail generated at: {local_thumbnail_path}")

    # Upload thumbnail to the destination bucket
    upload_blob(DESTINATION_BUCKET_NAME, local_thumbnail_path, f"{base_file_name}/thumbnail/{base_file_name}.jpg")

def job_completed_notification(file_name):
    """Send a Pub/Sub notification about job completion."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_NAME)
    message = json.dumps({"message": f"Processing completed for {file_name}"}).encode("utf-8")
    publisher.publish(topic_path, message)
    logger.info(f"Notification sent to Pub/Sub: {message}")


def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Download a blob from a bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logger.info(f"Downloaded {source_blob_name} to {destination_file_name}.")


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Upload a file to a bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"Uploaded {source_file_name} to {destination_blob_name} in {bucket_name}.")


def upload_directory(local_directory, bucket_name, destination_prefix):
    """Upload a directory to a bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for root, _, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            destination_blob_name = os.path.join(destination_prefix, relative_path)
            upload_blob(bucket_name, local_path, destination_blob_name)


def clear_tmp_files(base_file_name):
    tmp_paths = [
        f"/tmp/{base_file_name}_dash",
        f"/tmp/{base_file_name}_txt",
        f"/tmp/{base_file_name}.jpg",
    ]
    for tmp_path in tmp_paths:
        if os.path.exists(tmp_path):
            if os.path.isdir(tmp_path):
                for root, _, files in os.walk(tmp_path):
                    for file in files:
                        os.remove(os.path.join(root, file))
                os.rmdir(tmp_path)
            else:
                os.remove(tmp_path)
            logger.info(f"Removed: {tmp_path}")
