import subprocess
import logging
import os
import json
from config import TARGET_HEIGHT, CRF, FFMPEG_PRESET

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FFmpegError(Exception):
    """Custom exception for FFmpeg encoding errors"""
    pass


class VideoAlreadyOptimized(Exception):
    """Exception raised when video is already optimized and doesn't need encoding"""
    pass


def get_video_info(input_path: str) -> dict:
    if not os.path.exists(input_path):
        error_msg = f"Input file not found: {input_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        input_path
    ]

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=30
        )

        probe_data = json.loads(result.stdout)

        # Find video stream
        video_stream = None
        for stream in probe_data.get('streams', []):
            if stream.get('codec_type') == 'video':
                video_stream = stream
                break

        if not video_stream:
            raise FFmpegError("No video stream found in file")

        # Extract format name from container format
        format_name = probe_data.get('format', {}).get('format_name', '')

        return {
            'codec_name': video_stream.get('codec_name', ''),
            'width': int(video_stream.get('width', 0)),
            'height': int(video_stream.get('height', 0)),
            'format_name': format_name
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"ffprobe failed with exit code {e.returncode}"
        logger.error(error_msg)
        logger.error(f"ffprobe stderr: {e.stderr}")
        raise FFmpegError(f"{error_msg}: {e.stderr}")

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse ffprobe output: {e}"
        logger.error(error_msg)
        raise FFmpegError(error_msg)

    except Exception as e:
        error_msg = f"Unexpected error during ffprobe: {str(e)}"
        logger.error(error_msg)
        raise FFmpegError(error_msg)


def encode(input_path: str, output_path: str):
    """
    Encode video from HEVC/MOV to H.264 MP4 at 720p

    Skips encoding if video is already MP4 and resolution is 720p or lower.

    Raises:
        FFmpegError: If encoding fails
        FileNotFoundError: If input file doesn't exist
        VideoAlreadyOptimized: If video is already optimized (MP4 with 720p or lower resolution)
    """
    # Validate input file exists
    if not os.path.exists(input_path):
        error_msg = f"Input file not found: {input_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # Validate input file size
    input_size = os.path.getsize(input_path)
    if input_size == 0:
        error_msg = f"Input file is empty: {input_path}"
        logger.error(error_msg)
        raise FFmpegError(error_msg)

    # Check if video is already optimized
    logger.info(f"Checking video info for: {input_path}")
    try:
        video_info = get_video_info(input_path)
        logger.info(f"Video info - Format: {video_info['format_name']}, "
                   f"Codec: {video_info['codec_name']}, "
                   f"Resolution: {video_info['width']}x{video_info['height']}")

        # Check if already MP4 and resolution is 720p or lower
        is_mp4 = 'mp4' in video_info['format_name'].lower()
        height = video_info['height']

        if height <= TARGET_HEIGHT:
            msg = (f"Video already optimized: MP4 format with {height}p resolution "
                  f"(<= {TARGET_HEIGHT}p target). Skipping encoding.")
            logger.info(msg)
            raise VideoAlreadyOptimized(msg)

    except VideoAlreadyOptimized:
        # Re-raise VideoAlreadyOptimized
        raise
    except Exception as e:
        # Log ffprobe errors but continue with encoding
        logger.warning(f"Could not probe video info, continuing with encoding: {e}")

    logger.info(f"Starting encode: {input_path} ({input_size / 1024 / 1024:.2f} MB) -> {output_path}")
    logger.info(f"Target: {TARGET_HEIGHT}p, CRF: {CRF}, Preset: {FFMPEG_PRESET}")

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", f"scale=-2:{TARGET_HEIGHT}",  # -2 ensures even dimensions
        "-c:v", "libx264",
        "-preset", FFMPEG_PRESET,
        "-crf", str(CRF),
        "-pix_fmt", "yuv420p",  # Ensure compatibility
        "-r", "30",  # Standardize frame rate
        "-movflags", "+faststart",  # Enable progressive download
        "-c:a", "aac",
        "-b:a", "128k",  # Audio bitrate for consistent quality
        output_path,
    ]

    try:
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout for large files
        )

        # Validate output file was created
        if not os.path.exists(output_path):
            error_msg = "FFmpeg completed but output file was not created"
            logger.error(error_msg)
            raise FFmpegError(error_msg)

        output_size = os.path.getsize(output_path)
        if output_size == 0:
            error_msg = "FFmpeg created empty output file"
            logger.error(error_msg)
            raise FFmpegError(error_msg)

        logger.info(f"Encode successful: {output_path} ({output_size / 1024 / 1024:.2f} MB)")
        logger.info(f"Compression ratio: {input_size / output_size:.2f}x")

    except subprocess.TimeoutExpired:
        error_msg = f"FFmpeg encoding timed out after 1 hour for {input_path}"
        logger.error(error_msg)
        raise FFmpegError(error_msg)

    except subprocess.CalledProcessError as e:
        error_msg = f"FFmpeg encoding failed with exit code {e.returncode}"
        logger.error(error_msg)
        logger.error(f"FFmpeg stderr: {e.stderr}")
        logger.error(f"FFmpeg stdout: {e.stdout}")
        raise FFmpegError(f"{error_msg}: {e.stderr}")

    except Exception as e:
        error_msg = f"Unexpected error during encoding: {str(e)}"
        logger.error(error_msg)
        raise FFmpegError(error_msg)

def process_job():
    return ""