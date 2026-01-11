import subprocess
import logging
import os
from config import TARGET_HEIGHT, CRF, FFMPEG_PRESET

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FFmpegError(Exception):
    """Custom exception for FFmpeg encoding errors"""
    pass


def encode(input_path: str, output_path: str):
    """
    Encode video from HEVC/MOV to H.264 MP4 at 720p

    Args:
        input_path: Path to input video file
        output_path: Path to output MP4 file

    Raises:
        FFmpegError: If encoding fails
        FileNotFoundError: If input file doesn't exist
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

    logger.info(f"Starting encode: {input_path} ({input_size / 1024 / 1024:.2f} MB) -> {output_path}")
    logger.info(f"Target: {TARGET_HEIGHT}p, CRF: {CRF}, Preset: {FFMPEG_PRESET}")

    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-vf", f"scale=-2:{TARGET_HEIGHT}",  # -2 ensures even dimensions
        "-c:v", "libx264",
        "-preset", FFMPEG_PRESET,
        "-crf", str(CRF),
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
