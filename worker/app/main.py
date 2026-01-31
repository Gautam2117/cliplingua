import os
import json
import uuid
import time
import subprocess
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv

from supabase import create_client, Client

load_dotenv()

# Local temp dirs (Render has ephemeral disk, fine for processing)
TMP_DIR = Path(os.getenv("TMP_DIR", "./tmp"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

# Supabase config (set in Render env vars)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "artifacts")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    # Fail fast so you don't deploy a broken worker
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env var")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

app = FastAPI(title="ClipLingua Worker", version="0.2.0")


class CreateJobBody(BaseModel):
    url: HttpUrl


def run_cmd(cmd: list[str], cwd: Optional[Path] = None):
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def db_get_job(job_id: str) -> dict:
    res = supabase.table("jobs").select("*").eq("id", job_id).single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="job not found")
    return res.data


def db_update_job(job_id: str, patch: dict) -> None:
    supabase.table("jobs").update(patch).eq("id", job_id).execute()


def storage_upload(local_path: Path, remote_path: str, content_type: str) -> str:
    """
    Upload file to Supabase Storage and return the remote path.
    """
    with local_path.open("rb") as f:
        # upsert True so retries are safe
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=remote_path,
            file=f,
            file_options={"content-type": content_type, "upsert": "true"},
        )
    return remote_path


def process_job(job_id: str, url: str):
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    out_video = tmp_job_dir / "video.mp4"
    out_audio = tmp_job_dir / "audio.wav"
    out_log = tmp_job_dir / "log.txt"

    log_lines: list[str] = []

    # mark running
    db_update_job(job_id, {"status": "running", "error": None})

    try:
        # 1) Download video using yt-dlp with JS challenge solving via Node
        # This avoids "Only images are available" and signature failures
        dl_cmd = [
            "yt-dlp",
            "--js-runtimes",
            "node",
            "--no-playlist",
            "-f",
            "bv*+ba/b",
            "--merge-output-format",
            "mp4",
            "-o",
            str(out_video),
            str(url),
            "-v",
        ]
        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)

        if rc != 0 or not out_video.exists():
            raise RuntimeError(f"download failed (rc={rc})")

        # 2) Extract audio to wav (16k mono)
        ff_cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(out_video),
            "-ac",
            "1",
            "-ar",
            "16000",
            str(out_audio),
        ]
        rc, out = run_cmd(ff_cmd, cwd=tmp_job_dir)
        log_lines.append("== ffmpeg ==")
        log_lines.append(out)

        if rc != 0 or not out_audio.exists():
            raise RuntimeError("audio extraction failed")

        # Write log file
        out_log.write_text("\n".join(log_lines), encoding="utf-8")

        # 3) Upload artifacts to Supabase Storage
        base = f"jobs/{job_id}"
        video_remote = storage_upload(out_video, f"{base}/video.mp4", "video/mp4")
        audio_remote = storage_upload(out_audio, f"{base}/audio.wav", "audio/wav")
        log_remote = storage_upload(out_log, f"{base}/log.txt", "text/plain")

        # 4) Mark done with storage paths
        db_update_job(
            job_id,
            {
                "status": "done",
                "video_path": video_remote,
                "audio_path": audio_remote,
                "log_path": log_remote,
                "error": None,
            },
        )

    except Exception as e:
        # ensure we have a log
        try:
            out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8")
            # try upload log even on failure
            base = f"jobs/{job_id}"
            log_remote = storage_upload(out_log, f"{base}/log.txt", "text/plain")
            db_update_job(
                job_id,
                {"status": "error", "error": str(e), "log_path": log_remote},
            )
        except Exception:
            db_update_job(job_id, {"status": "error", "error": str(e)})


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/jobs")
def create_job(body: CreateJobBody):
    job_id = str(uuid.uuid4())

    # Insert initial row
    supabase.table("jobs").insert(
        {"id": job_id, "url": str(body.url), "status": "queued"}
    ).execute()

    # Spawn background process (works on Render, no threads needed)
    subprocess.Popen(
        [
            "python3",
            "-c",
            (
                "from app.main import process_job;"
                f"process_job('{job_id}', '{str(body.url)}')"
            ),
        ],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return db_get_job(job_id)
