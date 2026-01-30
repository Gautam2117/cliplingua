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

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
TMP_DIR = Path(os.getenv("TMP_DIR", "./tmp"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="ClipLingua Worker", version="0.1.0")


class CreateJobBody(BaseModel):
    url: HttpUrl


def job_path(job_id: str) -> Path:
    return DATA_DIR / f"{job_id}.json"


def save_job(job_id: str, payload: dict):
    job_path(job_id).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_job(job_id: str) -> dict:
    p = job_path(job_id)
    if not p.exists():
        raise HTTPException(status_code=404, detail="job not found")
    return json.loads(p.read_text(encoding="utf-8"))


def run_cmd(cmd: list[str], cwd: Optional[Path] = None):
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def process_job(job_id: str, url: str):
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    out_video = tmp_job_dir / "video.mp4"
    out_audio = tmp_job_dir / "audio.wav"
    out_log = tmp_job_dir / "log.txt"

    job = load_job(job_id)
    job["status"] = "running"
    job["updated_at"] = time.time()
    save_job(job_id, job)

    log_lines = []

    try:
        # 1) Download best mp4
        dl_cmd = [
            "yt-dlp",
            "-f",
            "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best",
            "-o",
            str(out_video),
            str(url),
        ]
        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)
        if rc != 0 or not out_video.exists():
            raise RuntimeError("download failed")

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

        out_log.write_text("\n".join(log_lines), encoding="utf-8")

        job = load_job(job_id)
        job["status"] = "done"
        job["updated_at"] = time.time()
        job["artifacts"] = {
            "video_path": str(out_video),
            "audio_path": str(out_audio),
            "log_path": str(out_log),
        }
        save_job(job_id, job)

    except Exception as e:
        out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8")
        job = load_job(job_id)
        job["status"] = "error"
        job["updated_at"] = time.time()
        job["error"] = str(e)
        job["artifacts"] = {"log_path": str(out_log)}
        save_job(job_id, job)


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/jobs")
def create_job(body: CreateJobBody):
    job_id = str(uuid.uuid4())
    payload = {
        "id": job_id,
        "url": str(body.url),
        "status": "queued",
        "created_at": time.time(),
        "updated_at": time.time(),
        "artifacts": {},
    }
    save_job(job_id, payload)

    # simple background process: spawn python thread-free subprocess
    # to keep it minimal and deployable without extra infra
    subprocess.Popen(
        ["python3", "-c", f"from app.main import process_job; process_job('{job_id}', '{body.url}')"],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return load_job(job_id)
