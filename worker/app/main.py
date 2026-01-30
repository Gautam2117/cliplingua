import os
import json
import uuid
import time
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
TMP_DIR = Path(os.getenv("TMP_DIR", "./tmp")).resolve()

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


def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str]:
    """
    Run command and capture combined stdout/stderr.
    """
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def now_ts() -> float:
    return float(time.time())


def process_job(job_id: str, url: str):
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    # yt-dlp should write separate files then merge -> final mp4
    # IMPORTANT: output template must include %(ext)s, not a fixed .mp4
    out_template = tmp_job_dir / "video.%(ext)s"
    out_video = tmp_job_dir / "video.mp4"
    out_audio = tmp_job_dir / "audio.wav"
    out_log = tmp_job_dir / "log.txt"

    job = load_job(job_id)
    job["status"] = "running"
    job["updated_at"] = now_ts()
    save_job(job_id, job)

    log_lines: List[str] = []
    log_lines.append(f"job_id={job_id}")
    log_lines.append(f"url={url}")
    log_lines.append(f"tmp_dir={tmp_job_dir}")
    log_lines.append("")

    try:
        # 1) Download best video+audio then merge to mp4
        # Key fix: JS challenge solving via node
        dl_cmd = [
            "yt-dlp",
            "--js-runtimes", "node",
            "-f", "bv*+ba/b",
            "--merge-output-format", "mp4",
            "-o", str(out_template),
            url,
        ]
        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)

        if rc != 0:
            raise RuntimeError(f"download failed (rc={rc})")

        if not out_video.exists():
            # yt-dlp sometimes chooses different final name if template mismatched
            # but with out_template + merge mp4, we expect video.mp4
            raise RuntimeError("download failed (video.mp4 not found)")

        # 2) Extract audio to wav (16k mono)
        ff_cmd = [
            "ffmpeg",
            "-y",
            "-i", str(out_video),
            "-ac", "1",
            "-ar", "16000",
            str(out_audio),
        ]
        rc, out = run_cmd(ff_cmd, cwd=tmp_job_dir)
        log_lines.append("")
        log_lines.append("== ffmpeg ==")
        log_lines.append(out)

        if rc != 0 or not out_audio.exists():
            raise RuntimeError("audio extraction failed")

        out_log.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

        job = load_job(job_id)
        job["status"] = "done"
        job["updated_at"] = now_ts()
        job["artifacts"] = {
            "video_path": str(out_video),
            "audio_path": str(out_audio),
            "log_path": str(out_log),
        }
        job.pop("error", None)
        save_job(job_id, job)

    except Exception as e:
        # Always write log
        out_log.write_text(
            "\n".join(log_lines) + f"\nERROR: {type(e).__name__}: {e}\n",
            encoding="utf-8",
        )
        job = load_job(job_id)
        job["status"] = "error"
        job["updated_at"] = now_ts()
        job["error"] = f"{type(e).__name__}: {e}"
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
        "created_at": now_ts(),
        "updated_at": now_ts(),
        "artifacts": {},
    }
    save_job(job_id, payload)

    # Spawn background worker as a separate Python process
    # Avoid quoting issues by passing args to a tiny runner command.
    worker_root = Path(__file__).resolve().parents[1]  # .../worker
    subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from app.main import process_job; import sys; process_job(sys.argv[1], sys.argv[2])",
            job_id,
            str(body.url),
        ],
        cwd=str(worker_root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return load_job(job_id)
