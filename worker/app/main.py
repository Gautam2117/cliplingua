import os
import json
import uuid
import time
import subprocess
from pathlib import Path
from typing import Optional, Tuple
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv
from fastapi.responses import PlainTextResponse

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


def run_cmd(cmd: list[str], cwd: Optional[Path] = None) -> Tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def wait_for_file(path: Path, min_bytes: int, tries: int = 20, sleep_s: float = 0.25) -> bool:
    for _ in range(tries):
        if path.exists():
            try:
                if path.stat().st_size >= min_bytes:
                    return True
            except FileNotFoundError:
                pass
        time.sleep(sleep_s)
    return False


def process_job(job_id: str, url: str):
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    out_template = tmp_job_dir / "download.%(ext)s"
    merged_video = tmp_job_dir / "download.mp4"
    out_audio = tmp_job_dir / "audio.wav"
    out_log = tmp_job_dir / "log.txt"

    job = load_job(job_id)
    job["status"] = "running"
    job["updated_at"] = time.time()
    save_job(job_id, job)

    log_lines: list[str] = []

    try:
        # 1) Download and merge into download.mp4
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
            str(out_template),
            str(url),
        ]

        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)

        # yt-dlp may exit 0 even if nothing usable was produced.
        # We deterministically expect download.mp4 after merge.
        if rc != 0:
            raise RuntimeError(f"download failed (rc={rc})")

        if not wait_for_file(merged_video, min_bytes=1024 * 200):  # 200KB sanity
            # dump directory listing for debugging
            try:
                files = "\n".join(sorted([p.name for p in tmp_job_dir.iterdir()]))
            except Exception:
                files = "<could not list tmp dir>"
            log_lines.append("== tmp dir listing ==")
            log_lines.append(files)
            raise RuntimeError("download produced no merged mp4")

        # 2) Extract audio to wav (16k mono)
        ff_cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(merged_video),
            "-ac",
            "1",
            "-ar",
            "16000",
            str(out_audio),
        ]
        rc2, out2 = run_cmd(ff_cmd, cwd=tmp_job_dir)
        log_lines.append("== ffmpeg ==")
        log_lines.append(out2)

        if rc2 != 0 or not wait_for_file(out_audio, min_bytes=1024 * 10):
            raise RuntimeError("audio extraction failed")

        out_log.write_text("\n".join(log_lines), encoding="utf-8")

        job = load_job(job_id)
        job["status"] = "done"
        job["updated_at"] = time.time()
        job["error"] = None
        job["video_path"] = str(merged_video.resolve())
        job["audio_path"] = str(out_audio.resolve())
        job["log_path"] = str(out_log.resolve())
        save_job(job_id, job)

    except Exception as e:
        out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8")
        job = load_job(job_id)
        job["status"] = "error"
        job["updated_at"] = time.time()
        job["error"] = str(e)
        job["video_path"] = None
        job["audio_path"] = None
        job["log_path"] = str(out_log.resolve())
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
        "error": None,
        "video_path": None,
        "audio_path": None,
        "log_path": None,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    save_job(job_id, payload)

    subprocess.Popen(
        ["python3", "-c", f"from app.main import process_job; process_job('{job_id}', '{body.url}')"],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}/log", response_class=PlainTextResponse)
def get_job_log(job_id: str):
    job = load_job(job_id)
    lp = job.get("log_path")
    if not lp:
        raise HTTPException(status_code=404, detail="log not ready")
    p = Path(lp)
    if not p.exists():
        raise HTTPException(status_code=404, detail="log file missing")
    return p.read_text(encoding="utf-8", errors="ignore")


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return load_job(job_id)
