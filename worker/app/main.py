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
from fastapi.responses import FileResponse

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


def wait_for_file(path: Path, min_bytes: int, tries: int = 80, sleep_s: float = 0.25) -> bool:
    for _ in range(tries):
        try:
            if path.exists() and path.stat().st_size >= min_bytes:
                return True
        except FileNotFoundError:
            pass
        time.sleep(sleep_s)
    return False


def list_dir(folder: Path) -> str:
    try:
        items = []
        for p in sorted(folder.rglob("*")):
            rel = p.relative_to(folder)
            if p.is_dir():
                items.append(f"[DIR]  {rel}")
            else:
                try:
                    items.append(f"[FILE] {rel} ({p.stat().st_size} bytes)")
                except Exception:
                    items.append(f"[FILE] {rel} (size?)")
        return "\n".join(items) if items else "<empty>"
    except Exception as e:
        return f"<could not list dir: {e}>"


def process_job(job_id: str, url: str):
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    # Because we set cwd=tmp_job_dir, use filename-only for outputs in that folder
    out_template = "download.%(ext)s"
    merged_name = "download.mp4"
    audio_name = "audio.wav"

    merged_video = tmp_job_dir / merged_name
    out_audio = tmp_job_dir / audio_name
    out_log = tmp_job_dir / "log.txt"

    job = load_job(job_id)
    job["status"] = "running"
    job["updated_at"] = time.time()
    save_job(job_id, job)

    log_lines: list[str] = []

    try:
        # 1) Download via yt-dlp with JS runtime (node) and force merged mp4
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
            out_template,
            str(url),
        ]

        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)

        if rc != 0:
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError(f"download failed (rc={rc})")

        if not wait_for_file(merged_video, min_bytes=1024 * 200):
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError("download produced no merged mp4")

        # 2) Extract audio (16k mono wav)
        # Use relative names because cwd is tmp_job_dir
        ff_cmd_rel = [
            "ffmpeg",
            "-y",
            "-i",
            merged_name,
            "-ac",
            "1",
            "-ar",
            "16000",
            audio_name,
        ]
        rc2, out2 = run_cmd(ff_cmd_rel, cwd=tmp_job_dir)
        log_lines.append("== ffmpeg ==")
        log_lines.append(out2)

        # Fallback: if ffmpeg failed due to path weirdness, run without cwd using absolute paths
        if rc2 != 0:
            ff_cmd_abs = [
                "ffmpeg",
                "-y",
                "-i",
                str(merged_video.resolve()),
                "-ac",
                "1",
                "-ar",
                "16000",
                str(out_audio.resolve()),
            ]
            rc2b, out2b = run_cmd(ff_cmd_abs, cwd=None)
            log_lines.append("== ffmpeg (fallback abs) ==")
            log_lines.append(out2b)
            rc2 = rc2b

        if rc2 != 0 or not wait_for_file(out_audio, min_bytes=1024 * 10):
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            out_log.write_text("\n".join(log_lines), encoding="utf-8")
            raise RuntimeError("audio extraction failed")

        log_lines.append("== DONE ==")
        log_lines.append(f"video={merged_video.resolve()}")
        log_lines.append(f"audio={out_audio.resolve()}")

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

@app.get("/jobs/{job_id}/audio")
def get_job_audio(job_id: str):
    job = load_job(job_id)
    ap = job.get("audio_path")
    if not ap:
        raise HTTPException(status_code=404, detail="audio not ready")
    p = Path(ap)
    if not p.exists():
        raise HTTPException(status_code=404, detail="audio file missing")
    return FileResponse(
        path=str(p),
        media_type="audio/wav",
        filename=f"{job_id}.wav",
    )


@app.get("/jobs/{job_id}/video")
def get_job_video(job_id: str):
    job = load_job(job_id)
    vp = job.get("video_path")
    if not vp:
        raise HTTPException(status_code=404, detail="video not ready")
    p = Path(vp)
    if not p.exists():
        raise HTTPException(status_code=404, detail="video file missing")
    return FileResponse(
        path=str(p),
        media_type="video/mp4",
        filename=f"{job_id}.mp4",
    )

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return load_job(job_id)
