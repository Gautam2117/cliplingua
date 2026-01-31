import os
import sys
import json
import uuid
import time
import subprocess
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, Response
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
TMP_DIR = Path(os.getenv("TMP_DIR", "./tmp"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="ClipLingua Worker", version="0.3.0")


class CreateJobBody(BaseModel):
    url: HttpUrl


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def job_path(job_id: str) -> Path:
    return DATA_DIR / f"{job_id}.json"


def save_job(job_id: str, payload: Dict[str, Any]) -> None:
    job_path(job_id).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_job(job_id: str) -> Dict[str, Any]:
    p = job_path(job_id)
    if not p.exists():
        raise HTTPException(status_code=404, detail="job not found")
    return json.loads(p.read_text(encoding="utf-8"))


def update_job(job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    job = load_job(job_id)
    job.update(patch)
    save_job(job_id, job)
    return job


def run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> Tuple[int, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, proc.stdout


def wait_for_file(path: Path, min_bytes: int, tries: int = 140, sleep_s: float = 0.25) -> bool:
    for _ in range(tries):
        try:
            if path.exists() and path.is_file() and path.stat().st_size >= min_bytes:
                return True
        except FileNotFoundError:
            pass
        time.sleep(sleep_s)
    return False


def list_dir(folder: Path) -> str:
    try:
        items: List[str] = []
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


def safe_file_or_404(path_str: str, msg: str) -> Path:
    p = Path(path_str)
    if not p.exists() or not p.is_file():
        raise HTTPException(status_code=404, detail=msg)
    return p


def process_job(job_id: str, url: str) -> None:
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    out_template = "download.%(ext)s"
    merged_name = "download.mp4"
    audio_name = "audio.wav"

    merged_video = tmp_job_dir / merged_name
    out_audio = tmp_job_dir / audio_name
    out_log = tmp_job_dir / "log.txt"

    update_job(
        job_id,
        {
            "status": "running",
            "updated_at": now_iso(),
            "updated_at_ts": time.time(),
        },
    )

    log_lines: List[str] = []

    try:
        # 1) yt-dlp download + merge to mp4
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

        # 2) ffmpeg audio extraction (16k mono wav)
        ff_cmd_rel = ["ffmpeg", "-y", "-i", merged_name, "-ac", "1", "-ar", "16000", audio_name]
        rc2, out2 = run_cmd(ff_cmd_rel, cwd=tmp_job_dir)
        log_lines.append("== ffmpeg ==")
        log_lines.append(out2)

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
            raise RuntimeError("audio extraction failed")

        log_lines.append("== DONE ==")
        log_lines.append(f"video={merged_video.resolve()}")
        log_lines.append(f"audio={out_audio.resolve()}")

        out_log.write_text("\n".join(log_lines), encoding="utf-8")

        update_job(
            job_id,
            {
                "status": "done",
                "error": None,
                "video_path": str(merged_video.resolve()),
                "audio_path": str(out_audio.resolve()),
                "log_path": str(out_log.resolve()),
                "updated_at": now_iso(),
                "updated_at_ts": time.time(),
            },
        )

    except Exception as e:
        try:
            out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8")
        except Exception:
            pass

        update_job(
            job_id,
            {
                "status": "error",
                "error": str(e),
                "video_path": None,
                "audio_path": None,
                "log_path": str(out_log.resolve()),
                "updated_at": now_iso(),
                "updated_at_ts": time.time(),
            },
        )


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/routes")
def routes():
    out = []
    for r in app.router.routes:
        methods = sorted(list(getattr(r, "methods", []) or []))
        path = getattr(r, "path", "")
        name = getattr(r, "name", "")
        if path:
            out.append({"path": path, "methods": methods, "name": name})
    return {"routes": out}


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
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "updated_at_ts": time.time(),
    }
    save_job(job_id, payload)

    # Safer than string interpolation into Python code:
    # pass params via env and run a tiny inline runner.
    env = os.environ.copy()
    env["CLIPLINGUA_JOB_ID"] = job_id
    env["CLIPLINGUA_JOB_URL"] = str(body.url)

    runner = (
        "import os;"
        "from app.main import process_job;"
        "process_job(os.environ['CLIPLINGUA_JOB_ID'], os.environ['CLIPLINGUA_JOB_URL'])"
    )

    subprocess.Popen(
        [sys.executable, "-c", runner],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return load_job(job_id)


@app.get("/jobs/{job_id}/log", response_class=PlainTextResponse)
def get_job_log(job_id: str):
    job = load_job(job_id)
    lp = job.get("log_path")
    if not lp:
        raise HTTPException(status_code=404, detail="log not ready")
    p = safe_file_or_404(lp, "log file missing")
    return p.read_text(encoding="utf-8", errors="ignore")


def _file_head_or_404(path_str: Optional[str]) -> Response:
    if not path_str:
        return Response(status_code=404)
    p = Path(path_str)
    if not p.exists() or not p.is_file():
        return Response(status_code=404)
    return Response(status_code=200)


@app.get("/jobs/{job_id}/audio")
def get_job_audio(job_id: str):
    job = load_job(job_id)
    ap = job.get("audio_path")
    if not ap:
        raise HTTPException(status_code=404, detail="audio not ready")
    p = safe_file_or_404(ap, "audio file missing")
    return FileResponse(path=str(p), media_type="audio/wav", filename=f"{job_id}.wav")


@app.head("/jobs/{job_id}/audio")
def head_job_audio(job_id: str):
    job = load_job(job_id)
    return _file_head_or_404(job.get("audio_path"))


@app.get("/jobs/{job_id}/video")
def get_job_video(job_id: str):
    job = load_job(job_id)
    vp = job.get("video_path")
    if not vp:
        raise HTTPException(status_code=404, detail="video not ready")
    p = safe_file_or_404(vp, "video file missing")
    return FileResponse(path=str(p), media_type="video/mp4", filename=f"{job_id}.mp4")


@app.head("/jobs/{job_id}/video")
def head_job_video(job_id: str):
    job = load_job(job_id)
    return _file_head_or_404(job.get("video_path"))
