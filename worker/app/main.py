import os
import sys
import json
import uuid
import time
import base64
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, Response
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/cliplingua/data"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp/cliplingua/tmp"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

DATA_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="ClipLingua Worker", version="0.6.2")


class CreateJobBody(BaseModel):
    url: HttpUrl


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def job_path(job_id: str) -> Path:
    return DATA_DIR / f"{job_id}.json"


def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def save_job(job_id: str, payload: Dict[str, Any]) -> None:
    atomic_write_text(job_path(job_id), json.dumps(payload, indent=2))


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


def wait_for_file(path: Path, min_bytes: int, tries: int = 200, sleep_s: float = 0.25) -> bool:
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


def require_bin(name: str) -> str:
    p = shutil.which(name)
    if not p:
        raise RuntimeError(f"missing dependency: {name} not found in PATH")
    return p


def _artifact_urls(job_id: str) -> Dict[str, Optional[str]]:
    if not PUBLIC_BASE_URL:
        return {"video_url": None, "audio_url": None, "log_url": None}
    return {
        "video_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/video",
        "audio_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/audio",
        "log_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/log",
    }


def materialize_cookies(tmp_job_dir: Path, log_lines: List[str]) -> Optional[Path]:
    b64 = (os.getenv("YTDLP_COOKIES_B64") or "").strip()
    if not b64:
        log_lines.append("cookies=none")
        return None
    try:
        cookies_path = tmp_job_dir / "cookies.txt"
        raw = base64.b64decode(b64.encode("utf-8"))
        cookies_path.write_bytes(raw)
        os.chmod(cookies_path, 0o600)
        log_lines.append("cookies=materialized")
        return cookies_path
    except Exception as e:
        log_lines.append(f"cookies_error={e}")
        return None


def yt_dlp_supports(yt_dlp_bin: str, flag: str) -> bool:
    rc, out = run_cmd([yt_dlp_bin, "--help"], cwd=None)
    if rc != 0:
        return False
    return flag in out


def process_job(job_id: str, url: str) -> None:
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    out_template = "download.%(ext)s"
    out_log = tmp_job_dir / "log.txt"
    runner_log = tmp_job_dir / "runner.log"

    update_job(
        job_id,
        {
            "status": "running",
            "updated_at": now_iso(),
            "updated_at_ts": time.time(),
            "log_path": str(out_log.resolve()),
            "runner_log_path": str(runner_log.resolve()),
        },
    )

    log_lines: List[str] = []
    try:
        yt_dlp_bin = require_bin("yt-dlp")
        ffmpeg_bin = require_bin("ffmpeg")

        log_lines.append("== ENV ==")
        log_lines.append(f"yt-dlp={yt_dlp_bin}")
        log_lines.append(f"ffmpeg={ffmpeg_bin}")
        log_lines.append(f"tmp_job_dir={tmp_job_dir}")

        cookies_file = materialize_cookies(tmp_job_dir, log_lines)
        cookies_args: List[str] = ["--cookies", str(cookies_file)] if cookies_file else []

        dl_cmd: List[str] = [
            yt_dlp_bin,
            "--no-playlist",
            "--newline",
            "--retries", "5",
            "--fragment-retries", "5",
            "--socket-timeout", "30",
            "--concurrent-fragments", "2",
            "--sleep-interval", "1",
            "--max-sleep-interval", "3",

            # IMPORTANT: use only web client when using cookies
            "--extractor-args", "youtube:player_client=web",
        ]

        # Enable JS runtime if supported
        if yt_dlp_supports(yt_dlp_bin, "--js-runtimes"):
            dl_cmd += ["--js-runtimes", "node"]
            log_lines.append("js_runtime=enabled(--js-runtimes node)")

        # IMPORTANT: enable EJS remote components so challenges can be solved
        if yt_dlp_supports(yt_dlp_bin, "--remote-components"):
            dl_cmd += ["--remote-components", "ejs:github"]
            log_lines.append("remote_components=enabled(ejs:github)")

        # More resilient format selection:
        # Prefer mp4, else bestvideo+bestaudio, else best.
        dl_cmd += [
            "-S", "ext:mp4:m4a,codec:h264",
            "-f", "bv*+ba/best",
            "--merge-output-format", "mp4",
            "-o", out_template,
            *cookies_args,
            str(url),
        ]

        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)

        if rc != 0:
            tail = "\n".join(out.splitlines()[-160:])
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError(f"download failed (rc={rc})\n{tail}")

        mp4s = sorted(tmp_job_dir.glob("download*.mp4"), key=lambda p: p.stat().st_size, reverse=True)
        if not mp4s:
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError("download produced no mp4")

        merged_video = mp4s[0]
        if not wait_for_file(merged_video, min_bytes=1024 * 200):
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError("mp4 too small or not ready")

        out_audio = tmp_job_dir / "audio.wav"
        ff_cmd = [ffmpeg_bin, "-y", "-i", str(merged_video), "-ac", "1", "-ar", "16000", str(out_audio)]
        rc2, out2 = run_cmd(ff_cmd, cwd=None)
        log_lines.append("== ffmpeg ==")
        log_lines.append(out2)

        if rc2 != 0:
            tail = "\n".join(out2.splitlines()[-160:])
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError(f"audio extraction failed (rc={rc2})\n{tail}")

        if not wait_for_file(out_audio, min_bytes=1024 * 10):
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            raise RuntimeError("audio wav not ready")

        log_lines.append("== DONE ==")
        log_lines.append(f"video={merged_video.resolve()}")
        log_lines.append(f"audio={out_audio.resolve()}")

        out_log.write_text("\n".join(log_lines), encoding="utf-8")

        urls = _artifact_urls(job_id)
        update_job(
            job_id,
            {
                "status": "done",
                "error": None,
                "video_path": str(merged_video.resolve()),
                "audio_path": str(out_audio.resolve()),
                "log_path": str(out_log.resolve()),
                **urls,
                "updated_at": now_iso(),
                "updated_at_ts": time.time(),
            },
        )

    except Exception as e:
        try:
            out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8")
        except Exception:
            pass

        urls = _artifact_urls(job_id)
        update_job(
            job_id,
            {
                "status": "error",
                "error": str(e),
                "video_path": None,
                "audio_path": None,
                "log_path": str(out_log.resolve()),
                **urls,
                "updated_at": now_iso(),
                "updated_at_ts": time.time(),
            },
        )


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/debug/binaries")
def debug_binaries():
    return {
        "python": sys.version,
        "yt_dlp": shutil.which("yt-dlp"),
        "ffmpeg": shutil.which("ffmpeg"),
        "node": shutil.which("node"),
        "DATA_DIR": str(DATA_DIR.resolve()),
        "TMP_DIR": str(TMP_DIR.resolve()),
        "PUBLIC_BASE_URL": PUBLIC_BASE_URL or None,
        "has_cookies_b64": bool((os.getenv("YTDLP_COOKIES_B64") or "").strip()),
    }


@app.post("/jobs")
def create_job(body: CreateJobBody):
    job_id = str(uuid.uuid4())
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    out_log = tmp_job_dir / "log.txt"
    runner_log = tmp_job_dir / "runner.log"

    payload: Dict[str, Any] = {
        "id": job_id,
        "url": str(body.url),
        "status": "queued",
        "error": None,
        "video_path": None,
        "audio_path": None,
        "log_path": str(out_log.resolve()),
        "runner_log_path": str(runner_log.resolve()),
        **_artifact_urls(job_id),
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "updated_at_ts": time.time(),
    }
    save_job(job_id, payload)

    env = os.environ.copy()
    env["CLIPLINGUA_JOB_ID"] = job_id
    env["CLIPLINGUA_JOB_URL"] = str(body.url)

    runner = (
        "import os;"
        "from app.main import process_job;"
        "process_job(os.environ['CLIPLINGUA_JOB_ID'], os.environ['CLIPLINGUA_JOB_URL'])"
    )

    log_f = open(runner_log, "a", encoding="utf-8")

    subprocess.Popen(
        [sys.executable, "-u", "-c", runner],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=log_f,
        stderr=log_f,
        env=env,
        start_new_session=True,
    )

    return {"jobId": job_id}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    return load_job(job_id)


@app.get("/jobs/{job_id}/log", response_class=PlainTextResponse)
def get_job_log(job_id: str):
    job = load_job(job_id)
    lp = job.get("log_path")
    if lp and Path(lp).exists():
        return Path(lp).read_text(encoding="utf-8", errors="ignore")

    rlp = job.get("runner_log_path")
    if rlp and Path(rlp).exists():
        return Path(rlp).read_text(encoding="utf-8", errors="ignore")

    raise HTTPException(status_code=404, detail="log not ready")


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
