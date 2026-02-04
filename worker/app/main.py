"""
ClipLingua Worker (Render) - durable jobs + durable dubs

Key upgrades vs your current file:
- Dub status + dub logs are persisted in Supabase (survive Render restarts)
- Dub audio/video artifacts are persisted in Supabase Storage (survive restarts)
- /dubs/* endpoints read Supabase first, then local filesystem as fallback
- Background runner import path is robust via CLIPLINGUA_MAIN_MODULE env var

Required Supabase SQL (run once):
  alter table public.clip_jobs
  add column if not exists dub_status jsonb not null default '{}'::jsonb,
  add column if not exists dub_log_text jsonb not null default '{}'::jsonb;

You already added:
  storage_video_key, storage_audio_key, storage_log_key, log_text
"""

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
from functools import lru_cache
from datetime import datetime, timezone
import importlib

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, Response
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/cliplingua/data"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp/cliplingua/tmp"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
ARTIFACT_BUCKET = (os.getenv("ARTIFACT_BUCKET", "artifacts") or "artifacts").strip()

# The python module path where this file lives.
# Example: worker.app.main (if file is worker/app/main.py)
MAIN_MODULE = (os.getenv("CLIPLINGUA_MAIN_MODULE") or "worker.app.main").strip()

# If this file is worker/app/main.py, repo root is parents[2].
PROJECT_ROOT = Path(__file__).resolve().parents[2]

SUPPORTED_DUB_LANGS = {"hi", "en", "es"}

app = FastAPI(title="ClipLingua Worker", version="0.6.5")


def ensure_writable_dir(p: Path, fallback: Path) -> Path:
    try:
        p.mkdir(parents=True, exist_ok=True)
        test = p / ".write_test"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        return p
    except Exception:
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


DATA_DIR = ensure_writable_dir(DATA_DIR, Path("/tmp/cliplingua/data"))
TMP_DIR = ensure_writable_dir(TMP_DIR, Path("/tmp/cliplingua/tmp"))
JOB_STORE_DIR = ensure_writable_dir(DATA_DIR / "jobs", DATA_DIR / "jobs")

# -----------------------------------------------------------------------------
# Supabase client
# -----------------------------------------------------------------------------

_supabase = None
if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
    try:
        from supabase import create_client

        _supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    except Exception:
        _supabase = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    # Sometimes libraries may return JSON strings
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


# -----------------------------------------------------------------------------
# Storage helpers
# -----------------------------------------------------------------------------

def storage_key(job_id: str, filename: str) -> str:
    # filename can be nested like "dubs/hi/video.mp4"
    return f"jobs/{job_id}/{filename}"


def sb_upload_file(job_id: str, local_path: Path, filename: str, content_type: str) -> Optional[str]:
    if not _supabase:
        return None

    key = storage_key(job_id, filename)
    try:
        if not local_path.exists() or local_path.stat().st_size == 0:
            raise RuntimeError(f"missing/empty file: {local_path}")

        with open(local_path, "rb") as f:
            _supabase.storage.from_(ARTIFACT_BUCKET).upload(
                path=key,
                file=f,
                file_options={
                    "cache-control": "3600",
                    "content-type": content_type,
                    "upsert": True,
                },
            )
        return key
    except Exception as e:
        try:
            sb_append_log(job_id, f"\nSTORAGE_UPLOAD_ERROR ({filename}): {e}\n")
        except Exception:
            pass
        return None


def sb_download_key(key: str, dest_path: Path) -> bool:
    if not _supabase or not key:
        return False
    try:
        data = _supabase.storage.from_(ARTIFACT_BUCKET).download(key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(data)
        return dest_path.exists() and dest_path.stat().st_size > 0
    except Exception:
        return False


def sb_download_file(job_id: str, filename: str, dest_path: Path) -> bool:
    return sb_download_key(storage_key(job_id, filename), dest_path)


def ensure_local_from_storage(job_id: str, job: Dict[str, Any], filename: str, local_path: Path, key_field: str) -> bool:
    try:
        if local_path.exists() and local_path.stat().st_size > 0:
            return True
    except Exception:
        pass

    key = job.get(key_field)
    if key:
        ok = sb_download_key(key, local_path)
        if ok:
            return True

    return sb_download_file(job_id, filename, local_path)


def dub_storage_keys_from_status(dub_status: Dict[str, Any], lang: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    info = _as_dict(dub_status.get(lang))
    return (
        info.get("audio_key"),
        info.get("video_key"),
        info.get("log_key"),
    )


def ensure_local_dub_from_storage(
    job_id: str,
    lang: str,
    dub_status: Dict[str, Any],
    kind: str,  # "audio" | "video" | "log"
    local_path: Path,
) -> bool:
    """
    Ensure dub artifact exists locally. Checks:
    1) local
    2) keys stored in dub_status json
    3) computed fallback key jobs/<job_id>/dubs/<lang>/<file>
    """
    try:
        if local_path.exists() and local_path.stat().st_size > 0:
            return True
    except Exception:
        pass

    audio_key, video_key, log_key = dub_storage_keys_from_status(dub_status, lang)

    key = None
    if kind == "audio":
        key = audio_key
        fallback_filename = f"dubs/{lang}/audio.wav"
    elif kind == "video":
        key = video_key
        fallback_filename = f"dubs/{lang}/video.mp4"
    else:
        key = log_key
        fallback_filename = f"dubs/{lang}/log.txt"

    if key and sb_download_key(key, local_path):
        return True

    return sb_download_file(job_id, fallback_filename, local_path)


# -----------------------------------------------------------------------------
# Job store (local json + Supabase table)
# -----------------------------------------------------------------------------

def job_dir(job_id: str) -> Path:
    return JOB_STORE_DIR / job_id


def job_json_path(job_id: str) -> Path:
    return job_dir(job_id) / "job.json"


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def save_job_local(job_id: str, payload: Dict[str, Any]) -> None:
    atomic_write_text(job_json_path(job_id), json.dumps(payload, indent=2))


def load_job_local(job_id: str) -> Optional[Dict[str, Any]]:
    p = job_json_path(job_id)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))

    legacy = DATA_DIR / f"{job_id}.json"
    if legacy.exists():
        job = json.loads(legacy.read_text(encoding="utf-8"))
        jd = job_dir(job_id)
        jd.mkdir(parents=True, exist_ok=True)
        job.setdefault("id", job_id)
        job.update(_artifact_urls(job_id))
        save_job_local(job_id, job)
        return job

    return None


def _artifact_urls(job_id: str) -> Dict[str, Optional[str]]:
    if not PUBLIC_BASE_URL:
        return {"video_url": None, "audio_url": None, "log_url": None}
    return {
        "video_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/video",
        "audio_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/audio",
        "log_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/log",
    }


def sb_upsert_job(job_id: str, payload: Dict[str, Any]) -> None:
    if not _supabase:
        return
    try:
        row = {
            "id": job_id,
            "url": payload.get("url"),
            "status": payload.get("status", "queued"),
            "error": payload.get("error"),
            "updated_at": now_iso(),
            "video_url": payload.get("video_url"),
            "audio_url": payload.get("audio_url"),
            "log_url": payload.get("log_url"),
            "storage_video_key": payload.get("storage_video_key"),
            "storage_audio_key": payload.get("storage_audio_key"),
            "storage_log_key": payload.get("storage_log_key"),
            # Durable dubs
            "dub_status": payload.get("dub_status"),
            "dub_log_text": payload.get("dub_log_text"),
        }
        # Remove None keys so upsert does not overwrite jsonb columns unintentionally
        row = {k: v for k, v in row.items() if v is not None}
        _supabase.table("clip_jobs").upsert(row).execute()
    except Exception:
        pass


def sb_get_job(job_id: str) -> Optional[Dict[str, Any]]:
    if not _supabase:
        return None
    try:
        res = _supabase.table("clip_jobs").select("*").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
        return None
    except Exception:
        return None


def sb_append_log(job_id: str, text: str) -> None:
    if not _supabase:
        return
    try:
        res = _supabase.table("clip_jobs").select("log_text").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None) or []
        current = ""
        if data and isinstance(data, list) and len(data) > 0:
            current = data[0].get("log_text") or ""
        updated = current + ("" if current.endswith("\n") or current == "" else "\n") + text
        _supabase.table("clip_jobs").update({"log_text": updated, "updated_at": now_iso()}).eq("id", job_id).execute()
    except Exception:
        pass


def sb_get_log(job_id: str) -> Optional[str]:
    if not _supabase:
        return None
    try:
        res = _supabase.table("clip_jobs").select("log_text").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None)
        if data and isinstance(data, list) and len(data) > 0:
            return data[0].get("log_text") or ""
        return None
    except Exception:
        return None


def sb_get_dub_status_map(job_id: str) -> Dict[str, Any]:
    if not _supabase:
        return {}
    try:
        res = _supabase.table("clip_jobs").select("dub_status").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None) or []
        if data and isinstance(data, list) and len(data) > 0:
            return _as_dict(data[0].get("dub_status"))
        return {}
    except Exception:
        return {}


def sb_upsert_dub_status(
    job_id: str,
    lang: str,
    status: str,
    error: Optional[str] = None,
    audio_key: Optional[str] = None,
    video_key: Optional[str] = None,
    log_key: Optional[str] = None,
) -> None:
    if not _supabase:
        return
    try:
        current = sb_get_dub_status_map(job_id)
        prev = _as_dict(current.get(lang))
        prev.update(
            {
                "status": status,
                "error": error,
                "updated_at": now_iso(),
            }
        )
        if audio_key:
            prev["audio_key"] = audio_key
        if video_key:
            prev["video_key"] = video_key
        if log_key:
            prev["log_key"] = log_key

        current[lang] = prev
        _supabase.table("clip_jobs").update({"dub_status": current, "updated_at": now_iso()}).eq("id", job_id).execute()
    except Exception:
        pass


def sb_get_dub_log_text(job_id: str, lang: str) -> Optional[str]:
    if not _supabase:
        return None
    try:
        res = _supabase.table("clip_jobs").select("dub_log_text").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None) or []
        if not data or not isinstance(data, list):
            return None
        cur = _as_dict(data[0].get("dub_log_text"))
        return (cur.get(lang) or "").strip("\n")
    except Exception:
        return None


def sb_append_dub_log(job_id: str, lang: str, line: str) -> None:
    if not _supabase:
        return
    try:
        res = _supabase.table("clip_jobs").select("dub_log_text").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None) or []
        current = {}
        if data and isinstance(data, list) and len(data) > 0:
            current = _as_dict(data[0].get("dub_log_text"))

        prev = current.get(lang) or ""
        updated = prev + ("" if prev.endswith("\n") or prev == "" else "\n") + line.rstrip()
        current[lang] = updated

        _supabase.table("clip_jobs").update({"dub_log_text": current, "updated_at": now_iso()}).eq("id", job_id).execute()
    except Exception:
        pass


def load_job(job_id: str) -> Dict[str, Any]:
    sb = sb_get_job(job_id)
    if sb:
        return {
            "id": str(sb.get("id")),
            "url": sb.get("url"),
            "status": sb.get("status"),
            "error": sb.get("error"),
            "video_url": sb.get("video_url"),
            "audio_url": sb.get("audio_url"),
            "log_url": sb.get("log_url"),
            "created_at": sb.get("created_at"),
            "updated_at": sb.get("updated_at"),
            "storage_video_key": sb.get("storage_video_key"),
            "storage_audio_key": sb.get("storage_audio_key"),
            "storage_log_key": sb.get("storage_log_key"),
            "dub_status": sb.get("dub_status") or {},
            "dub_log_text": sb.get("dub_log_text") or {},
        }

    local = load_job_local(job_id)
    if local:
        return local

    raise HTTPException(status_code=404, detail="job not found")


def update_job(job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    local = load_job_local(job_id)

    if not local:
        sb = sb_get_job(job_id)
        if sb:
            local = {
                "id": str(sb.get("id")),
                "url": sb.get("url"),
                "status": sb.get("status"),
                "error": sb.get("error"),
                "video_url": sb.get("video_url"),
                "audio_url": sb.get("audio_url"),
                "log_url": sb.get("log_url"),
                "created_at": sb.get("created_at"),
                "updated_at": sb.get("updated_at"),
                "storage_video_key": sb.get("storage_video_key"),
                "storage_audio_key": sb.get("storage_audio_key"),
                "storage_log_key": sb.get("storage_log_key"),
                "dub_status": sb.get("dub_status") or {},
                "dub_log_text": sb.get("dub_log_text") or {},
            }
        else:
            local = {"id": job_id}

    local.update(patch)
    save_job_local(job_id, local)
    sb_upsert_job(job_id, local)
    return local


# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_models():
    models: Dict[str, Any] = {}

    from faster_whisper import WhisperModel

    models["whisper"] = WhisperModel("small", device="cpu", compute_type="int8")

    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

    nllb_model = "facebook/nllb-200-distilled-600M"
    models["nllb_tokenizer"] = AutoTokenizer.from_pretrained(nllb_model)
    models["nllb"] = AutoModelForSeq2SeqLM.from_pretrained(nllb_model)

    from TTS.api import TTS

    models["tts"] = TTS("tts_models/multilingual/multi-dataset/xtts_v2")

    return models


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

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


def yt_dlp_supports(yt_dlp_bin: str, flag: str) -> bool:
    rc, out = run_cmd([yt_dlp_bin, "--help"], cwd=None)
    return rc == 0 and (flag in out)


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


def _job_artifact_paths(job_id: str) -> Dict[str, Path]:
    jd = job_dir(job_id)
    return {
        "job_dir": jd,
        "video": jd / "video.mp4",
        "audio": jd / "audio.wav",
        "log": jd / "log.txt",
        "runner_log": jd / "runner.log",
    }


# -----------------------------------------------------------------------------
# Dubs: local paths
# -----------------------------------------------------------------------------

def dub_dir(job_id: str, lang: str) -> Path:
    return job_dir(job_id) / "dubs" / lang


def dub_status_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "status.json"


def dub_audio_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "audio.wav"


def dub_video_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "video.mp4"


def dub_log_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "log.txt"


def dub_runner_log_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "runner.log"


def write_dub_status(job_id: str, lang: str, status: str, error: Optional[str] = None) -> None:
    # local
    p = dub_status_path(job_id, lang)
    p.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(
        p,
        json.dumps(
            {
                "job_id": job_id,
                "lang": lang,
                "status": status,
                "error": error,
                "updated_at": now_iso(),
            },
            indent=2,
        ),
    )
    # supabase
    sb_upsert_dub_status(job_id, lang, status, error=error)


# -----------------------------------------------------------------------------
# Base job processing (download + extract audio)
# -----------------------------------------------------------------------------

def process_job(job_id: str, url: str) -> None:
    tmp_job_dir = TMP_DIR / job_id
    tmp_job_dir.mkdir(parents=True, exist_ok=True)

    paths = _job_artifact_paths(job_id)
    paths["job_dir"].mkdir(parents=True, exist_ok=True)

    out_template = "download.%(ext)s"
    out_log = paths["log"]
    runner_log = paths["runner_log"]

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
        log_lines.append(f"DATA_DIR={DATA_DIR}")
        log_lines.append(f"TMP_DIR={TMP_DIR}")
        log_lines.append(f"job_dir={paths['job_dir']}")
        log_lines.append(f"tmp_job_dir={tmp_job_dir}")

        cookies_file = materialize_cookies(tmp_job_dir, log_lines)
        cookies_args: List[str] = ["--cookies", str(cookies_file)] if cookies_file else []

        dl_cmd: List[str] = [
            yt_dlp_bin,
            "--no-playlist",
            "--newline",
            "--retries",
            "5",
            "--fragment-retries",
            "5",
            "--socket-timeout",
            "30",
            "--concurrent-fragments",
            "2",
            "--sleep-interval",
            "1",
            "--max-sleep-interval",
            "3",
            "--extractor-args",
            "youtube:player_client=web",
        ]

        if yt_dlp_supports(yt_dlp_bin, "--js-runtimes"):
            dl_cmd += ["--js-runtimes", "node"]
            log_lines.append("js_runtime=enabled(--js-runtimes node)")

        if yt_dlp_supports(yt_dlp_bin, "--remote-components"):
            dl_cmd += ["--remote-components", "ejs:github"]
            log_lines.append("remote_components=enabled(ejs:github)")

        dl_cmd += [
            "-S",
            "ext:mp4:m4a,codec:h264",
            "-f",
            "bv*+ba/best",
            "--merge-output-format",
            "mp4",
            "-o",
            out_template,
            *cookies_args,
            str(url),
        ]

        rc, out = run_cmd(dl_cmd, cwd=tmp_job_dir)
        log_lines.append("== yt-dlp ==")
        log_lines.append(out)
        sb_append_log(job_id, "\n".join(log_lines[-80:]) + "\n")

        if rc != 0:
            tail = "\n".join(out.splitlines()[-200:])
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            sb_append_log(job_id, "\nERROR: download failed\n" + tail + "\n")
            raise RuntimeError(f"download failed (rc={rc})\n{tail}")

        mp4s = sorted(tmp_job_dir.glob("download*.mp4"), key=lambda p: p.stat().st_size, reverse=True)
        if not mp4s:
            log_lines.append("== tmp dir listing ==")
            log_lines.append(list_dir(tmp_job_dir))
            sb_append_log(job_id, "\nERROR: no mp4 produced\n")
            raise RuntimeError("download produced no mp4")

        merged_video = mp4s[0]
        if not wait_for_file(merged_video, min_bytes=1024 * 200):
            sb_append_log(job_id, "\nERROR: mp4 too small/not ready\n")
            raise RuntimeError("mp4 too small or not ready")

        tmp_audio = tmp_job_dir / "audio.wav"
        ff_cmd = [ffmpeg_bin, "-y", "-i", str(merged_video), "-ac", "1", "-ar", "16000", str(tmp_audio)]
        rc2, out2 = run_cmd(ff_cmd, cwd=None)
        log_lines.append("== ffmpeg ==")
        log_lines.append(out2)
        sb_append_log(job_id, "\n".join(log_lines[-80:]) + "\n")

        if rc2 != 0:
            tail = "\n".join(out2.splitlines()[-200:])
            sb_append_log(job_id, "\nERROR: audio extraction failed\n" + tail + "\n")
            raise RuntimeError(f"audio extraction failed (rc={rc2})\n{tail}")

        if not wait_for_file(tmp_audio, min_bytes=1024 * 10):
            sb_append_log(job_id, "\nERROR: wav not ready\n")
            raise RuntimeError("audio wav not ready")

        paths["video"].parent.mkdir(parents=True, exist_ok=True)
        merged_video.replace(paths["video"])
        tmp_audio.replace(paths["audio"])

        log_lines.append("== DONE ==")
        log_lines.append(f"video={paths['video'].resolve()}")
        log_lines.append(f"audio={paths['audio'].resolve()}")

        out_log.write_text("\n".join(log_lines), encoding="utf-8", errors="ignore")
        sb_append_log(job_id, "\n== DONE ==\n")

        urls = _artifact_urls(job_id)

        # Persist to Storage
        video_key = sb_upload_file(job_id, paths["video"], "video.mp4", "video/mp4")
        audio_key = sb_upload_file(job_id, paths["audio"], "audio.wav", "audio/wav")
        log_key = sb_upload_file(job_id, paths["log"], "log.txt", "text/plain")

        update_job(job_id, {"storage_video_key": video_key, "storage_audio_key": audio_key, "storage_log_key": log_key})

        update_job(
            job_id,
            {
                "status": "done",
                "error": None,
                "video_path": str(paths["video"].resolve()),
                "audio_path": str(paths["audio"].resolve()),
                "log_path": str(out_log.resolve()),
                **urls,
                "updated_at": now_iso(),
                "updated_at_ts": time.time(),
            },
        )

    except Exception as e:
        try:
            out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8", errors="ignore")
        except Exception:
            pass

        sb_append_log(job_id, f"\nERROR: {e}\n")

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
    finally:
        try:
            shutil.rmtree(tmp_job_dir, ignore_errors=True)
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Dub processing (ASR -> Translate -> TTS -> Mux)
# -----------------------------------------------------------------------------

def whisper_transcribe(audio_path: Path) -> Dict[str, Any]:
    m = get_models()
    whisper = m["whisper"]

    segments, info = whisper.transcribe(str(audio_path), beam_size=3)
    seg_list = []
    full = []
    for s in segments:
        seg_list.append({"start": s.start, "end": s.end, "text": s.text})
        full.append(s.text.strip())
    return {
        "language": getattr(info, "language", None),
        "text": " ".join([t for t in full if t]),
        "segments": seg_list,
    }


def nllb_translate(text: str, target_lang: str) -> str:
    if not text.strip():
        return ""

    tgt_map = {"en": "eng_Latn", "hi": "hin_Deva", "es": "spa_Latn"}
    tgt = tgt_map[target_lang]

    m = get_models()
    tok = m["nllb_tokenizer"]
    model = m["nllb"]

    inputs = tok(text, return_tensors="pt", truncation=True, max_length=1024)
    forced_bos = tok.convert_tokens_to_ids(tgt)
    out = model.generate(**inputs, forced_bos_token_id=forced_bos, max_new_tokens=512)
    return tok.batch_decode(out, skip_special_tokens=True)[0]


def xtts_speak(text: str, lang: str, out_wav: Path) -> None:
    out_wav.parent.mkdir(parents=True, exist_ok=True)
    m = get_models()
    tts = m["tts"]
    tts.tts_to_file(text=text, file_path=str(out_wav), language=lang)


def mux_audio_into_video(ffmpeg_bin: str, video_in: Path, audio_in: Path, video_out: Path) -> None:
    video_out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        ffmpeg_bin,
        "-y",
        "-i",
        str(video_in),
        "-i",
        str(audio_in),
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-shortest",
        str(video_out),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    if rc != 0:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"ffmpeg mux failed (rc={rc})\n{tail}")


def load_job_for_artifacts(job_id: str) -> Dict[str, Any]:
    local = load_job_local(job_id)
    if local and local.get("status"):
        return local

    sb = sb_get_job(job_id)
    if sb:
        job = {
            "id": str(sb.get("id")),
            "url": sb.get("url"),
            "status": sb.get("status"),
            "error": sb.get("error"),
            "video_url": sb.get("video_url"),
            "audio_url": sb.get("audio_url"),
            "log_url": sb.get("log_url"),
            "created_at": sb.get("created_at"),
            "updated_at": sb.get("updated_at"),
            "storage_video_key": sb.get("storage_video_key"),
            "storage_audio_key": sb.get("storage_audio_key"),
            "storage_log_key": sb.get("storage_log_key"),
            "dub_status": sb.get("dub_status") or {},
            "dub_log_text": sb.get("dub_log_text") or {},
        }
        if local:
            job.update(local)
        save_job_local(job_id, job)
        return job

    if local:
        return local
    return load_job(job_id)


def ensure_base_artifacts_local(job_id: str, job: Dict[str, Any]) -> Tuple[Path, Path]:
    paths = _job_artifact_paths(job_id)
    paths["job_dir"].mkdir(parents=True, exist_ok=True)

    video_p = Path(job["video_path"]) if job.get("video_path") else paths["video"]
    audio_p = Path(job["audio_path"]) if job.get("audio_path") else paths["audio"]

    if (not video_p.exists()) or video_p.stat().st_size < 10_000:
        key = job.get("storage_video_key") or storage_key(job_id, "video.mp4")
        if not sb_download_key(key, video_p):
            raise RuntimeError("base video missing locally and not found in storage")

    if (not audio_p.exists()) or audio_p.stat().st_size < 2_000:
        key = job.get("storage_audio_key") or storage_key(job_id, "audio.wav")
        if not sb_download_key(key, audio_p):
            raise RuntimeError("base audio missing locally and not found in storage")

    patch = {}
    if not job.get("video_path"):
        patch["video_path"] = str(video_p.resolve())
    if not job.get("audio_path"):
        patch["audio_path"] = str(audio_p.resolve())
    if patch:
        patch["updated_at"] = now_iso()
        patch["updated_at_ts"] = time.time()
        update_job(job_id, patch)

    return video_p, audio_p


def process_dub(job_id: str, lang: str) -> None:
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        sb_upsert_dub_status(job_id, lang, "error", error="unsupported lang")
        return

    dd = dub_dir(job_id, lang)
    dd.mkdir(parents=True, exist_ok=True)
    local_log_path = dub_log_path(job_id, lang)

    def log(line: str) -> None:
        line = line.rstrip()
        with open(local_log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        sb_append_dub_log(job_id, lang, line)

    try:
        write_dub_status(job_id, lang, "running")
        log("== DUB START ==")

        job = load_job_for_artifacts(job_id)
        if job.get("status") != "done":
            raise RuntimeError(f"base job not done (status={job.get('status')})")

        video_in, audio_in = ensure_base_artifacts_local(job_id, job)

        out_audio = dub_audio_path(job_id, lang)
        out_video = dub_video_path(job_id, lang)

        # If already produced on this instance, mark done and ensure Storage keys exist later (optional)
        if out_audio.exists() and out_video.exists() and out_audio.stat().st_size > 2048 and out_video.stat().st_size > 10_000:
            log("cached=true (local files already exist)")
        else:
            ffmpeg_bin = require_bin("ffmpeg")

            log("loading models (first time can be slow)")
            tr = whisper_transcribe(audio_in)
            src_text = tr.get("text", "")
            log(f"transcribed_chars={len(src_text)}")

            if lang == "en":
                translated = src_text
            else:
                translated = nllb_translate(src_text, lang)

            if not translated.strip():
                raise RuntimeError("translation empty")

            log(f"translated_chars={len(translated)}")

            xtts_speak(translated, lang, out_audio)
            if not out_audio.exists() or out_audio.stat().st_size < 2048:
                raise RuntimeError("dub audio not generated")

            mux_audio_into_video(ffmpeg_bin, video_in, out_audio, out_video)
            if not out_video.exists() or out_video.stat().st_size < 10_000:
                raise RuntimeError("dub video not generated")

            log("dub_files=generated")

        # Upload dub artifacts to Storage (durable)
        audio_key = sb_upload_file(job_id, out_audio, f"dubs/{lang}/audio.wav", "audio/wav")
        video_key = sb_upload_file(job_id, out_video, f"dubs/{lang}/video.mp4", "video/mp4")
        log_key = sb_upload_file(job_id, local_log_path, f"dubs/{lang}/log.txt", "text/plain")

        log("== DUB DONE ==")
        sb_upsert_dub_status(job_id, lang, "done", error=None, audio_key=audio_key, video_key=video_key, log_key=log_key)

        # keep local status.json in sync too
        write_dub_status(job_id, lang, "done")

    except Exception as e:
        log(f"ERROR: {e}")
        sb_upsert_dub_status(job_id, lang, "error", error=str(e))
        write_dub_status(job_id, lang, "error", str(e))


# -----------------------------------------------------------------------------
# API Schemas
# -----------------------------------------------------------------------------

class CreateJobBody(BaseModel):
    url: HttpUrl


class DubBody(BaseModel):
    lang: str  # hi | en | es


# -----------------------------------------------------------------------------
# API Routes
# -----------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/", response_class=PlainTextResponse)
def root():
    return "ClipLingua Worker OK"


@app.head("/")
def head_root():
    return Response(status_code=200)


@app.get("/debug/binaries")
def debug_binaries():
    return {
        "python": sys.version,
        "yt_dlp": shutil.which("yt-dlp"),
        "ffmpeg": shutil.which("ffmpeg"),
        "node": shutil.which("node"),
        "DATA_DIR": str(DATA_DIR.resolve()),
        "TMP_DIR": str(TMP_DIR.resolve()),
        "JOB_STORE_DIR": str(JOB_STORE_DIR.resolve()),
        "PUBLIC_BASE_URL": PUBLIC_BASE_URL or None,
        "has_cookies_b64": bool((os.getenv("YTDLP_COOKIES_B64") or "").strip()),
        "supabase_enabled": bool(_supabase),
        "MAIN_MODULE": MAIN_MODULE,
        "PROJECT_ROOT": str(PROJECT_ROOT),
    }


@app.post("/jobs")
def create_job(body: CreateJobBody):
    job_id = str(uuid.uuid4())

    paths = _job_artifact_paths(job_id)
    paths["job_dir"].mkdir(parents=True, exist_ok=True)

    payload: Dict[str, Any] = {
        "id": job_id,
        "url": str(body.url),
        "status": "queued",
        "error": None,
        "video_path": None,
        "audio_path": None,
        "log_path": str(paths["log"].resolve()),
        "runner_log_path": str(paths["runner_log"].resolve()),
        **_artifact_urls(job_id),
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "updated_at_ts": time.time(),
        "data_dir": str(paths["job_dir"].resolve()),
    }
    save_job_local(job_id, payload)
    sb_upsert_job(job_id, payload)

    env = os.environ.copy()
    env["CLIPLINGUA_JOB_ID"] = job_id
    env["CLIPLINGUA_JOB_URL"] = str(body.url)
    env["CLIPLINGUA_MAIN_MODULE"] = MAIN_MODULE

    runner = (
        "import os, importlib;"
        "m = importlib.import_module(os.environ.get('CLIPLINGUA_MAIN_MODULE','worker.app.main'));"
        "m.process_job(os.environ['CLIPLINGUA_JOB_ID'], os.environ['CLIPLINGUA_JOB_URL'])"
    )

    log_f = open(paths["runner_log"], "a", encoding="utf-8")

    subprocess.Popen(
        [sys.executable, "-u", "-c", runner],
        cwd=str(PROJECT_ROOT),
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
    paths = _job_artifact_paths(job_id)
    lp = Path(job.get("log_path") or paths["log"])

    sb = sb_get_log(job_id)
    if sb:
        return sb

    if lp.exists():
        return lp.read_text(encoding="utf-8", errors="ignore")

    ok = ensure_local_from_storage(job_id, job, "log.txt", lp, "storage_log_key")
    if ok:
        return lp.read_text(encoding="utf-8", errors="ignore")

    rlp = paths["runner_log"]
    if rlp.exists():
        return rlp.read_text(encoding="utf-8", errors="ignore")

    raise HTTPException(status_code=404, detail="log not ready")


@app.get("/jobs/{job_id}/audio")
def get_job_audio(job_id: str):
    job = load_job(job_id)
    paths = _job_artifact_paths(job_id)

    ap = job.get("audio_path")
    local_p = Path(ap) if ap else paths["audio"]

    ok = ensure_local_from_storage(job_id, job, "audio.wav", local_p, "storage_audio_key")
    if not ok:
        raise HTTPException(status_code=404, detail="audio not ready")

    if not job.get("audio_path"):
        update_job(job_id, {"audio_path": str(local_p.resolve()), "updated_at": now_iso(), "updated_at_ts": time.time()})

    return FileResponse(path=str(local_p), media_type="audio/wav", filename=f"{job_id}.wav")


@app.get("/jobs/{job_id}/video")
def get_job_video(job_id: str):
    job = load_job(job_id)
    paths = _job_artifact_paths(job_id)

    vp = job.get("video_path")
    local_p = Path(vp) if vp else paths["video"]

    ok = ensure_local_from_storage(job_id, job, "video.mp4", local_p, "storage_video_key")
    if not ok:
        raise HTTPException(status_code=404, detail="video not ready")

    if not job.get("video_path"):
        update_job(job_id, {"video_path": str(local_p.resolve()), "updated_at": now_iso(), "updated_at_ts": time.time()})

    return FileResponse(path=str(local_p), media_type="video/mp4", filename=f"{job_id}.mp4")


@app.post("/jobs/{job_id}/dub")
def dub_job(job_id: str, body: DubBody):
    lang = (body.lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang (use hi/en/es)")

    _ = load_job_for_artifacts(job_id)

    write_dub_status(job_id, lang, "queued")

    env = os.environ.copy()
    env["CLIPLINGUA_DUB_JOB_ID"] = job_id
    env["CLIPLINGUA_DUB_LANG"] = lang
    env["CLIPLINGUA_MAIN_MODULE"] = MAIN_MODULE

    runner = (
        "import os, importlib;"
        "m = importlib.import_module(os.environ.get('CLIPLINGUA_MAIN_MODULE','worker.app.main'));"
        "m.process_dub(os.environ['CLIPLINGUA_DUB_JOB_ID'], os.environ['CLIPLINGUA_DUB_LANG'])"
    )

    dd = dub_dir(job_id, lang)
    dd.mkdir(parents=True, exist_ok=True)

    runner_log_f = open(dub_runner_log_path(job_id, lang), "a", encoding="utf-8")

    subprocess.Popen(
        [sys.executable, "-u", "-c", runner],
        cwd=str(PROJECT_ROOT),
        stdout=runner_log_f,
        stderr=runner_log_f,
        env=env,
        start_new_session=True,
    )

    return {
        "ok": True,
        "lang": lang,
        "status_url": f"/jobs/{job_id}/dubs/{lang}/status",
        "audio_url": f"/jobs/{job_id}/dubs/{lang}/audio",
        "video_url": f"/jobs/{job_id}/dubs/{lang}/video",
        "log_url": f"/jobs/{job_id}/dubs/{lang}/log",
        "runner_url": f"/jobs/{job_id}/dubs/{lang}/runner",
    }


@app.get("/jobs/{job_id}/dubs/{lang}/status")
def get_dub_status(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    # Supabase first
    dub_status = sb_get_dub_status_map(job_id)
    info = _as_dict(dub_status.get(lang))
    if info.get("status"):
        return {"job_id": job_id, "lang": lang, **info}

    # Local fallback
    p = dub_status_path(job_id, lang)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))

    return {"job_id": job_id, "lang": lang, "status": "not_started"}


@app.get("/jobs/{job_id}/dubs/{lang}/log", response_class=PlainTextResponse)
def get_dub_log(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    # Supabase first
    sb_txt = sb_get_dub_log_text(job_id, lang)
    if sb_txt:
        return sb_txt

    # Local
    lp = dub_log_path(job_id, lang)
    if lp.exists():
        return lp.read_text(encoding="utf-8", errors="ignore")

    # Storage
    dub_status = sb_get_dub_status_map(job_id)
    ok = ensure_local_dub_from_storage(job_id, lang, dub_status, "log", lp)
    if ok and lp.exists():
        return lp.read_text(encoding="utf-8", errors="ignore")

    # Runner local fallback
    rp = dub_runner_log_path(job_id, lang)
    if rp.exists():
        return rp.read_text(encoding="utf-8", errors="ignore")

    raise HTTPException(status_code=404, detail="log not ready")


@app.get("/jobs/{job_id}/dubs/{lang}/audio")
def get_dub_audio(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    p = dub_audio_path(job_id, lang)

    if not p.exists() or p.stat().st_size < 2048:
        dub_status = sb_get_dub_status_map(job_id)
        if not ensure_local_dub_from_storage(job_id, lang, dub_status, "audio", p):
            raise HTTPException(status_code=404, detail="dub audio not ready")

    return FileResponse(path=str(p), media_type="audio/wav", filename=f"{job_id}_{lang}.wav")


@app.get("/jobs/{job_id}/dubs/{lang}/video")
def get_dub_video(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    p = dub_video_path(job_id, lang)

    if not p.exists() or p.stat().st_size < 10_000:
        dub_status = sb_get_dub_status_map(job_id)
        if not ensure_local_dub_from_storage(job_id, lang, dub_status, "video", p):
            raise HTTPException(status_code=404, detail="dub video not ready")

    return FileResponse(path=str(p), media_type="video/mp4", filename=f"{job_id}_{lang}.mp4")


@app.get("/jobs/{job_id}/dubs/{lang}/runner", response_class=PlainTextResponse)
def get_dub_runner_log(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    p = dub_runner_log_path(job_id, lang)
    if not p.exists():
        raise HTTPException(status_code=404, detail="runner log not ready")
    return p.read_text(encoding="utf-8", errors="ignore")
