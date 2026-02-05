"""
ClipLingua Worker (Render) - durable jobs + durable dubs (free-tier safe)

Free-tier OOM root cause:
- Importing/initializing Transformers (NLLB) or Coqui TTS will often exceed Render free memory.
This version guarantees:
- DISABLE_XTTS=1 prevents any Coqui TTS import
- ENABLE_LOCAL_NLLB=0 prevents any Transformers import
- Whisper is loaded lazily and only once (tiny + int8)
- Translation defaults to google_free (no key)
- TTS fallback uses edge-tts or espeak (no heavy python deps)

Key features:
- Durable base artifacts (video/audio/log) -> Supabase Storage
- Durable dub artifacts (audio/video/log) -> Supabase Storage
- Dub status/logs persisted in Supabase jsonb columns (survive restarts)
- /dubs endpoints: Supabase first -> local -> storage fallback
- Stale running detection: marks dub as error if heartbeat is old

Required Supabase SQL (run once):
  alter table public.clip_jobs
    add column if not exists dub_status jsonb not null default '{}'::jsonb,
    add column if not exists dub_log_text jsonb not null default '{}'::jsonb;

Recommended env for free tier:
  WHISPER_MODEL=tiny
  ENABLE_LOCAL_NLLB=0
  TRANSLATE_PROVIDER=google_free
  DISABLE_XTTS=1
  DUB_STALE_SECONDS=600
  TTS_PROVIDER=auto
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
import urllib.parse
import urllib.request
import threading

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, Response
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

TRANSLATE_PROVIDER = (os.getenv("TRANSLATE_PROVIDER") or "google_free").strip().lower()
ENABLE_LOCAL_NLLB = (os.getenv("ENABLE_LOCAL_NLLB") or "").strip().lower() in {"1", "true", "yes"}
MAX_TRANSCRIPT_CHARS = int(os.getenv("MAX_TRANSCRIPT_CHARS", "4500"))

DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/cliplingua/data"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp/cliplingua/tmp"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
ARTIFACT_BUCKET = (os.getenv("ARTIFACT_BUCKET", "artifacts") or "artifacts").strip()

SUPPORTED_DUB_LANGS = {"hi", "en", "es"}

WHISPER_MODEL = (os.getenv("WHISPER_MODEL") or "tiny").strip()
NLLB_MODEL = (os.getenv("NLLB_MODEL") or "facebook/nllb-200-distilled-300M").strip()
DISABLE_XTTS = (os.getenv("DISABLE_XTTS") or "").strip().lower() in {"1", "true", "yes"}

DUB_STALE_SECONDS = int(os.getenv("DUB_STALE_SECONDS", "600"))

# Optional: tag so you can confirm redeploy
BUILD_TAG = (os.getenv("BUILD_TAG") or "").strip() or None

app = FastAPI(title="ClipLingua Worker", version="0.7.3")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _as_dict(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

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
    except Exception as e:
        print(f"WARNING: Supabase client creation failed: {e}")
        _supabase = None

# -----------------------------------------------------------------------------
# Storage helpers
# -----------------------------------------------------------------------------

def storage_key(job_id: str, filename: str) -> str:
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
                    "upsert": "true",
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
    if key and sb_download_key(key, local_path):
        return True

    return sb_download_file(job_id, filename, local_path)

def dub_storage_keys_from_status(dub_status: Dict[str, Any], lang: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    info = _as_dict(dub_status.get(lang))
    return (info.get("audio_key"), info.get("video_key"), info.get("log_key"))

def ensure_local_dub_from_storage(
    job_id: str,
    lang: str,
    dub_status: Dict[str, Any],
    kind: str,
    local_path: Path,
) -> bool:
    try:
        if local_path.exists() and local_path.stat().st_size > 0:
            return True
    except Exception:
        pass

    audio_key, video_key, log_key = dub_storage_keys_from_status(dub_status, lang)

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

def _artifact_urls(job_id: str) -> Dict[str, Optional[str]]:
    if not PUBLIC_BASE_URL:
        return {"video_url": None, "audio_url": None, "log_url": None}
    return {
        "video_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/video",
        "audio_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/audio",
        "log_url": f"{PUBLIC_BASE_URL}/jobs/{job_id}/log",
    }

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
            "dub_status": payload.get("dub_status"),
            "dub_log_text": payload.get("dub_log_text"),
        }
        row = {k: v for k, v in row.items() if v is not None}
        _supabase.table("clip_jobs").upsert(row).execute()
    except Exception as e:
        print(f"WARNING: sb_upsert_job failed: {e}")

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
    except Exception as e:
        print(f"WARNING: sb_append_log failed: {e}")

def sb_get_log(job_id: str) -> Optional[str]:
    if not _supabase:
        return None
    try:
        res = _supabase.table("clip_jobs").select("log_text").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None)
        if data and isinstance(data, list) and len(data) > 0:
            txt = data[0].get("log_text")
            if not txt or not str(txt).strip():
                return None
            return txt
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
        prev.update({"status": status, "error": error, "updated_at": now_iso()})
        if audio_key:
            prev["audio_key"] = audio_key
        if video_key:
            prev["video_key"] = video_key
        if log_key:
            prev["log_key"] = log_key
        current[lang] = prev
        _supabase.table("clip_jobs").update({"dub_status": current, "updated_at": now_iso()}).eq("id", job_id).execute()
    except Exception as e:
        print(f"WARNING: sb_upsert_dub_status failed: {e}")

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
    except Exception as e:
        print(f"WARNING: sb_append_dub_log failed: {e}")

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
# Models (lazy + free-tier safe)
# -----------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_whisper():
    from faster_whisper import WhisperModel
    return WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")

def _get_nllb():
    """Only called when ENABLE_LOCAL_NLLB=1."""
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    tok = AutoTokenizer.from_pretrained(NLLB_MODEL)
    model = AutoModelForSeq2SeqLM.from_pretrained(NLLB_MODEL)
    return tok, model

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
        log_lines.append(f"BUILD_TAG={BUILD_TAG}")
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

        out_log.write_text("\n".join(log_lines), encoding="utf-8", errors="ignore")
        sb_append_log(job_id, "\n== DONE ==\n")

        urls = _artifact_urls(job_id)

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
    whisper = _get_whisper()
    segments, info = whisper.transcribe(str(audio_path), beam_size=2)
    seg_list = []
    full = []
    for s in segments:
        seg_list.append({"start": s.start, "end": s.end, "text": s.text})
        txt = (s.text or "").strip()
        if txt:
            full.append(txt)
    return {
        "language": getattr(info, "language", None),
        "text": " ".join(full),
        "segments": seg_list,
    }

def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    head = s[:n]
    return head.rsplit(" ", 1)[0] if " " in head else head

def translate_text(text: str, target_lang: str) -> str:
    text = _truncate(text, MAX_TRANSCRIPT_CHARS)
    if not text:
        return ""
    if target_lang == "en":
        return text

    if ENABLE_LOCAL_NLLB:
        tok, model = _get_nllb()
        tgt_map = {"en": "eng_Latn", "hi": "hin_Deva", "es": "spa_Latn"}
        tgt = tgt_map.get(target_lang)
        if not tgt:
            return text
        inputs = tok(text, return_tensors="pt", truncation=True, max_length=1024)
        forced_bos = tok.convert_tokens_to_ids(tgt)
        out = model.generate(**inputs, forced_bos_token_id=forced_bos, max_new_tokens=512)
        return tok.batch_decode(out, skip_special_tokens=True)[0]

    if TRANSLATE_PROVIDER == "libretranslate":
        base = (os.getenv("LIBRETRANSLATE_URL") or "").strip().rstrip("/")
        if not base:
            raise RuntimeError("LIBRETRANSLATE_URL not set")
        api_key = (os.getenv("LIBRETRANSLATE_API_KEY") or "").strip()

        payload = {"q": text, "source": "auto", "target": target_lang, "format": "text"}
        if api_key:
            payload["api_key"] = api_key

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=f"{base}/translate",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        out = json.loads(body)
        return (out.get("translatedText") or "").strip()

    # Default: google_free
    q = urllib.parse.quote(text)
    url = (
        "https://translate.googleapis.com/translate_a/single"
        f"?client=gtx&sl=auto&tl={urllib.parse.quote(target_lang)}&dt=t&q={q}"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    arr = json.loads(raw)
    translated = "".join([chunk[0] for chunk in arr[0] if chunk and chunk[0]])
    return translated.strip()

import asyncio

def _edge_voice_for(lang: str) -> str:
    return {
        "hi": os.getenv("EDGE_TTS_VOICE_HI", "hi-IN-SwaraNeural"),
        "en": os.getenv("EDGE_TTS_VOICE_EN", "en-IN-NeerjaNeural"),
        "es": os.getenv("EDGE_TTS_VOICE_ES", "es-ES-ElviraNeural"),
    }.get(lang, "en-IN-NeerjaNeural")

async def _edge_tts_async(text: str, lang: str, out_wav: Path) -> None:
    import edge_tts

    out_wav.parent.mkdir(parents=True, exist_ok=True)

    voice = _edge_voice_for(lang)
    rate = os.getenv("EDGE_TTS_RATE", "+0%")
    volume = os.getenv("EDGE_TTS_VOLUME", "+0%")

    tmp_mp3 = out_wav.with_suffix(".mp3")
    communicate = edge_tts.Communicate(text=text, voice=voice, rate=rate, volume=volume)
    await communicate.save(str(tmp_mp3))

    if not tmp_mp3.exists() or tmp_mp3.stat().st_size < 1024:
        raise RuntimeError("edge-tts produced empty audio")

    ffmpeg_bin = require_bin("ffmpeg")
    cmd = [
        ffmpeg_bin, "-y",
        "-i", str(tmp_mp3),
        "-ac", "1",
        "-ar", "16000",
        str(out_wav),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    tmp_mp3.unlink(missing_ok=True)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"ffmpeg convert failed (rc={rc})\n{tail}")

def _espeak_voice_for(lang: str) -> str:
    return {"hi": "hi", "en": "en-us", "es": "es"}.get(lang, "en-us")

def tts_espeak(text: str, lang: str, out_wav: Path) -> None:
    voice = _espeak_voice_for(lang)
    tmp = out_wav.with_suffix(".espeak.wav")

    cmd = ["espeak-ng", "-v", voice, "-w", str(tmp), "--stdin"]
    proc = subprocess.run(cmd, input=text, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        raise RuntimeError(f"espeak-ng failed (rc={proc.returncode})\n{proc.stdout[-2000:]}")

    ffmpeg_bin = require_bin("ffmpeg")
    rc, out = run_cmd([ffmpeg_bin, "-y", "-i", str(tmp), "-ac", "1", "-ar", "16000", str(out_wav)], cwd=None)
    tmp.unlink(missing_ok=True)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        raise RuntimeError(f"ffmpeg convert failed (rc={rc})\n{out.splitlines()[-200:]}")

def tts_speak(text: str, lang: str, out_wav: Path) -> None:
    text = (text or "").strip()
    if not text:
        raise RuntimeError("TTS text empty")

    provider = (os.getenv("TTS_PROVIDER") or "auto").strip().lower()

    def try_edge():
        asyncio.run(_edge_tts_async(text, lang, out_wav))

    def try_espeak():
        tts_espeak(text, lang, out_wav)

    attempts = []
    if provider == "edge":
        attempts = [("edge", try_edge)]
    elif provider == "espeak":
        attempts = [("espeak", try_espeak)]
    else:
        # auto: try edge (nice voice) then espeak (always works)
        attempts = [("edge", try_edge), ("espeak", try_espeak)]

    last = None
    for name, fn in attempts:
        try:
            fn()
            return
        except Exception as e:
            last = f"{name}: {e}"

    raise RuntimeError(f"TTS failed: {last}")

def mux_audio_into_video(ffmpeg_bin: str, video_in: Path, audio_in: Path, video_out: Path, log_fn=None) -> None:
    video_out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        ffmpeg_bin,
        "-y",
        "-i", str(video_in),
        "-stream_loop", "-1",
        "-i", str(audio_in),
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "128k",
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-shortest",
        str(video_out),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    if log_fn:
        log_fn("== ffmpeg mux ==")
        log_fn(out)
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

    def heartbeat() -> None:
        sb_upsert_dub_status(job_id, lang, "running", error=None)

    try:
        write_dub_status(job_id, lang, "running")
        log("== DUB START ==")
        heartbeat()

        log(f"models: whisper={WHISPER_MODEL} nllb={NLLB_MODEL} xtts_disabled={DISABLE_XTTS} translate={TRANSLATE_PROVIDER} local_nllb={ENABLE_LOCAL_NLLB}")

        job = load_job_for_artifacts(job_id)
        if job.get("status") != "done":
            raise RuntimeError(f"base job not done (status={job.get('status')})")

        video_in, audio_in = ensure_base_artifacts_local(job_id, job)

        out_audio = dub_audio_path(job_id, lang)
        out_video = dub_video_path(job_id, lang)

        if out_audio.exists() and out_video.exists() and out_audio.stat().st_size > 2048 and out_video.stat().st_size > 10_000:
            log("cached=true (local dub files exist)")
        else:
            ffmpeg_bin = require_bin("ffmpeg")

            log("starting transcription...")
            tr = whisper_transcribe(audio_in)
            src_text = tr.get("text", "")
            log(f"transcribed_chars={len(src_text)}")
            heartbeat()

            if not src_text.strip():
                raise RuntimeError("transcription empty")

            log(f"translating to {lang}...")
            translated = src_text if lang == "en" else translate_text(src_text, lang)
            log(f"translated_chars={len(translated)}")
            heartbeat()

            if not translated.strip():
                raise RuntimeError("translation empty")

            log(f"generating TTS audio...")
            tts_speak(translated, lang, out_audio)
            if not out_audio.exists() or out_audio.stat().st_size < 2048:
                raise RuntimeError("dub audio not generated")

            log("muxing audio into video...")
            mux_audio_into_video(ffmpeg_bin, video_in, out_audio, out_video, log_fn=log)

            if not out_video.exists() or out_video.stat().st_size < 10_000:
                raise RuntimeError("dub video not generated")

            log("dub_files=generated")

        log("uploading to storage...")
        audio_key = sb_upload_file(job_id, out_audio, f"dubs/{lang}/audio.wav", "audio/wav")
        video_key = sb_upload_file(job_id, out_video, f"dubs/{lang}/video.mp4", "video/mp4")
        log_key = sb_upload_file(job_id, local_log_path, f"dubs/{lang}/log.txt", "text/plain")
        log("uploaded_to_storage=true")

        sb_upsert_dub_status(job_id, lang, "done", error=None, audio_key=audio_key, video_key=video_key, log_key=log_key)
        write_dub_status(job_id, lang, "done")
        log("== DUB DONE ==")

    except Exception as e:
        error_msg = f"ERROR: {e}"
        try:
            with open(local_log_path, "a", encoding="utf-8") as f:
                f.write(error_msg + "\n")
        except Exception:
            pass
        sb_upsert_dub_status(job_id, lang, "error", error=str(e))
        write_dub_status(job_id, lang, "error", str(e))
        print(error_msg)

# -----------------------------------------------------------------------------
# API Schemas
# -----------------------------------------------------------------------------

class CreateJobBody(BaseModel):
    url: HttpUrl

class DubBody(BaseModel):
    lang: str

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
        "espeak_ng": shutil.which("espeak-ng"),
        "espeak": shutil.which("espeak"),
        "DATA_DIR": str(DATA_DIR.resolve()),
        "TMP_DIR": str(TMP_DIR.resolve()),
        "JOB_STORE_DIR": str(JOB_STORE_DIR.resolve()),
        "PUBLIC_BASE_URL": PUBLIC_BASE_URL or None,
        "has_cookies_b64": bool((os.getenv("YTDLP_COOKIES_B64") or "").strip()),
        "supabase_enabled": bool(_supabase),
        "WHISPER_MODEL": WHISPER_MODEL,
        "NLLB_MODEL": NLLB_MODEL,
        "DISABLE_XTTS": DISABLE_XTTS,
        "TRANSLATE_PROVIDER": TRANSLATE_PROVIDER,
        "ENABLE_LOCAL_NLLB": ENABLE_LOCAL_NLLB,
        "MAX_TRANSCRIPT_CHARS": MAX_TRANSCRIPT_CHARS,
        "DUB_STALE_SECONDS": DUB_STALE_SECONDS,
        "BUILD_TAG": BUILD_TAG,
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
        "dub_status": {},
        "dub_log_text": {},
    }
    save_job_local(job_id, payload)
    sb_upsert_job(job_id, payload)
    sb_append_log(job_id, f"spawned job runner at {now_iso()} url={body.url}\n")

    threading.Thread(
        target=process_job,
        args=(job_id, str(body.url)),
        daemon=True,
    ).start()

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
    if sb is not None:
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

    threading.Thread(
        target=process_dub,
        args=(job_id, lang),
        daemon=True,
    ).start()

    return {
        "ok": True,
        "lang": lang,
        "status_url": f"/jobs/{job_id}/dubs/{lang}/status",
        "audio_url": f"/jobs/{job_id}/dubs/{lang}/audio",
        "video_url": f"/jobs/{job_id}/dubs/{lang}/video",
        "log_url": f"/jobs/{job_id}/dubs/{lang}/log",
    }

def _parse_iso(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

@app.get("/jobs/{job_id}/dubs/{lang}/status")
def get_dub_status(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    if _supabase:
        ds = sb_get_dub_status_map(job_id)
        if lang in ds:
            st = _as_dict(ds[lang])
            if st.get("status") == "running":
                t = _parse_iso(st.get("updated_at") or "")
                if t:
                    age = (datetime.now(timezone.utc) - t).total_seconds()
                    if age > DUB_STALE_SECONDS:
                        sb_upsert_dub_status(job_id, lang, "error", error="stale running: worker restarted or OOM")
                        return {
                            "job_id": job_id,
                            "lang": lang,
                            "status": "error",
                            "error": "stale running: worker restarted or OOM",
                            "updated_at": now_iso(),
                        }
            return {"job_id": job_id, "lang": lang, **st}

    p = dub_status_path(job_id, lang)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return {"job_id": job_id, "lang": lang, "status": "not_started"}

@app.get("/jobs/{job_id}/dubs/{lang}/log", response_class=PlainTextResponse)
def get_dub_log(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")

    sb_txt = sb_get_dub_log_text(job_id, lang)
    if sb_txt:
        return sb_txt

    lp = dub_log_path(job_id, lang)
    if lp.exists():
        return lp.read_text(encoding="utf-8", errors="ignore")

    dub_status = sb_get_dub_status_map(job_id)
    ok = ensure_local_dub_from_storage(job_id, lang, dub_status, "log", lp)
    if ok and lp.exists():
        return lp.read_text(encoding="utf-8", errors="ignore")

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