# app/main.py
"""
ClipLingua Worker - TIMED DUB VERSION (Free-tier safe)

Goal: make dubbing sound less robotic by fixing the biggest offender:
timing. We synthesize TTS per Whisper segment, then pad/fit each segment
to its original time window, and finally stitch everything back together.

Free-tier safe defaults:
- DISABLE_XTTS=1 (no Coqui)
- ENABLE_LOCAL_NLLB=0 (no local transformers)
- TTS uses edge-tts (with espeak-ng fallback)
- Translation uses google_free (or LibreTranslate if configured)

Main quality upgrades vs the earlier "improved" version:
1) Segment-timed dubbing (align speech to original pacing)
2) Avoid slowing speech down (we pad silence instead). Speed up only when needed.
3) Better audio stitching (ffmpeg concat filter, no fragile "copy" concat)
4) Better muxing: no infinite audio loop. We generate audio to match video length.
5) Small fades on each segment to reduce clicks
"""

import os
import sys
import json
import uuid
import time
import base64
import shutil
import subprocess
import re
import wave
import math
import numpy as np

from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from functools import lru_cache
from datetime import datetime, timezone
import urllib.parse
import urllib.request
import threading

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, Response
from pydantic import BaseModel, HttpUrl, Field
from dotenv import load_dotenv

load_dotenv()

JOB_SEM = threading.Semaphore(1)
DUB_SEM = threading.Semaphore(1)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

MAX_LOG_CHARS = int(os.getenv("MAX_LOG_CHARS", "20000"))

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

# Quality toggles
ENABLE_AUDIO_NORMALIZATION = (os.getenv("ENABLE_AUDIO_NORMALIZATION") or "1").strip().lower() in {"1", "true", "yes"}
ENABLE_SENTENCE_SPLITTING = (os.getenv("ENABLE_SENTENCE_SPLITTING") or "1").strip().lower() in {"1", "true", "yes"}

# New: timed dubbing controls
ENABLE_TIMED_DUB = (os.getenv("ENABLE_TIMED_DUB") or "1").strip().lower() in {"1", "true", "yes"}
MAX_DUB_SEGMENTS = int(os.getenv("MAX_DUB_SEGMENTS", "140"))  # safety cap
MERGE_GAP_SECONDS = float(os.getenv("MERGE_GAP_SECONDS", "0.18"))
MIN_SEG_CHARS = int(os.getenv("MIN_SEG_CHARS", "10"))
MAX_SEG_CHARS = int(os.getenv("MAX_SEG_CHARS", "220"))
SEGMENT_FIT_TOLERANCE = float(os.getenv("SEGMENT_FIT_TOLERANCE", "1.06"))  # 6% over target is "fit enough"

# Edge TTS tuning (no custom SSML; edge-tts removed full SSML support)
EDGE_TTS_VOLUME = (os.getenv("EDGE_TTS_VOLUME") or "+0%").strip()
EDGE_TTS_PITCH = (os.getenv("EDGE_TTS_PITCH") or "+0Hz").strip()

BUILD_TAG = (os.getenv("BUILD_TAG") or "").strip() or None

app = FastAPI(title="ClipLingua Worker", version="0.9.0-timed-dub")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
CAPTION_STYLE_FORCE = {"clean", "bold", "boxed", "big"}

def srt_ts(sec: float) -> str:
    sec = max(0.0, float(sec))
    whole = math.floor(sec)
    ms = int(round((sec - whole) * 1000.0))
    h = int(whole // 3600)
    m = int((whole % 3600) // 60)
    s = int(whole % 60)

    if ms >= 1000:
        ms -= 1000
        s += 1
        if s >= 60:
            s = 0
            m += 1
            if m >= 60:
                m = 0
                h += 1

    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def write_srt(entries: List[Dict[str, Any]], out_path: Path) -> None:
    lines: List[str] = []
    for i, e in enumerate(entries, start=1):
        lines.append(str(i))
        lines.append(f"{srt_ts(e['start'])} --> {srt_ts(e['end'])}")
        lines.append((e.get("text") or "").strip())
        lines.append("")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8", errors="ignore")

# --------------------------------------------------------------------------
# Captions (robust fonts + autosize)
# --------------------------------------------------------------------------

def probe_video_size(video_in: Path) -> Tuple[int, int]:
    """Return (w,h) or (1280,720) fallback."""
    try:
        ffprobe = require_bin("ffprobe")
        rc, out = run_cmd(
            [
                ffprobe, "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "csv=p=0:s=x",
                str(video_in),
            ],
            cwd=None,
        )
        if rc != 0:
            return (1280, 720)
        val = out.strip().splitlines()[-1].strip()
        if "x" in val:
            w, h = val.split("x", 1)
            return (int(w), int(h))
        return (1280, 720)
    except Exception:
        return (1280, 720)

def _font_exists(font_name: str) -> bool:
    """Check if a font exists using fc-list when available."""
    try:
        fc = shutil.which("fc-list")
        if not fc:
            return False
        rc, out = run_cmd([fc, ":family"], cwd=None)
        if rc != 0:
            return False
        fams = out.lower()
        return font_name.lower() in fams
    except Exception:
        return False

CAPTION_FONT_PREFS = {
    # Put Devanagari-capable fonts first for Hindi
    "hi": [
        os.getenv("CAPTION_FONT_HI", "").strip(),
        "Noto Sans Devanagari",
        "Lohit Devanagari",
        "Mangal",
        "Nirmala UI",
        "DejaVu Sans",
    ],
    "en": [
        os.getenv("CAPTION_FONT_EN", "").strip(),
        "Noto Sans",
        "DejaVu Sans",
        "Arial",
    ],
    "es": [
        os.getenv("CAPTION_FONT_ES", "").strip(),
        "Noto Sans",
        "DejaVu Sans",
        "Arial",
    ],
}

def pick_caption_font(lang: str) -> Optional[str]:
    lang = (lang or "en").lower()
    prefs = CAPTION_FONT_PREFS.get(lang, CAPTION_FONT_PREFS["en"])
    prefs = [p for p in prefs if p]  # drop empty
    # If we can detect installed fonts, pick the first that exists.
    for f in prefs:
        if _font_exists(f):
            return f
    # If we cannot detect (fc-list missing), still return a strong default.
    # For Hindi, prefer Noto Sans Devanagari.
    if lang == "hi":
        return "Noto Sans Devanagari"
    return "Noto Sans"

def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))

def build_caption_force_style(style_id: str, video_h: int, lang: str) -> str:
    """
    Auto-size based on video height but clamp so 9:16 videos do not get giant captions.
    """
    style_id = (style_id or "clean").lower()
    font = pick_caption_font(lang)

    # scale based on 720p baseline, but clamp for tall videos (Shorts/Reels)
    scale = clamp(video_h / 720.0, 0.80, 1.25)

    base_size = int(round(24 * scale))     # 720p ~24
    big_size  = int(round(base_size * 1.20))
    outline   = int(round(2 * scale))
    outline   = max(1, min(outline, 3))
    margin_v  = int(round(56 * scale))

    # Common base: readable, not bulky
    base = [
        f"Fontname={font}",
        f"Fontsize={base_size}",
        "PrimaryColour=&H00FFFFFF",   # white
        "OutlineColour=&H00000000",   # black
        f"Outline={outline}",
        "Shadow=0",
        f"MarginV={margin_v}",
        "Alignment=2",                # bottom-center
        "WrapStyle=2",                # smart wrapping
    ]

    if style_id == "bold":
        base += ["Bold=1", f"Outline={min(3, outline + 1)}"]
    elif style_id == "boxed":
        base += [
            "BorderStyle=3",
            "BackColour=&H80000000",   # 50% black
            f"Outline={outline}",
        ]
    elif style_id == "big":
        base = [x if not x.startswith("Fontsize=") else f"Fontsize={big_size}" for x in base]
        base += ["Bold=1", f"Outline={min(3, outline + 1)}"]

    return ",".join(base)

def burn_captions(
    video_in: Path,
    srt_path: Path,
    video_out: Path,
    caption_style: str,
    lang: str,
    log_fn=None,
) -> None:
    ffmpeg = require_bin("ffmpeg")

    # fontsdir helps libass find fonts in slim containers
    fontsdir = (os.getenv("CAPTION_FONTS_DIR") or "/usr/share/fonts").strip()
    _, vh = probe_video_size(video_in)
    style = build_caption_force_style(caption_style, vh, lang)

    # Escape for ffmpeg subtitles filter
    p = str(srt_path).replace("\\", "/")
    p = p.replace(":", "\\:").replace("'", "\\'")
    fontsdir = fontsdir.replace("\\", "/").replace(":", "\\:").replace("'", "\\'")

    vf = f"subtitles='{p}':charenc=UTF-8:fontsdir='{fontsdir}':force_style='{style}'"

    cmd = [
        ffmpeg, "-y",
        "-i", str(video_in),
        "-vf", vf,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "copy",
        str(video_out),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    if log_fn:
        log_fn("== ffmpeg burn captions ==")
        log_fn(out)
        log_fn(f"caption_font={pick_caption_font(lang)} video_h={vh} style={caption_style}")
        log_fn(f"fontsdir={fontsdir}")
    if rc != 0 or (not video_out.exists()) or video_out.stat().st_size < 10_000:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"burn captions failed (rc={rc})\n{tail}")

def wav_duration_seconds(path: Path) -> float:
    with wave.open(str(path), "rb") as wf:
        frames = wf.getnframes()
        sr = wf.getframerate()
    return float(frames) / float(sr) if sr else 0.0

def safe_move(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))

def make_silence_wav(duration_s: float, out_wav: Path) -> None:
    duration_s = float(duration_s)
    if duration_s <= 0.01:
        raise RuntimeError("silence duration too small")

    ffmpeg_bin = require_bin("ffmpeg")
    cmd = [
        ffmpeg_bin, "-y",
        "-f", "lavfi",
        "-i", f"anullsrc=r=16000:cl=mono",
        "-t", f"{duration_s:.3f}",
        "-ar", "16000", "-ac", "1",
        str(out_wav),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        tail = "\n".join(out.splitlines()[-120:])
        raise RuntimeError(f"make_silence_wav failed (rc={rc})\n{tail}")

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
                file_options={"cache-control": "3600", "content-type": content_type, "upsert": "true"},
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

def ensure_local_dub_from_storage(job_id: str, lang: str, dub_status: Dict[str, Any], kind: str, local_path: Path) -> bool:
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
# Job store
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
        updated = (current + ("" if current.endswith("\n") or current == "" else "\n") + text)
        updated = updated[-MAX_LOG_CHARS:]

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
        updated = (prev + ("" if prev.endswith("\n") or prev == "" else "\n") + line.rstrip())
        updated = updated[-MAX_LOG_CHARS:]

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
# Models
# -----------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_whisper():
    from faster_whisper import WhisperModel
    return WhisperModel(WHISPER_MODEL, device="cpu", compute_type="int8")

def _get_nllb():
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    tok = AutoTokenizer.from_pretrained(NLLB_MODEL)
    model = AutoModelForSeq2SeqLM.from_pretrained(NLLB_MODEL)
    return tok, model

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def _read_wav_mono(path: Path) -> Tuple[np.ndarray, int]:
    with wave.open(str(path), "rb") as wf:
        ch = wf.getnchannels()
        sr = wf.getframerate()
        n = wf.getnframes()
        raw = wf.readframes(n)

    x = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
    if ch > 1:
        x = x.reshape(-1, ch).mean(axis=1)
    return x, sr

def estimate_median_f0(audio_wav: Path, sr_expected: int = 16000) -> Optional[float]:
    """
    Lightweight pitch estimate via autocorrelation.
    Works fine for voice and avoids heavy deps.
    Returns median F0 in Hz or None.
    """
    try:
        x, sr = _read_wav_mono(audio_wav)
        if sr != sr_expected:
            # your pipeline already outputs 16k mono, so normally not needed
            sr = sr_expected

        if x.size < sr * 1:
            return None

        frame_ms = 40
        hop_ms = 10
        frame = int(sr * frame_ms / 1000)
        hop = int(sr * hop_ms / 1000)

        f0s: List[float] = []

        min_hz = 70
        max_hz = 300

        min_lag = int(sr / max_hz)
        max_lag = int(sr / min_hz)

        for i in range(0, len(x) - frame, hop):
            w = x[i:i + frame]
            w = w - float(w.mean())
            energy = float(np.mean(w * w))
            if energy < 1e-5:
                continue

            # FFT autocorr
            n = len(w)
            fft = np.fft.rfft(w, n=2 * n)
            ac = np.fft.irfft(fft * np.conj(fft))[:n]
            if ac[0] <= 0:
                continue

            seg = ac[min_lag:max_lag]
            if seg.size <= 0:
                continue

            peak = float(np.max(seg))
            if peak / float(ac[0]) < 0.25:
                continue

            lag = int(min_lag + int(np.argmax(seg)))
            if lag <= 0:
                continue

            f0 = float(sr / lag)
            if min_hz <= f0 <= max_hz:
                f0s.append(f0)

        if not f0s:
            return None
        return float(np.median(np.array(f0s, dtype=np.float32)))
    except Exception:
        return None

def infer_gender_from_f0(f0_hz: Optional[float]) -> str:
    """
    Safer heuristic:
    - < 145 Hz: likely male
    - > 190 Hz: likely female
    - between: unknown (avoid wrong flips)
    """
    if not f0_hz:
        return "unknown"
    if f0_hz < 145.0:
        return "male"
    if f0_hz > 190.0:
        return "female"
    return "unknown"

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
    # Supports either a mounted cookies file path OR base64 content
    pth = (os.getenv("YTDLP_COOKIES_PATH") or "").strip()
    if pth:
        cp = Path(pth)
        if cp.exists() and cp.stat().st_size > 10:
            log_lines.append(f"cookies=path:{cp}")
            return cp
        log_lines.append("cookies_path_invalid")
    b64 = (os.getenv("YTDLP_COOKIES_B64") or "").strip()
    if not b64:
        log_lines.append("cookies=none")
        return None
    try:
        cookies_path = tmp_job_dir / "cookies.txt"
        raw = base64.b64decode(b64.encode("utf-8"))
        cookies_path.write_bytes(raw)
        os.chmod(cookies_path, 0o600)
        log_lines.append("cookies=materialized_b64")
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
            {"job_id": job_id, "lang": lang, "status": status, "error": error, "updated_at": now_iso()},
            indent=2,
        ),
    )
    sb_upsert_dub_status(job_id, lang, status, error=error)

# -----------------------------------------------------------------------------
# Base job processing
# -----------------------------------------------------------------------------

def process_job(job_id: str, url: str) -> None:
    with JOB_SEM:
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
            out = out[-20000:]
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
            out2 = out2[-20000:]
            log_lines.append("== ffmpeg extract ==")
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
            safe_move(merged_video, paths["video"])
            safe_move(tmp_audio, paths["audio"])

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
# Transcribe + translate
# -----------------------------------------------------------------------------

def whisper_transcribe(audio_path: Path) -> Dict[str, Any]:
    whisper = _get_whisper()
    segments, info = whisper.transcribe(str(audio_path), beam_size=2)
    seg_list = []
    full = []
    for s in segments:
        seg_list.append({"start": float(s.start), "end": float(s.end), "text": (s.text or "")})
        txt = (s.text or "").strip()
        if txt:
            full.append(txt)
    return {"language": getattr(info, "language", None), "text": " ".join(full), "segments": seg_list}

def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    head = s[:n]
    return head.rsplit(" ", 1)[0] if " " in head else head

def clean_text_for_translation(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\.{2,}", ".", text)
    text = re.sub(r"([.!?])\1+", r"\1", text)
    return text.strip()

def split_into_sentences(text: str) -> List[str]:
    sentences = re.split(r"([.!?]+\s+)", text)
    result = []
    for i in range(0, len(sentences) - 1, 2):
        sent = sentences[i] + (sentences[i + 1] if i + 1 < len(sentences) else "")
        sent = sent.strip()
        if sent:
            result.append(sent)
    if not result and text.strip():
        result = [text.strip()]
    return result

_translate_cache: Dict[str, str] = {}

def translate_text(text: str, target_lang: str) -> str:
    text = _truncate(text, MAX_TRANSCRIPT_CHARS)
    text = clean_text_for_translation(text)
    if not text:
        return ""
    if target_lang == "en":
        return text

    cache_key = f"{target_lang}::{text}"
    if cache_key in _translate_cache:
        return _translate_cache[cache_key]

    if ENABLE_LOCAL_NLLB:
        tok, model = _get_nllb()
        tgt_map = {"en": "eng_Latn", "hi": "hin_Deva", "es": "spa_Latn"}
        tgt = tgt_map.get(target_lang)
        if not tgt:
            _translate_cache[cache_key] = text
            return text
        inputs = tok(text, return_tensors="pt", truncation=True, max_length=1024)
        forced_bos = tok.convert_tokens_to_ids(tgt)
        out = model.generate(**inputs, forced_bos_token_id=forced_bos, max_new_tokens=512)
        translated = tok.batch_decode(out, skip_special_tokens=True)[0].strip()
        _translate_cache[cache_key] = translated
        return translated

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
            url=f"{base}/translate", data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
        out = json.loads(body)
        translated = (out.get("translatedText") or "").strip()
        _translate_cache[cache_key] = translated
        return translated

    # Default: google_free
    q = urllib.parse.quote(text)
    url = (
        "https://translate.googleapis.com/translate_a/single"
        f"?client=gtx&sl=auto&tl={urllib.parse.quote(target_lang)}&dt=t&q={q}"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    arr = json.loads(raw)
    translated = "".join([chunk[0] for chunk in arr[0] if chunk and chunk[0]]).strip()
    _translate_cache[cache_key] = translated
    return translated

# -----------------------------------------------------------------------------
# TTS
# -----------------------------------------------------------------------------

import asyncio

def _edge_voice_for(lang: str, gender: str = "unknown") -> str:
    """
    gender: male | female | unknown
    Falls back to EDGE_TTS_VOICE_* if per-gender env not provided.
    """
    lang = (lang or "en").lower()
    gender = (gender or "unknown").lower()

    # Per-gender env keys
    key_map = {
        ("hi", "male"): "EDGE_TTS_VOICE_HI_MALE",
        ("hi", "female"): "EDGE_TTS_VOICE_HI_FEMALE",
        ("en", "male"): "EDGE_TTS_VOICE_EN_MALE",
        ("en", "female"): "EDGE_TTS_VOICE_EN_FEMALE",
        ("es", "male"): "EDGE_TTS_VOICE_ES_MALE",
        ("es", "female"): "EDGE_TTS_VOICE_ES_FEMALE",
    }

    # Fallback env keys (your existing ones)
    fallback_env = {
        "hi": os.getenv("EDGE_TTS_VOICE_HI", "hi-IN-MadhurNeural"),
        "en": os.getenv("EDGE_TTS_VOICE_EN", "en-IN-PrabhatNeural"),
        "es": os.getenv("EDGE_TTS_VOICE_ES", "es-ES-ElviraNeural"),
    }

    # Default male/female if nothing set
    defaults = {
        ("hi", "male"): "hi-IN-MadhurNeural",
        ("hi", "female"): "hi-IN-SwaraNeural",
        ("en", "male"): "en-IN-PrabhatNeural",
        ("en", "female"): "en-IN-NeerjaNeural",
        ("es", "male"): "es-ES-AlvaroNeural",
        ("es", "female"): "es-ES-ElviraNeural",
    }

    env_key = key_map.get((lang, gender))
    if env_key:
        v = (os.getenv(env_key) or "").strip()
        if v:
            return v

    # If unknown gender, prefer your existing voice env
    if gender == "unknown":
        return fallback_env.get(lang, fallback_env["en"])

    return defaults.get((lang, gender), fallback_env.get(lang, fallback_env["en"]))

def _base_rate_for_lang(lang: str) -> str:
    # Keep this close to natural. Timing is handled mostly by segment fitting.
    rates = {
        "hi": os.getenv("EDGE_TTS_RATE_HI", os.getenv("EDGE_TTS_RATE", "-5%")),
        "en": os.getenv("EDGE_TTS_RATE_EN", os.getenv("EDGE_TTS_RATE", "+0%")),
        "es": os.getenv("EDGE_TTS_RATE_ES", os.getenv("EDGE_TTS_RATE", "+0%")),
    }
    return rates.get(lang, os.getenv("EDGE_TTS_RATE", "+0%"))

def _rate_candidates(base_rate: str) -> List[str]:
    # If we must fit into a short segment, we try faster rates.
    # edge-tts expects strings like "+10%" or "-5%"
    # We keep it conservative to avoid "chipmunk" speech.
    return [
        base_rate,
        "+10%",
        "+20%",
        "+30%",
        "+40%",
    ]

def _safe_text_for_tts(text: str) -> str:
    text = clean_text_for_translation(text)
    # Light cleanup to prevent awkward pauses
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"([,.;:!?])([A-Za-z])", r"\1 \2", text)
    return text.strip()

async def _edge_tts_to_mp3_async(
    text: str, lang: str, out_mp3: Path, rate: str, volume: str, pitch: str, gender: str = "unknown"
) -> None:
    import edge_tts
    out_mp3.parent.mkdir(parents=True, exist_ok=True)
    voice = _edge_voice_for(lang, gender)
    communicate = edge_tts.Communicate(text=text, voice=voice, rate=rate, volume=volume, pitch=pitch)
    await communicate.save(str(out_mp3))

    if not out_mp3.exists() or out_mp3.stat().st_size < 1024:
        raise RuntimeError("edge-tts produced empty audio")

def tts_edge(text: str, lang: str, out_wav: Path, rate: str, log_fn=None, gender: str = "unknown") -> None:
    text = _safe_text_for_tts(text)
    if not text:
        raise RuntimeError("TTS text empty")

    tmp_mp3 = out_wav.with_suffix(".mp3")
    asyncio.run(_edge_tts_to_mp3_async(text, lang, tmp_mp3, rate=rate, volume=EDGE_TTS_VOLUME, pitch=EDGE_TTS_PITCH, gender=gender))

    ffmpeg_bin = require_bin("ffmpeg")
    cmd = [ffmpeg_bin, "-y", "-i", str(tmp_mp3), "-ac", "1", "-ar", "16000", str(out_wav)]
    rc, out = run_cmd(cmd, cwd=None)
    tmp_mp3.unlink(missing_ok=True)
    if log_fn:
        log_fn(out)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"ffmpeg convert failed (rc={rc})\n{tail}")

def _espeak_voice_for(lang: str) -> str:
    return {"hi": "hi", "en": "en-us", "es": "es"}.get(lang, "en-us")

def tts_espeak(text: str, lang: str, out_wav: Path) -> None:
    text = _safe_text_for_tts(text)
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

def tts_speak(text: str, lang: str, out_wav: Path, log_fn=None, gender: str = "unknown") -> None:
    """
    Non-timed fallback: sentence splitting + stitch.
    Timed dubbing uses a different path (segment-based).
    """
    text = (text or "").strip()
    if not text:
        raise RuntimeError("TTS text empty")

    provider = (os.getenv("TTS_PROVIDER") or "auto").strip().lower()

    if ENABLE_SENTENCE_SPLITTING and len(text) > 200:
        sentences = split_into_sentences(text)
        if log_fn:
            log_fn(f"split_sentences={len(sentences)}")

        temp_wavs: List[Path] = []
        for i, sentence in enumerate(sentences):
            if not sentence.strip():
                continue
            tmp_wav = out_wav.with_suffix(f".sent{i}.wav")

            def try_edge():
                tts_edge(sentence, lang, tmp_wav, rate=_base_rate_for_lang(lang), log_fn=None, gender=gender)

            def try_espeak():
                tts_espeak(sentence, lang, tmp_wav)

            attempts: List[Tuple[str, Any]] = []
            if provider == "edge":
                attempts = [("edge", try_edge)]
            elif provider == "espeak":
                attempts = [("espeak", try_espeak)]
            else:
                attempts = [("edge", try_edge), ("espeak", try_espeak)]

            success = False
            for name, fn in attempts:
                try:
                    fn()
                    temp_wavs.append(tmp_wav)
                    success = True
                    break
                except Exception as e:
                    if log_fn:
                        log_fn(f"sentence_tts_failed idx={i} provider={name} err={e}")

            if not success:
                raise RuntimeError(f"Failed to generate TTS for sentence {i+1}")

        ffmpeg_concat_wavs(temp_wavs, out_wav, log_fn=log_fn)

        for w in temp_wavs:
            w.unlink(missing_ok=True)
        return

    # Single shot
    last = None
    if provider in {"auto", "edge"}:
        try:
            tts_edge(text, lang, out_wav, rate=_base_rate_for_lang(lang), log_fn=None, gender=gender)
            return
        except Exception as e:
            last = f"edge: {e}"
            if log_fn:
                log_fn(f"edge_failed={e}")
    if provider in {"auto", "espeak"}:
        try:
            tts_espeak(text, lang, out_wav)
            return
        except Exception as e:
            last = f"espeak: {e}"
            if log_fn:
                log_fn(f"espeak_failed={e}")
    raise RuntimeError(f"TTS failed: {last}")

# -----------------------------------------------------------------------------
# Audio + video timing helpers
# -----------------------------------------------------------------------------

def video_duration_seconds(p: Path) -> Optional[float]:
    try:
        ffprobe = require_bin("ffprobe")
        rc, out = run_cmd(
            [
                ffprobe,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=nw=1:nk=1",
                str(p),
            ],
            cwd=None,
        )
        if rc != 0:
            return None
        val = out.strip().splitlines()[-1].strip()
        return float(val)
    except Exception:
        return None

def atempo_chain(factor: float) -> str:
    # ffmpeg atempo supports 0.5..2.0 per filter. We chain when needed.
    if factor <= 0:
        return "atempo=1.0"
    parts: List[float] = []
    f = factor
    while f > 2.0:
        parts.append(2.0)
        f /= 2.0
    while f < 0.5:
        parts.append(0.5)
        f /= 0.5
    parts.append(f)
    return ",".join([f"atempo={p:.4f}" for p in parts])

def stretch_or_pad_to_duration(in_wav: Path, out_wav: Path, target_sec: float, log_fn=None) -> None:
    """
    Keep speech natural:
    - If audio is shorter: do NOT slow it down. Just pad silence.
    - If audio is longer: speed up to fit (atempo), then trim to target.
    Always produce exactly target_sec duration.
    """
    ffmpeg = require_bin("ffmpeg")
    d_in = max(0.001, wav_duration_seconds(in_wav))
    target = max(0.05, float(target_sec))

    # Speed up only if needed
    if d_in > target:
        tempo = d_in / target
        tempo_f = atempo_chain(tempo)
        af = f"{tempo_f},apad"
    else:
        af = "apad"

    # Add tiny fades to reduce clicks
    fade_d = min(0.03, target / 4.0)
    # if fade is too tiny, skip
    if fade_d > 0.005:
        st_out = max(0.0, target - fade_d)
        af = f"{af},afade=t=in:st=0:d={fade_d:.4f},afade=t=out:st={st_out:.4f}:d={fade_d:.4f}"

    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(in_wav),
        "-af",
        af,
        "-t",
        f"{target:.4f}",
        "-ac",
        "1",
        "-ar",
        "16000",
        str(out_wav),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    if log_fn:
        log_fn(out)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"segment fit failed (rc={rc})\n{tail}")

def ffmpeg_concat_wavs(inputs: List[Path], out_wav: Path, log_fn=None) -> None:
    if not inputs:
        raise RuntimeError("concat: no inputs")
    if len(inputs) == 1:
        safe_move(inputs[0], out_wav)
        return

    ffmpeg = require_bin("ffmpeg")
    args = [ffmpeg, "-y"]
    for p in inputs:
        args += ["-i", str(p)]

    n = len(inputs)
    filter_parts = []
    for i in range(n):
        filter_parts.append(f"[{i}:a]")
    filter_str = "".join(filter_parts) + f"concat=n={n}:v=0:a=1[a]"

    args += ["-filter_complex", filter_str, "-map", "[a]", "-ac", "1", "-ar", "16000", str(out_wav)]
    rc, out = run_cmd(args, cwd=None)
    if log_fn:
        log_fn(out)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"concat failed (rc={rc})\n{tail}")

def normalize_audio(audio_path: Path, log_fn=None) -> None:
    if not ENABLE_AUDIO_NORMALIZATION:
        return
    try:
        ffmpeg_bin = require_bin("ffmpeg")
        normalized = audio_path.with_suffix(".normalized.wav")
        rc, out = run_cmd(
            [
                ffmpeg_bin,
                "-y",
                "-i",
                str(audio_path),
                "-af",
                "loudnorm=I=-16:TP=-1.5:LRA=11",
                "-ar",
                "16000",
                "-ac",
                "1",
                str(normalized),
            ],
            cwd=None,
        )
        if rc == 0 and normalized.exists() and normalized.stat().st_size > 2048:
            normalized.replace(audio_path)
            if log_fn:
                log_fn("audio_normalized=true")
        else:
            if log_fn:
                log_fn("audio_normalized=false (non-critical)")
    except Exception as e:
        if log_fn:
            log_fn(f"audio_normalized_error={e} (non-critical)")

def mux_audio_into_video(video_in: Path, audio_in: Path, video_out: Path, log_fn=None) -> None:
    ffmpeg = require_bin("ffmpeg")
    video_out.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(video_in),
        "-i",
        str(audio_in),
        "-map",
        "0:v:0",
        "-map",
        "1:a:0",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "160k",
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

# -----------------------------------------------------------------------------
# Segment shaping
# -----------------------------------------------------------------------------

def merge_whisper_segments(segs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    def flush():
        nonlocal cur
        if not cur:
            return
        cur["text"] = clean_text_for_translation(cur.get("text", ""))
        if cur["text"]:
            out.append(cur)
        cur = None

    for s in segs:
        start = float(s.get("start") or 0.0)
        end = float(s.get("end") or start)
        text = (s.get("text") or "").strip()

        if not text:
            continue

        if cur is None:
            cur = {"start": start, "end": end, "text": text}
            continue

        gap = start - float(cur["end"])
        merged_text = (cur["text"] + " " + text).strip()

        should_merge = (
            gap <= MERGE_GAP_SECONDS
            and (len(cur["text"]) < MIN_SEG_CHARS or len(merged_text) <= MAX_SEG_CHARS)
        )

        if should_merge:
            cur["end"] = end
            cur["text"] = merged_text
        else:
            flush()
            cur = {"start": start, "end": end, "text": text}

    flush()
    return out

def pad_or_trim_to_video_length(dub_wav: Path, video_in: Path, out_wav: Path, log_fn=None) -> None:
    """
    Ensure final dub audio matches video duration.
    If we cannot get duration, do nothing.
    """
    vd = video_duration_seconds(video_in)
    if vd is None or vd <= 0:
        if log_fn:
            log_fn("video_duration_unknown=true; skipping final pad/trim")
        dub_wav.replace(out_wav)
        return

    ffmpeg = require_bin("ffmpeg")
    cmd = [
        ffmpeg,
        "-y",
        "-i",
        str(dub_wav),
        "-af",
        "apad",
        "-t",
        f"{vd:.4f}",
        "-ac",
        "1",
        "-ar",
        "16000",
        str(out_wav),
    ]
    rc, out = run_cmd(cmd, cwd=None)
    if log_fn:
        log_fn(f"final_audio_target_sec={vd:.4f}")
        log_fn(out)
    if rc != 0 or (not out_wav.exists()) or out_wav.stat().st_size < 2048:
        tail = "\n".join(out.splitlines()[-200:])
        raise RuntimeError(f"final pad/trim failed (rc={rc})\n{tail}")

# -----------------------------------------------------------------------------
# Artifact retrieval
# -----------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------
# Dubbing
# -----------------------------------------------------------------------------

def process_dub(job_id: str, lang: str, caption_style: str = "clean") -> None:
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        sb_upsert_dub_status(job_id, lang, "error", error="unsupported lang")
        return

    with DUB_SEM:
        dd = dub_dir(job_id, lang)
        dd.mkdir(parents=True, exist_ok=True)
        local_log_path = dub_log_path(job_id, lang)    

        srt_path = dd / "captions.srt"

        def log(line: str) -> None:
            line = line.rstrip()
            with open(local_log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            sb_append_dub_log(job_id, lang, line)

        def heartbeat() -> None:
            sb_upsert_dub_status(job_id, lang, "running", error=None)

        try:
            caption_style = (caption_style or "clean").strip().lower()
            if caption_style not in CAPTION_STYLE_FORCE:
                caption_style = "clean"
            write_dub_status(job_id, lang, "running")
            log("== DUB START (TIMED) ==")
            log(f"timed_dub={ENABLE_TIMED_DUB}")
            log(f"audio_norm={ENABLE_AUDIO_NORMALIZATION}")
            log(f"whisper_model={WHISPER_MODEL} translate={TRANSLATE_PROVIDER}")

            heartbeat()

            job = load_job_for_artifacts(job_id)
            if job.get("status") != "done":
                raise RuntimeError(f"base job not done (status={job.get('status')})")

            video_in, audio_in = ensure_base_artifacts_local(job_id, job)
            out_audio = dub_audio_path(job_id, lang)
            out_video = dub_video_path(job_id, lang)

            mode = (os.getenv("VOICE_GENDER_MODE") or "auto").strip().lower()
            speaker_gender = "unknown"
            speaker_f0 = None

            if mode in {"male", "female"}:
                speaker_gender = mode
            else:
                speaker_f0 = estimate_median_f0(audio_in)
                speaker_gender = infer_gender_from_f0(speaker_f0)

            log(f"speaker_f0_hz={speaker_f0} speaker_gender={speaker_gender}")

            if (
                out_audio.exists()
                and out_video.exists()
                and srt_path.exists()
                and out_audio.stat().st_size > 2048
                and out_video.stat().st_size > 10_000
            ):
                log("cached=true (local dub exists)")
            else:
                ffmpeg = require_bin("ffmpeg")
                _ = ffmpeg  # keeps lint calm

                log("transcribing...")
                tr = whisper_transcribe(audio_in)
                src_text = tr.get("text", "") or ""
                segs = tr.get("segments", []) or []
                log(f"transcribed_chars={len(src_text)} segments={len(segs)}")
                heartbeat()

                if not src_text.strip():
                    raise RuntimeError("transcription empty")

                merged = merge_whisper_segments(segs) if (ENABLE_TIMED_DUB and segs) else []
                log(f"segments_merged={len(merged)}")
                # Timed path
                if ENABLE_TIMED_DUB and merged and len(merged) <= MAX_DUB_SEGMENTS:

                    # Build timed audio using segment timeline + gap silence (best lip sync)
                    
                    if not merged:
                        raise RuntimeError("no usable segments after merge")

                    # Optional: cap segments for speed
                    max_segments = int(os.getenv("MAX_DUB_SEGMENTS", "140"))
                    merged = merged[:max_segments]

                    timeline_parts: List[Path] = []
                    prev_end = 0.0

                    seg_tmp_dir = dd / "segs"
                    seg_tmp_dir.mkdir(parents=True, exist_ok=True)

                    log(f"building_timed_audio segments={len(merged)}")

                    subs: List[Dict[str, Any]] = []

                    for idx, s in enumerate(merged):
                        start = float(s.get("start", 0.0))
                        end = float(s.get("end", 0.0))
                        seg_text = (s.get("text") or "").strip()
                        dur = max(0.06, end - start)

                        if not seg_text:
                            continue

                        heartbeat()

                        # Insert gap silence so we respect original timing
                        gap = start - prev_end
                        if gap > 0.02:
                            sil = seg_tmp_dir / f"seg_{idx:04d}_gap.wav"
                            make_silence_wav(gap, sil)
                            timeline_parts.append(sil)

                        # Translate per segment (keeps boundaries aligned)
                        seg_tr = seg_text if lang == "en" else translate_text(seg_text, lang)
                        seg_tr = (seg_tr or "").strip()
                        if not seg_tr:
                            seg_tr = seg_text  # fallback to avoid accidental silence
                        subs.append({"start": start, "end": end, "text": seg_tr})
                        seg_raw = seg_tmp_dir / f"seg_{idx:04d}_raw.wav"
                        seg_fit = seg_tmp_dir / f"seg_{idx:04d}_fit.wav"

                        # TTS per segment with gender-aware voice
                        voice = _edge_voice_for(lang, speaker_gender)
                        log(f"seg={idx} dur={dur:.2f}s gender={speaker_gender} voice={voice} text={seg_tr[:60]}")

                        # Generate raw segment audio
                        def tts_edge_best_fit(text: str, lang: str, out_wav: Path, target_sec: float, gender: str) -> Tuple[str, float]:
                            """
                            Prefer Edge-native speaking rate changes over ffmpeg atempo.
                            Returns (rate_used, duration_sec).
                            """
                            base = _base_rate_for_lang(lang)
                            for rate in _rate_candidates(base):
                                tts_edge(text, lang, out_wav, rate=rate, log_fn=None, gender=gender)
                                d = wav_duration_seconds(out_wav)
                                if d <= target_sec * SEGMENT_FIT_TOLERANCE:
                                    return rate, d
                            # still too long - keep last and let stretch_or_pad_to_duration atempo it
                            d = wav_duration_seconds(out_wav)
                            return _rate_candidates(base)[-1], d

                        provider = (os.getenv("TTS_PROVIDER") or "auto").strip().lower()
                        if provider in {"auto", "edge"}:
                            rate_used, raw_d = tts_edge_best_fit(seg_tr, lang, seg_raw, dur, speaker_gender)
                            log(f"seg={idx} edge_rate={rate_used} raw_dur={raw_d:.2f}s target={dur:.2f}s")
                        else:
                            tts_espeak(seg_tr, lang, seg_raw)
                            raw_d = wav_duration_seconds(seg_raw)
                            log(f"seg={idx} espeak raw_dur={raw_d:.2f}s target={dur:.2f}s")

                        # Fit to exact segment window without slowing down (pad if short, speed up only if long)
                        stretch_or_pad_to_duration(seg_raw, seg_fit, dur, log_fn=None)
                        timeline_parts.append(seg_fit)

                        prev_end = max(prev_end, end)

                    if subs:
                        write_srt(subs, srt_path)

                    if not timeline_parts:
                        raise RuntimeError("no timeline parts generated")

                    stitched = dd / "stitched.wav"
                    log(f"stitching_parts={len(timeline_parts)}")
                    ffmpeg_concat_wavs(timeline_parts, stitched, log_fn=None)

                    final = dd / "final.wav"
                    pad_or_trim_to_video_length(stitched, video_in, final, log_fn=None)

                    normalize_audio(final, log_fn=log)
                    safe_move(final, out_audio)

                    log("muxing_audio_into_video...")
                    tmp_video = dd / "video_with_audio.mp4"
                    mux_audio_into_video(video_in, out_audio, tmp_video, log_fn=log)

                    if subs:
                        log(f"burning_captions style={caption_style} ...")
                        burn_captions(tmp_video, srt_path, out_video, caption_style, lang=lang, log_fn=log)

                    else:
                        tmp_video.replace(out_video)

                    if not out_video.exists() or out_video.stat().st_size < 10_000:
                        raise RuntimeError("dub video not generated")

                    log("dub_files=generated (timed)")
                else:
                    # Fallback: full text TTS
                    log("timed_dub_unavailable=true (fallback to full-text TTS)")
                    translated = src_text if lang == "en" else translate_text(src_text, lang)
                    vd = video_duration_seconds(video_in) or 0.0
                    end_ts = vd if vd > 0 else float(merged[-1]["end"]) if merged else 5.0
                    write_srt([{"start": 0.0, "end": end_ts, "text": translated}], srt_path)

                    if not translated.strip():
                        raise RuntimeError("translation empty")

                    tts_speak(translated, lang, out_audio, log_fn=log, gender=speaker_gender)
                    if not out_audio.exists() or out_audio.stat().st_size < 2048:
                        raise RuntimeError("dub audio not generated")

                    normalize_audio(out_audio, log_fn=log)

                    log("muxing_audio_into_video...")
                    tmp_video = dd / "video_with_audio.mp4"
                    mux_audio_into_video(video_in, out_audio, tmp_video, log_fn=log)

                    log(f"burning_captions style={caption_style} ...")
                    burn_captions(tmp_video, srt_path, out_video, caption_style, lang=lang, log_fn=log)

                    if not out_video.exists() or out_video.stat().st_size < 10_000:
                        raise RuntimeError("dub video not generated")

                    log("dub_files=generated (fallback)")

            log("uploading_to_storage...")
            audio_key = sb_upload_file(job_id, out_audio, f"dubs/{lang}/audio.wav", "audio/wav")
            video_key = sb_upload_file(job_id, out_video, f"dubs/{lang}/video.mp4", "video/mp4")
            log_key = sb_upload_file(job_id, local_log_path, f"dubs/{lang}/log.txt", "text/plain")

            srt_key = sb_upload_file(job_id, srt_path, f"dubs/{lang}/captions.srt", "text/plain")

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
# API Schemas & Routes
# -----------------------------------------------------------------------------

class CreateJobBody(BaseModel):
    url: HttpUrl

class DubBody(BaseModel):
    lang: str
    caption_style: str = Field(default="clean", alias="captionStyle")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=PlainTextResponse)
def root():
    return "ClipLingua Worker OK (Timed Dub v0.9.0)"

@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/debug/binaries")
def debug_binaries():
    return {
        "python": sys.version,
        "yt_dlp": shutil.which("yt-dlp"),
        "ffmpeg": shutil.which("ffmpeg"),
        "ffprobe": shutil.which("ffprobe"),
        "node": shutil.which("node"),
        "espeak_ng": shutil.which("espeak-ng"),
        "DATA_DIR": str(DATA_DIR.resolve()),
        "TMP_DIR": str(TMP_DIR.resolve()),
        "JOB_STORE_DIR": str(JOB_STORE_DIR.resolve()),
        "PUBLIC_BASE_URL": PUBLIC_BASE_URL or None,
        "has_cookies_b64": bool((os.getenv("YTDLP_COOKIES_B64") or "").strip()),
        "cookies_path": (os.getenv("YTDLP_COOKIES_PATH") or "").strip() or None,
        "supabase_enabled": bool(_supabase),
        "WHISPER_MODEL": WHISPER_MODEL,
        "NLLB_MODEL": NLLB_MODEL,
        "DISABLE_XTTS": DISABLE_XTTS,
        "TRANSLATE_PROVIDER": TRANSLATE_PROVIDER,
        "ENABLE_LOCAL_NLLB": ENABLE_LOCAL_NLLB,
        "MAX_TRANSCRIPT_CHARS": MAX_TRANSCRIPT_CHARS,
        "DUB_STALE_SECONDS": DUB_STALE_SECONDS,
        "BUILD_TAG": BUILD_TAG,
        "ENABLE_AUDIO_NORMALIZATION": ENABLE_AUDIO_NORMALIZATION,
        "ENABLE_SENTENCE_SPLITTING": ENABLE_SENTENCE_SPLITTING,
        "ENABLE_TIMED_DUB": ENABLE_TIMED_DUB,
        "MAX_DUB_SEGMENTS": MAX_DUB_SEGMENTS,
        "EDGE_TTS_VOLUME": EDGE_TTS_VOLUME,
        "EDGE_TTS_PITCH": EDGE_TTS_PITCH,
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
    threading.Thread(target=process_job, args=(job_id, str(body.url)), daemon=True).start()
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
    caption_style = (body.caption_style or "clean").strip().lower()

    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang (use hi/en/es)")

    _ = load_job_for_artifacts(job_id)
    write_dub_status(job_id, lang, "queued")

    threading.Thread(target=process_dub, args=(job_id, lang, caption_style), daemon=True).start()

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
            if st.get("status") in {"running", "queued"}:
                t = _parse_iso(st.get("updated_at") or "")
                if t:
                    age = (datetime.now(timezone.utc) - t).total_seconds()
                    if age > DUB_STALE_SECONDS:
                        sb_upsert_dub_status(job_id, lang, "error", error="stale: worker crashed or restarted")
                        return {
                            "job_id": job_id,
                            "lang": lang,
                            "status": "error",
                            "error": "stale: worker crashed or restarted",
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
