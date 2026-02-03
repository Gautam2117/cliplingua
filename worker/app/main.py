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
from urllib.request import urlretrieve

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, Response
from pydantic import BaseModel, HttpUrl
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "/tmp/cliplingua/data"))
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp/cliplingua/tmp"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_SERVICE_ROLE_KEY = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

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

# Optional Supabase client (service role, server-side only)
_supabase = None
if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
    try:
        from supabase import create_client
        _supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    except Exception:
        _supabase = None

from typing import Optional
from pathlib import Path

ARTIFACT_BUCKET = (os.getenv("ARTIFACT_BUCKET", "artifacts") or "artifacts").strip()

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
                    "upsert": "true",
                    # Supabase docs say MIME defaults to text/html if file_options not specified.
                    # They do not show the exact key for MIME in python, so keep it out for now
                    # to avoid a silent failure. :contentReference[oaicite:2]{index=2}
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
    """
    Download a known storage key to dest_path.
    """
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
    """
    Download by computed key jobs/<job_id>/<filename>.
    """
    return sb_download_key(storage_key(job_id, filename), dest_path)

app = FastAPI(title="ClipLingua Worker", version="0.6.4")

@lru_cache(maxsize=1)
def get_models():
    """
    Lazy-load models once per worker instance.
    Keeps them in memory for faster subsequent jobs.
    """
    models = {}

    # 1) ASR: faster-whisper
    from faster_whisper import WhisperModel
    # tiny/medium tradeoff: start with "small" for quality-speed balance on CPU
    models["whisper"] = WhisperModel("small", device="cpu", compute_type="int8")

    # 2) Translator: NLLB (offline)
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    nllb_model = "facebook/nllb-200-distilled-600M"
    models["nllb_tokenizer"] = AutoTokenizer.from_pretrained(nllb_model)
    models["nllb"] = AutoModelForSeq2SeqLM.from_pretrained(nllb_model)

    # 3) TTS: Coqui XTTS v2
    # Heavy model. We load it once.
    from TTS.api import TTS
    models["tts"] = TTS("tts_models/multilingual/multi-dataset/xtts_v2")

    return models

class CreateJobBody(BaseModel):
    url: HttpUrl

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def job_dir(job_id: str) -> Path:
    return JOB_STORE_DIR / job_id


def job_json_path(job_id: str) -> Path:
    return job_dir(job_id) / "job.json"


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


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
            "updated_at": datetime.now(timezone.utc).isoformat(),

            "video_url": payload.get("video_url"),
            "audio_url": payload.get("audio_url"),
            "log_url": payload.get("log_url"),

            # NEW: persist storage keys
            "storage_video_key": payload.get("storage_video_key"),
            "storage_audio_key": payload.get("storage_audio_key"),
            "storage_log_key": payload.get("storage_log_key"),
        }

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
        # Append by fetching current, then updating. Simple and reliable for now.
        res = _supabase.table("clip_jobs").select("log_text").eq("id", job_id).limit(1).execute()
        data = getattr(res, "data", None)
        current = ""
        if data and isinstance(data, list) and len(data) > 0:
            current = data[0].get("log_text") or ""
        updated = current + ("" if current.endswith("\n") or current == "" else "\n") + text
        _supabase.table("clip_jobs").update(
            {"log_text": updated, "updated_at": datetime.now(timezone.utc).isoformat()}
        ).eq("id", job_id).execute()
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


def save_job_local(job_id: str, payload: Dict[str, Any]) -> None:
    atomic_write_text(job_json_path(job_id), json.dumps(payload, indent=2))


def load_job_local(job_id: str) -> Optional[Dict[str, Any]]:
    p = job_json_path(job_id)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))

    # Backward compat
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


def load_job(job_id: str) -> Dict[str, Any]:
    # Supabase first for persistence
    sb = sb_get_job(job_id)
    if sb:
        # Return shape similar to local JSON
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

        }

    local = load_job_local(job_id)
    if local:
        return local

    raise HTTPException(status_code=404, detail="job not found")


def update_job(job_id: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    local = load_job_local(job_id) or {"id": job_id}
    local.update(patch)
    save_job_local(job_id, local)
    sb_upsert_job(job_id, local)
    return local


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


def _job_artifact_paths(job_id: str) -> Dict[str, Path]:
    jd = job_dir(job_id)
    return {
        "job_dir": jd,
        "video": jd / "video.mp4",
        "audio": jd / "audio.wav",
        "log": jd / "log.txt",
        "runner_log": jd / "runner.log",
    }

def dub_status_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "status.json"

def write_dub_status(job_id: str, lang: str, status: str, error: str | None = None) -> None:
    p = dub_status_path(job_id, lang)
    p.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(p, json.dumps({
        "job_id": job_id,
        "lang": lang,
        "status": status,   # queued | running | done | error
        "error": error,
        "updated_at": now_iso(),
    }, indent=2))

SUPPORTED_DUB_LANGS = {"hi", "en", "es"}

def dub_dir(job_id: str, lang: str) -> Path:
    return job_dir(job_id) / "dubs" / lang

def dub_audio_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "audio.wav"

def dub_video_path(job_id: str, lang: str) -> Path:
    return dub_dir(job_id, lang) / "video.mp4"

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
            "--retries", "5",
            "--fragment-retries", "5",
            "--socket-timeout", "30",
            "--concurrent-fragments", "2",
            "--sleep-interval", "1",
            "--max-sleep-interval", "3",
            "--extractor-args", "youtube:player_client=web",
        ]

        if yt_dlp_supports(yt_dlp_bin, "--js-runtimes"):
            dl_cmd += ["--js-runtimes", "node"]
            log_lines.append("js_runtime=enabled(--js-runtimes node)")

        if yt_dlp_supports(yt_dlp_bin, "--remote-components"):
            dl_cmd += ["--remote-components", "ejs:github"]
            log_lines.append("remote_components=enabled(ejs:github)")

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
        sb_append_log(job_id, "\n".join(log_lines[-60:]) + "\n")

        if rc != 0:
            tail = "\n".join(out.splitlines()[-160:])
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
        sb_append_log(job_id, "\n".join(log_lines[-60:]) + "\n")

        if rc2 != 0:
            tail = "\n".join(out2.splitlines()[-160:])
            sb_append_log(job_id, "\nERROR: audio extraction failed\n" + tail + "\n")
            raise RuntimeError(f"audio extraction failed (rc={rc2})\n{tail}")

        if not wait_for_file(tmp_audio, min_bytes=1024 * 10):
            sb_append_log(job_id, "\nERROR: wav not ready\n")
            raise RuntimeError("audio wav not ready")

        # Move finalized artifacts to job dir (ephemeral on Render, but consistent within uptime).
        paths["video"].parent.mkdir(parents=True, exist_ok=True)
        merged_video.replace(paths["video"])
        tmp_audio.replace(paths["audio"])

        log_lines.append("== DONE ==")
        log_lines.append(f"video={paths['video'].resolve()}")
        log_lines.append(f"audio={paths['audio'].resolve()}")

        out_log.write_text("\n".join(log_lines), encoding="utf-8")
        sb_append_log(job_id, "\n== DONE ==\n")

        urls = _artifact_urls(job_id)

        # Persist to Supabase Storage so artifacts survive restarts
        video_key = sb_upload_file(job_id, paths["video"], "video.mp4", "video/mp4")
        audio_key = sb_upload_file(job_id, paths["audio"], "audio.wav", "audio/wav")
        log_key   = sb_upload_file(job_id, paths["log"],  "log.txt",  "text/plain")

        persist_patch = {
            "storage_video_key": video_key,
            "storage_audio_key": audio_key,
            "storage_log_key": log_key,
        }

        # Save keys (local + supabase row)
        update_job(job_id, persist_patch)

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
            out_log.write_text("\n".join(log_lines) + f"\nERROR: {e}\n", encoding="utf-8")
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
    """
    target_lang: hi/en/es
    NLLB language codes:
      - eng_Latn
      - hin_Deva
      - spa_Latn
    """
    if not text.strip():
        return ""

    tgt_map = {"en": "eng_Latn", "hi": "hin_Deva", "es": "spa_Latn"}
    tgt = tgt_map[target_lang]

    m = get_models()
    tok = m["nllb_tokenizer"]
    model = m["nllb"]

    # NLLB needs src_lang set sometimes; we keep it generic.
    inputs = tok(text, return_tensors="pt", truncation=True, max_length=1024)

    forced_bos = tok.convert_tokens_to_ids(tgt)
    out = model.generate(**inputs, forced_bos_token_id=forced_bos, max_new_tokens=512)

    return tok.batch_decode(out, skip_special_tokens=True)[0]

def xtts_speak(text: str, lang: str, out_wav: Path) -> None:
    """
    XTTS supports language codes like: "en", "es", "hi"
    Voice cloning is optional; for now we use default speaker.
    """
    out_wav.parent.mkdir(parents=True, exist_ok=True)
    m = get_models()
    tts = m["tts"]

    # XTTS can synthesize directly to file
    tts.tts_to_file(text=text, file_path=str(out_wav), language=lang)

def mux_audio_into_video(ffmpeg_bin: str, video_in: Path, audio_in: Path, video_out: Path) -> None:
    video_out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        ffmpeg_bin, "-y",
        "-i", str(video_in),
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
    if rc != 0:
        tail = "\n".join(out.splitlines()[-120:])
        raise RuntimeError(f"ffmpeg mux failed (rc={rc})\n{tail}")


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

    runner = (
        "import os;"
        "from worker.app.main import process_job;"
        "process_job(os.environ['CLIPLINGUA_JOB_ID'], os.environ['CLIPLINGUA_JOB_URL'])"
    )

    log_f = open(paths["runner_log"], "a", encoding="utf-8")

    subprocess.Popen(
        [sys.executable, "-u", "-c", runner],
        cwd=str(Path(__file__).resolve().parents[2]),
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

    # First try supabase log_text
    sb = sb_get_log(job_id)
    if sb:
        return sb

    # Then local
    if lp.exists():
        return lp.read_text(encoding="utf-8", errors="ignore")

    # Then storage
    ok = ensure_local_from_storage(job_id, job, "log.txt", lp, "storage_log_key")
    if ok:
        return lp.read_text(encoding="utf-8", errors="ignore")

    # Then runner log
    rlp = paths["runner_log"]
    if rlp.exists():
        return rlp.read_text(encoding="utf-8", errors="ignore")

    raise HTTPException(status_code=404, detail="log not ready")

def _find_tmp_job_dir(job_id: str) -> Path:
    return TMP_DIR / job_id


def _pick_video_from_tmp(tmp_job_dir: Path) -> Optional[Path]:
    # Common outputs: download.mp4, download.<ext>, or merged mp4.
    candidates = []
    candidates += list(tmp_job_dir.glob("download*.mp4"))
    candidates += list(tmp_job_dir.glob("*.mp4"))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_size, reverse=True)[0]


def _pick_audio_from_tmp(tmp_job_dir: Path) -> Optional[Path]:
    p = tmp_job_dir / "audio.wav"
    if p.exists() and p.is_file():
        return p
    # fallback search
    wavs = [x for x in tmp_job_dir.glob("*.wav") if x.is_file()]
    if not wavs:
        return None
    return sorted(wavs, key=lambda p: p.stat().st_size, reverse=True)[0]


def _file_head_or_404(file_path: Optional[str]) -> Response:
    if not file_path:
        raise HTTPException(status_code=404, detail="file not ready")
    p = safe_file_or_404(file_path, "file missing")
    return Response(headers={"Content-Length": str(p.stat().st_size)})


def _resolve_artifacts_and_patch(job_id: str, job: Dict[str, Any]) -> Dict[str, Any]:
    """
    If job json is missing video_path/audio_path but files exist in JOB_STORE_DIR,
    patch job json so /video and /audio become consistent.
    """
    paths = _job_artifact_paths(job_id)
    jd = paths["job_dir"]

    patch: Dict[str, Any] = {}

    # Prefer JOB_STORE_DIR outputs
    if (not job.get("video_path")) and paths["video"].exists():
        patch["video_path"] = str(paths["video"].resolve())

    if (not job.get("audio_path")) and paths["audio"].exists():
        patch["audio_path"] = str(paths["audio"].resolve())

    if (not job.get("log_path")) and paths["log"].exists():
        patch["log_path"] = str(paths["log"].resolve())

    # Optional fallback: TMP_DIR (only if still exists)
    tmp_job_dir = TMP_DIR / job_id
    if (not patch.get("video_path")) and (not job.get("video_path")):
        v = _pick_video_from_tmp(tmp_job_dir) if tmp_job_dir.exists() else None
        if v:
            patch["video_path"] = str(v.resolve())

    if (not patch.get("audio_path")) and (not job.get("audio_path")):
        a = _pick_audio_from_tmp(tmp_job_dir) if tmp_job_dir.exists() else None
        if a:
            patch["audio_path"] = str(a.resolve())

    if patch:
        patch["updated_at"] = now_iso()
        patch["updated_at_ts"] = time.time()
        job = update_job(job_id, patch)

    return job

def load_job_for_artifacts(job_id: str) -> Dict[str, Any]:
    """
    For serving files, always prefer local job.json because it contains
    video_path/audio_path and points to files in JOB_STORE_DIR.
    Supabase row may not have those columns.
    """
    local = load_job_local(job_id)
    if local:
        return local

    # fallback
    return load_job(job_id)

def ensure_local_from_storage(job_id: str, job: Dict[str, Any], filename: str, local_path: Path, key_field: str) -> bool:
    """
    Ensure local_path exists. If missing, download from storage using saved key, else fallback computed key.
    """
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

    # fallback to computed key
    return sb_download_file(job_id, filename, local_path)

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

@app.head("/jobs/{job_id}/audio")
def head_job_audio(job_id: str):
    job = load_job_for_artifacts(job_id)
    job = _resolve_artifacts_and_patch(job_id, job)
    return _file_head_or_404(job.get("audio_path"))


@app.get("/jobs/{job_id}/video")
def get_job_video(job_id: str):
    job = load_job(job_id)  # IMPORTANT: use supabase row which has storage keys
    paths = _job_artifact_paths(job_id)

    # try local path first
    vp = job.get("video_path")
    local_p = Path(vp) if vp else paths["video"]

    ok = ensure_local_from_storage(job_id, job, "video.mp4", local_p, "storage_video_key")
    if not ok:
        raise HTTPException(status_code=404, detail="video not ready")

    # patch job.json if needed
    if not job.get("video_path"):
        update_job(job_id, {"video_path": str(local_p.resolve()), "updated_at": now_iso(), "updated_at_ts": time.time()})

    return FileResponse(path=str(local_p), media_type="video/mp4", filename=f"{job_id}.mp4")


@app.head("/jobs/{job_id}/video")
def head_job_video(job_id: str):
    job = load_job_for_artifacts(job_id)
    job = _resolve_artifacts_and_patch(job_id, job)
    return _file_head_or_404(job.get("video_path"))

class DubBody(BaseModel):
    lang: str  # "hi" | "en" | "es"

from urllib.request import urlretrieve

def ensure_base_artifacts_local(job_id: str, job: Dict[str, Any]) -> Tuple[Path, Path]:
    paths = _job_artifact_paths(job_id)
    paths["job_dir"].mkdir(parents=True, exist_ok=True)

    video_p = Path(job["video_path"]) if job.get("video_path") else paths["video"]
    audio_p = Path(job["audio_path"]) if job.get("audio_path") else paths["audio"]

    # If missing locally, pull from Storage using stored keys
    if (not video_p.exists()) or video_p.stat().st_size < 10_000:
        key = job.get("storage_video_key") or storage_key(job_id, "video.mp4")
        ok = sb_download_key(key, video_p)
        if not ok:
            raise RuntimeError("base video missing locally and not found in storage")

    if (not audio_p.exists()) or audio_p.stat().st_size < 2_000:
        key = job.get("storage_audio_key") or storage_key(job_id, "audio.wav")
        ok = sb_download_key(key, audio_p)
        if not ok:
            raise RuntimeError("base audio missing locally and not found in storage")

    # Patch paths for consistency
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
        write_dub_status(job_id, lang, "error", "unsupported lang")
        return

    dd = dub_dir(job_id, lang)
    dd.mkdir(parents=True, exist_ok=True)
    log_path = dd / "log.txt"

    def log(line: str):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")

    try:
        write_dub_status(job_id, lang, "running")
        log("== DUB START ==")

        job = load_job_for_artifacts(job_id)
        job = _resolve_artifacts_and_patch(job_id, job)

        if job.get("status") != "done":
            raise RuntimeError(f"base job not done (status={job.get('status')})")

        video_in, audio_in = ensure_base_artifacts_local(job_id, job)

        out_audio = dub_audio_path(job_id, lang)
        out_video = dub_video_path(job_id, lang)

        if out_audio.exists() and out_video.exists():
            log("cached=true (already exists)")
            write_dub_status(job_id, lang, "done")
            return

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

        log("== DUB DONE ==")
        write_dub_status(job_id, lang, "done")

    except Exception as e:
        log(f"ERROR: {e}")
        write_dub_status(job_id, lang, "error", str(e))


@app.post("/jobs/{job_id}/dub")
def dub_job(job_id: str, body: DubBody):
    lang = (body.lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang (use hi/en/es)")

    # Ensure base job exists
    _ = load_job_for_artifacts(job_id)

    # Write queued status immediately
    write_dub_status(job_id, lang, "queued")

    # Spawn background subprocess like your clip job
    env = os.environ.copy()
    env["CLIPLINGUA_DUB_JOB_ID"] = job_id
    env["CLIPLINGUA_DUB_LANG"] = lang

    runner = (
        "import os;"
        "from app.main import process_dub;"
        "process_dub(os.environ['CLIPLINGUA_DUB_JOB_ID'], os.environ['CLIPLINGUA_DUB_LANG'])"
    )

    dd = dub_dir(job_id, lang)
    dd.mkdir(parents=True, exist_ok=True)
    runner_log = open(dd / "runner.log", "a", encoding="utf-8")

    subprocess.Popen(
        [sys.executable, "-u", "-c", runner],
        cwd=str(Path(__file__).resolve().parents[1]),
        stdout=runner_log,
        stderr=runner_log,
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
    }

@app.get("/jobs/{job_id}/dubs/{lang}/status")
def get_dub_status(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")
    p = dub_status_path(job_id, lang)
    if not p.exists():
        return {"job_id": job_id, "lang": lang, "status": "not_started"}
    return json.loads(p.read_text(encoding="utf-8"))

@app.get("/jobs/{job_id}/dubs/{lang}/log", response_class=PlainTextResponse)
def get_dub_log(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    p = dub_dir(job_id, lang) / "log.txt"
    if not p.exists():
        # fall back to runner log
        rp = dub_dir(job_id, lang) / "runner.log"
        if rp.exists():
            return rp.read_text(encoding="utf-8", errors="ignore")
        raise HTTPException(status_code=404, detail="log not ready")
    return p.read_text(encoding="utf-8", errors="ignore")

@app.get("/jobs/{job_id}/dubs/{lang}/audio")
def get_dub_audio(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")
    p = dub_audio_path(job_id, lang)
    if not p.exists():
        raise HTTPException(status_code=404, detail="dub audio not ready")
    return FileResponse(path=str(p), media_type="audio/wav", filename=f"{job_id}_{lang}.wav")

@app.get("/jobs/{job_id}/dubs/{lang}/video")
def get_dub_video(job_id: str, lang: str):
    lang = (lang or "").strip().lower()
    if lang not in SUPPORTED_DUB_LANGS:
        raise HTTPException(status_code=400, detail="unsupported lang")
    p = dub_video_path(job_id, lang)
    if not p.exists():
        raise HTTPException(status_code=404, detail="dub video not ready")
    return FileResponse(path=str(p), media_type="video/mp4", filename=f"{job_id}_{lang}.mp4")

