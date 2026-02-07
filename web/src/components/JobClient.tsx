// JobClient.tsx
"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { supabase } from "@/lib/supabase";

type JobStatus = "queued" | "running" | "done" | "error";

type DubStatusValue =
  | "queued"
  | "running"
  | "done"
  | "error"
  | { status?: "queued" | "running" | "done" | "error"; error?: string | null; updated_at?: string };

type Job = {
  id: string;
  url: string;
  status: JobStatus;
  error: string | null;

  video_path: string | null;
  audio_path: string | null;
  log_path: string | null;

  video_url?: string | null;
  audio_url?: string | null;
  log_url?: string | null;

  dub_status?: Record<string, DubStatusValue>;
  created_at: string;
  updated_at: string;
};

type BaseArtifactType = "video" | "audio" | "log";
type DubArtifactType = "dub_video" | "dub_audio" | "dub_log";
type AnyArtifactType = BaseArtifactType | DubArtifactType;

const LANGS = [
  { code: "hi", label: "Hindi" },
  { code: "en", label: "English" },
  { code: "es", label: "Spanish" },
] as const;

type LangCode = (typeof LANGS)[number]["code"];

const CAPTION_STYLES = [
  { id: "clean", label: "Clean" },
  { id: "bold", label: "Bold" },
  { id: "boxed", label: "Boxed" },
  { id: "big", label: "Big" },
] as const;

type CaptionStyle = (typeof CAPTION_STYLES)[number]["id"];

type DubState = { status: "not_started" | "queued" | "running" | "done" | "error"; error?: string | null };

function parseJsonSafe<T = any>(text: string): T | null {
  try {
    return text ? (JSON.parse(text) as T) : null;
  } catch {
    return null;
  }
}

function readDubState(job: Job | null, lang: string): DubState {
  const raw = job?.dub_status?.[lang];
  if (!raw) return { status: "not_started" };

  if (typeof raw === "string") {
    if (raw === "queued" || raw === "running" || raw === "done" || raw === "error") return { status: raw };
    return { status: "running" };
  }

  const s = raw.status ?? "running";
  const e = raw.error ?? null;

  if (s === "queued" || s === "running" || s === "done" || s === "error") return { status: s, error: e };
  return { status: "running", error: e };
}

function anyDubRunning(job: Job | null): boolean {
  const ds = job?.dub_status;
  if (!ds) return false;

  for (const v of Object.values(ds)) {
    if (typeof v === "string") {
      if (v === "queued" || v === "running") return true;
    } else if (v && typeof v === "object") {
      if (v.status === "queued" || v.status === "running") return true;
    }
  }
  return false;
}

function safeOpen(url: string) {
  if (typeof window !== "undefined") window.open(url, "_blank", "noopener,noreferrer");
}

export default function JobClient({
  onJobCreated,
  onCreditsChanged,
}: {
  onJobCreated?: () => void;
  onCreditsChanged?: () => void;
}) {
  const [ytUrl, setYtUrl] = useState("");

  const [jobId, setJobId] = useState<string | null>(null);
  const [job, setJob] = useState<Job | null>(null);

  const [msg, setMsg] = useState<string | null>(null);

  const [creating, setCreating] = useState(false);
  const [polling, setPolling] = useState(false);
  const [dubStarting, setDubStarting] = useState(false);

  const [downloading, setDownloading] = useState<AnyArtifactType | null>(null);

  const [selectedLang, setSelectedLang] = useState<LangCode>("hi");
  const [captionStyle, setCaptionStyle] = useState<CaptionStyle>("clean");

  const [ytConnected, setYtConnected] = useState(false);
  const [ytConnecting, setYtConnecting] = useState(false);

  const [activeDubLang, setActiveDubLang] = useState<string | null>(null);

  // IMPORTANT: no "window" types here
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const busy = creating || polling || dubStarting;

  const canSubmit = useMemo(() => ytUrl.trim().length > 0 && !busy, [ytUrl, busy]);

  const clearTimer = useCallback(() => {
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = null;
  }, []);

  const abortInFlight = useCallback(() => {
    if (abortRef.current) abortRef.current.abort();
    abortRef.current = null;
  }, []);

  const getAccessToken = useCallback(async (): Promise<string | null> => {
    const { data } = await supabase.auth.getSession();
    return data.session?.access_token ?? null;
  }, []);

  const refreshYTStatus = useCallback(async () => {
    const token = await getAccessToken();
    if (!token) return;

    const res = await fetch("/api/youtube/status", {
      cache: "no-store",
      headers: { Authorization: `Bearer ${token}` },
    });

    const text = await res.text();
    const data = parseJsonSafe<any>(text) ?? {};
    setYtConnected(!!data?.connected);
  }, [getAccessToken]);

  const pollOnce = useCallback(
    async (id: string, signal: AbortSignal): Promise<Job> => {
      const token = await getAccessToken();
      if (!token) throw new Error("Session expired. Please sign in again.");

      const res = await fetch(`/api/clip/status?jobId=${encodeURIComponent(id)}`, {
        cache: "no-store",
        headers: { Authorization: `Bearer ${token}` },
        signal,
      });

      const text = await res.text();
      const data = parseJsonSafe<any>(text) ?? {};

      if (!res.ok) throw new Error(String(data?.error || text || `Request failed (${res.status})`));
      return data as Job;
    },
    [getAccessToken]
  );

  useEffect(() => {
    refreshYTStatus().catch(() => {});
  }, [refreshYTStatus]);

  useEffect(() => {
    if (!jobId) return;

    const id = jobId;

    let alive = true;
    clearTimer();
    abortInFlight();

    const controller = new AbortController();
    abortRef.current = controller;

    async function loop() {
      try {
        setPolling(true);

        const data = await pollOnce(id, controller.signal);
        if (!alive) return;

        setJob(data);

        const baseRunning = data.status === "queued" || data.status === "running";
        const baseErrored = data.status === "error";

        const activeLang = activeDubLang;
        const activeLangState = activeLang ? readDubState(data, activeLang) : null;

        // Auto-clear active dub lang when terminal
        if (activeLang && activeLangState && (activeLangState.status === "done" || activeLangState.status === "error")) {
          setActiveDubLang(null);
        }

        const dubStillRunning =
          anyDubRunning(data) ||
          (activeLangState?.status === "queued" || activeLangState?.status === "running");

        if (baseErrored) {
          setMsg(data.error || "Job failed");
          setPolling(false);
          clearTimer();
          return;
        }

        if (!baseRunning && !dubStillRunning) {
          setMsg("Done. Download artifacts below. Start a dub to generate translated versions.");
          setPolling(false);
          clearTimer();
          return;
        }

        if (baseRunning) {
          setMsg("Processing...");
        } else if (activeLang && activeLangState?.status && (activeLangState.status === "queued" || activeLangState.status === "running")) {
          setMsg(`Dub running for ${activeLang.toUpperCase()}...`);
        } else if (dubStillRunning) {
          setMsg("Dub running...");
        }

        timerRef.current = setTimeout(loop, 2000);
      } catch (e: any) {
        if (!alive) return;
        if (e?.name === "AbortError") return;
        setMsg(e?.message || "Polling failed");
        setPolling(false);
        clearTimer();
      }
    }

    loop();

    return () => {
      alive = false;
      clearTimer();
      abortInFlight();
      setPolling(false);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId, activeDubLang]);

  async function openArtifactUrl(params: { jobId: string; type: AnyArtifactType; lang?: string }) {
    const token = await getAccessToken();
    if (!token) throw new Error("Please sign in first.");

    const sp = new URLSearchParams();
    sp.set("jobId", params.jobId);
    sp.set("type", params.type);
    if (params.lang) sp.set("lang", params.lang);

    const res = await fetch(`/api/clip/artifact?${sp.toString()}`, {
      cache: "no-store",
      headers: { Authorization: `Bearer ${token}` },
    });

    const text = await res.text();
    const data = parseJsonSafe<any>(text) ?? {};

    if (!res.ok) throw new Error(String(data?.error || text || `Request failed (${res.status})`));

    const url = String(data?.url || "").trim();
    if (!url) throw new Error("No download URL returned");

    safeOpen(url);
  }

  async function createJob() {
    setMsg(null);
    setCreating(true);
    setJob(null);
    setJobId(null);
    setActiveDubLang(null);
    clearTimer();
    abortInFlight();

    try {
      const token = await getAccessToken();
      if (!token) throw new Error("Please sign in first.");

      const res = await fetch("/api/clip/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ url: ytUrl.trim() }),
      });

      const text = await res.text();
      const data = parseJsonSafe<any>(text) ?? {};

      if (!res.ok) throw new Error(String(data?.error || text || `Request failed (${res.status})`));

      const id: string | undefined = data?.jobId ?? data?.id;
      if (!id) throw new Error("Server did not return a job id");

      setJobId(id);
      setMsg("Job created. Processing...");
      onJobCreated?.();
      onCreditsChanged?.();
    } catch (e: any) {
      setMsg(e?.message || "Failed to create job");
    } finally {
      setCreating(false);
    }
  }

  async function downloadBase(type: BaseArtifactType) {
    if (!jobId) return;
    setMsg(null);
    setDownloading(type);
    try {
      await openArtifactUrl({ jobId, type });
    } catch (e: any) {
      setMsg(e?.message || "Download failed");
    } finally {
      setDownloading(null);
    }
  }

  async function startDub(lang: string) {
    if (!jobId) return;

    setMsg(null);
    setDubStarting(true);

    try {
      const token = await getAccessToken();
      if (!token) throw new Error("Please sign in first.");

      const res = await fetch("/api/clip/dub", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ jobId, lang, captionStyle }),
      });

      const text = await res.text();
      const data = parseJsonSafe<any>(text) ?? {};

      if (!res.ok) throw new Error(String(data?.error || text || `Request failed (${res.status})`));

      setActiveDubLang(lang);
      setMsg(`Dub started for ${lang.toUpperCase()}.`);
      onCreditsChanged?.();
    } catch (e: any) {
      setMsg(e?.message || "Dub start failed");
    } finally {
      setDubStarting(false);
    }
  }

  async function downloadDub(kind: "video" | "audio" | "log") {
    if (!jobId) return;

    const type = `dub_${kind}` as DubArtifactType;

    setMsg(null);
    setDownloading(type);
    try {
      await openArtifactUrl({ jobId, type, lang: selectedLang });
    } catch (e: any) {
      setMsg(e?.message || "Dub download failed");
    } finally {
      setDownloading(null);
    }
  }

  async function connectYouTube() {
    setMsg(null);
    setYtConnecting(true);

    try {
      const token = await getAccessToken();
      if (!token) throw new Error("Please sign in first.");

      const res = await fetch("/api/youtube/start", {
        cache: "no-store",
        headers: { Authorization: `Bearer ${token}` },
      });

      const text = await res.text();
      const data = parseJsonSafe<any>(text) ?? {};

      if (!res.ok || !data?.url) throw new Error(String(data?.error || "Failed to start OAuth"));

      const w = 520;
      const h = 700;

      // Center popup
      const left = Math.max(0, window.screenX + (window.outerWidth - w) / 2);
      const top = Math.max(0, window.screenY + (window.outerHeight - h) / 2);

      const popup = window.open(
        data.url,
        "yt_oauth",
        `width=${w},height=${h},left=${left},top=${top},noopener,noreferrer`
      );

      if (!popup) throw new Error("Popup blocked. Allow popups for this site.");

      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          window.removeEventListener("message", onMsg);
          reject(new Error("OAuth timed out. Please try again."));
        }, 2 * 60 * 1000);

        const onMsg = (ev: MessageEvent) => {
          if (ev.origin !== window.location.origin) return;
          if (ev.data?.type === "YT_OAUTH_DONE") {
            clearTimeout(timeout);
            window.removeEventListener("message", onMsg);
            resolve();
          }
        };

        window.addEventListener("message", onMsg);
      });

      await refreshYTStatus();
      setMsg("YouTube connected.");
    } catch (e: any) {
      setMsg(e?.message || "YouTube connect failed");
    } finally {
      setYtConnecting(false);
    }
  }

  const dubState = readDubState(job, selectedLang);
  const baseDone = job?.status === "done";

  return (
    <div className="w-full">
      <div className="mb-3 flex flex-wrap items-center gap-2">
        <button
          onClick={connectYouTube}
          disabled={ytConnecting}
          className="border rounded-md px-4 py-2 disabled:opacity-60"
        >
          {ytConnecting ? "Connecting..." : ytConnected ? "YouTube connected" : "Connect YouTube"}
        </button>

        {jobId && (
          <button
            className="border rounded-md px-4 py-2 disabled:opacity-60"
            onClick={async () => {
              try {
                await navigator.clipboard.writeText(jobId);
                setMsg("Copied job id.");
              } catch {
                setMsg("Copy failed.");
              }
            }}
          >
            Copy job id
          </button>
        )}
      </div>

      <div className="flex gap-2">
        <input
          className="border rounded-md px-4 py-3 flex-1"
          placeholder="https://www.youtube.com/watch?v=..."
          value={ytUrl}
          onChange={(e) => setYtUrl(e.target.value)}
        />
        <button
          onClick={createJob}
          disabled={!canSubmit}
          className="rounded-md px-5 py-3 bg-black text-white disabled:opacity-50"
        >
          {creating ? "Creating..." : polling ? "Working..." : "Create job"}
        </button>
      </div>

      {jobId && (
        <p className="mt-3 text-sm opacity-70">
          Job: <span className="font-mono">{jobId}</span>
        </p>
      )}

      {job && (
        <div className="mt-4 border rounded-lg p-4">
          <p className="text-sm">
            Status: <span className="font-semibold">{job.status}</span>
          </p>

          {job.status === "error" && job.error && (
            <p className="mt-2 text-sm text-red-700 whitespace-pre-wrap">{job.error}</p>
          )}

          {baseDone && (
            <>
              <div className="mt-4 flex flex-wrap gap-2">
                <button
                  onClick={() => downloadBase("video")}
                  className="border rounded-md px-4 py-2 disabled:opacity-60"
                  disabled={!!downloading}
                >
                  {downloading === "video" ? "Preparing..." : "Download video"}
                </button>

                <button
                  onClick={() => downloadBase("audio")}
                  className="border rounded-md px-4 py-2 disabled:opacity-60"
                  disabled={!!downloading}
                >
                  {downloading === "audio" ? "Preparing..." : "Download audio"}
                </button>

                <button
                  onClick={() => downloadBase("log")}
                  className="border rounded-md px-4 py-2 disabled:opacity-60"
                  disabled={!!downloading}
                >
                  {downloading === "log" ? "Preparing..." : "Download log"}
                </button>
              </div>

              <div className="mt-6 border-t pt-4">
                <div className="flex flex-wrap items-center gap-3">
                  <p className="text-sm font-semibold">Dub</p>

                  <select
                    className="border rounded-md px-3 py-2 text-sm"
                    value={selectedLang}
                    onChange={(e) => setSelectedLang(e.target.value as LangCode)}
                    disabled={dubStarting || !!downloading}
                  >
                    {LANGS.map((l) => (
                      <option key={l.code} value={l.code}>
                        {l.label} ({l.code})
                      </option>
                    ))}
                  </select>

                  <select
                    className="border rounded-md px-3 py-2 text-sm"
                    value={captionStyle}
                    onChange={(e) => setCaptionStyle(e.target.value as CaptionStyle)}
                    disabled={dubStarting || !!downloading}
                  >
                    {CAPTION_STYLES.map((s) => (
                      <option key={s.id} value={s.id}>
                        Captions: {s.label}
                      </option>
                    ))}
                  </select>

                  <button
                    onClick={() => startDub(selectedLang)}
                    disabled={dubStarting || !!downloading}
                    className="rounded-md px-4 py-2 bg-black text-white disabled:opacity-50"
                  >
                    {dubStarting ? "Starting..." : `Start dub (${selectedLang})`}
                  </button>

                  <span className="text-xs opacity-70">
                    Status: <span className="font-semibold">{dubState.status}</span>
                  </span>
                </div>

                {dubState.status === "error" && (
                  <p className="mt-2 text-sm text-red-700 whitespace-pre-wrap">
                    {dubState.error || "Dub failed"}
                  </p>
                )}

                <div className="mt-3 flex flex-wrap gap-2">
                  <button
                    onClick={() => downloadDub("video")}
                    className="border rounded-md px-4 py-2 disabled:opacity-60"
                    disabled={dubState.status !== "done" || !!downloading}
                  >
                    {downloading === "dub_video" ? "Preparing..." : "Download dub video"}
                  </button>

                  <button
                    onClick={() => downloadDub("audio")}
                    className="border rounded-md px-4 py-2 disabled:opacity-60"
                    disabled={dubState.status !== "done" || !!downloading}
                  >
                    {downloading === "dub_audio" ? "Preparing..." : "Download dub audio"}
                  </button>

                  <button
                    onClick={() => downloadDub("log")}
                    className="border rounded-md px-4 py-2 disabled:opacity-60"
                    disabled={dubState.status !== "done" || !!downloading}
                  >
                    {downloading === "dub_log" ? "Preparing..." : "Download dub log"}
                  </button>
                </div>
              </div>
            </>
          )}
        </div>
      )}

      {msg && <p className="mt-3 text-sm opacity-80 whitespace-pre-wrap">{msg}</p>}
    </div>
  );
}
