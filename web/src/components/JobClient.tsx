// src/components/JobClient.tsx
"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { supabase } from "@/lib/supabase";

type JobStatus = "queued" | "running" | "done" | "error";

type DubState = {
  status: "not_started" | "running" | "done" | "error";
  error?: string | null;
};

type Job = {
  id: string;
  url: string;
  status: JobStatus;
  error: string | null;

  // worker-side paths
  video_path: string | null;
  audio_path: string | null;
  log_path: string | null;

  // worker-side public URLs (optional)
  video_url?: string | null;
  audio_url?: string | null;
  log_url?: string | null;

  // worker dub status (optional)
  dub_status?: Record<string, { status?: string; error?: string | null } | string>;

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

function readDubStatus(job: Job | null, lang: string): DubState {
  if (!job?.dub_status) return { status: "not_started" };

  const raw = (job.dub_status as any)?.[lang];
  if (!raw) return { status: "not_started" };

  if (typeof raw === "string") {
    const s = raw as string;
    if (s === "done" || s === "running" || s === "error") return { status: s };
    return { status: "running" };
  }

  const s = String(raw?.status || "running");
  const e = raw?.error ?? null;

  if (s === "done" || s === "running" || s === "error") return { status: s, error: e };
  return { status: "running", error: e };
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

  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const [downloading, setDownloading] = useState<AnyArtifactType | null>(null);

  const [selectedLang, setSelectedLang] = useState<(typeof LANGS)[number]["code"]>("hi");
  const [dubBusy, setDubBusy] = useState(false);

  const pollTimer = useRef<number | null>(null);

  const canSubmit = useMemo(() => ytUrl.trim().length > 0 && !busy, [ytUrl, busy]);

  function clearPoll() {
    if (pollTimer.current) window.clearTimeout(pollTimer.current);
    pollTimer.current = null;
  }

  async function getAccessToken(): Promise<string | null> {
    const { data } = await supabase.auth.getSession();
    return data.session?.access_token ?? null;
  }

  async function createJob() {
    setMsg(null);
    setBusy(true);
    setDubBusy(false);
    setJob(null);
    setJobId(null);
    clearPoll();

    try {
      const token = await getAccessToken();
      if (!token) {
        setMsg("Please sign in first.");
        setBusy(false);
        return;
      }

      const res = await fetch("/api/clip/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ url: ytUrl.trim() }),
      });

      const text = await res.text();
      let data: any = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        // keep as {}
      }

      if (!res.ok) {
        const errMsg = data?.error || text || `Request failed (${res.status})`;
        throw new Error(errMsg);
      }

      const id: string | undefined = data?.jobId ?? data?.id;
      if (!id) throw new Error("Server did not return a job id");

      setJobId(id);
      setMsg("Job created. Processing...");

      onJobCreated?.();
      onCreditsChanged?.();
    } catch (e: any) {
      setMsg(e?.message || "Failed to create job");
      setBusy(false);
    }
  }

  async function pollOnce(id: string) {
    const token = await getAccessToken();
    if (!token) {
      setMsg("Session expired. Please sign in again.");
      setBusy(false);
      return;
    }

    const res = await fetch(`/api/clip/status?jobId=${encodeURIComponent(id)}`, {
      cache: "no-store",
      headers: { Authorization: `Bearer ${token}` },
    });

    const text = await res.text();
    let data: any = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      // ignore
    }

    if (!res.ok) {
      const errMsg = data?.error || text || `Request failed (${res.status})`;
      throw new Error(errMsg);
    }

    return data as Job;
  }

  useEffect(() => {
    if (!jobId) return;

    let alive = true;

    async function loop() {
      try {
        const data = await pollOnce(jobId);
        if (!alive || !data) return;

        setJob(data);

        if (data.status === "done") {
          setBusy(false);
          setMsg("Done. Download artifacts below. You can also start a dub.");
          clearPoll();
          return;
        }

        if (data.status === "error") {
          setBusy(false);
          setMsg(data.error || "Job failed");
          clearPoll();
          return;
        }

        pollTimer.current = window.setTimeout(loop, 2000);
      } catch (e: any) {
        if (!alive) return;
        setMsg(e?.message || "Polling failed");
        setBusy(false);
        clearPoll();
      }
    }

    loop();

    return () => {
      alive = false;
      clearPoll();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

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
    let data: any = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      // ignore
    }

    if (!res.ok) {
      const errMsg = data?.error || text || `Request failed (${res.status})`;
      throw new Error(errMsg);
    }

    const url = String(data?.url || "").trim();
    if (!url) throw new Error("No download URL returned");

    window.open(url, "_blank", "noopener,noreferrer");
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
    setDubBusy(true);

    try {
      const token = await getAccessToken();
      if (!token) throw new Error("Please sign in first.");

      const res = await fetch("/api/clip/dub", {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ jobId, lang }),
      });

      const text = await res.text();
      let data: any = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        // ignore
      }

      if (!res.ok) {
        const errMsg = data?.error || text || `Request failed (${res.status})`;
        throw new Error(errMsg);
      }

      setMsg(`Dub started for ${lang.toUpperCase()}.`);
      onCreditsChanged?.();

      // kick a quicker poll cycle
      setBusy(true);
      clearPoll();
      // poll effect will continue
    } catch (e: any) {
      setMsg(e?.message || "Dub start failed");
    } finally {
      setDubBusy(false);
    }
  }

  async function downloadDub(kind: "video" | "audio" | "log") {
    if (!jobId) return;
    const type = (`dub_${kind}` as DubArtifactType);

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

  const dubState = readDubStatus(job, selectedLang);

  return (
    <div className="w-full">
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
          {busy ? "Working..." : "Create job"}
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

          {job.status === "done" && (
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
                    onChange={(e) => setSelectedLang(e.target.value as any)}
                    disabled={dubBusy}
                  >
                    {LANGS.map((l) => (
                      <option key={l.code} value={l.code}>
                        {l.label} ({l.code})
                      </option>
                    ))}
                  </select>

                  <button
                    onClick={() => startDub(selectedLang)}
                    disabled={dubBusy || !!downloading}
                    className="rounded-md px-4 py-2 bg-black text-white disabled:opacity-50"
                  >
                    {dubBusy ? "Starting..." : `Start dub (${selectedLang})`}
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
