"use client";

import { useEffect, useMemo, useState } from "react";
import { supabase } from "@/lib/supabase";

type Job = {
  id: string;
  url: string;
  status: "queued" | "running" | "done" | "error";
  error: string | null;

  // local paths (worker-side)
  video_path: string | null;
  audio_path: string | null;
  log_path: string | null;

  // URLs (if worker PUBLIC_BASE_URL set, else null)
  video_url?: string | null;
  audio_url?: string | null;
  log_url?: string | null;

  created_at: string;
  updated_at: string;
};

type ArtifactType = "video" | "audio" | "log";

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
  const [downloading, setDownloading] = useState<ArtifactType | null>(null);

  const canSubmit = useMemo(() => ytUrl.trim().length > 0 && !busy, [ytUrl, busy]);

  async function createJob() {
    setMsg(null);
    setBusy(true);
    setJob(null);
    setJobId(null);

    try {
      const { data: sess } = await supabase.auth.getSession();
      const token = sess.session?.access_token;

      if (!token) {
        setMsg("Please sign in first.");
        setBusy(false);
        return;
      }

      const res = await fetch("/api/clip/submit", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ url: ytUrl.trim() }),
      });

      const text = await res.text();
      if (!res.ok) throw new Error(text || `Request failed (${res.status})`);

      const data = JSON.parse(text) as any;
      const id: string | undefined = data?.jobId ?? data?.id;
      if (!id) throw new Error("Worker did not return a job id");

      setJobId(id);
      setMsg("Job created. Processing...");

      if (onJobCreated) onJobCreated();
      if (onCreditsChanged) onCreditsChanged();
    } catch (e: any) {
      setMsg(e?.message || "Failed to create job");
      setBusy(false);
    }
  }

  useEffect(() => {
    if (!jobId) return;
    const id = jobId;

    let alive = true;

    async function tick() {
      try {
        const res = await fetch(`/api/clip/status?jobId=${encodeURIComponent(id)}`, {
          cache: "no-store",
        });

        const text = await res.text();
        if (!res.ok) throw new Error(text || `Request failed (${res.status})`);

        const data = JSON.parse(text) as Job;

        if (!alive) return;
        setJob(data);

        if (data.status === "done") {
          setBusy(false);
          setMsg("Done. Download artifacts below.");
          return;
        }
        if (data.status === "error") {
          setBusy(false);
          setMsg(data.error || "Job failed");
          return;
        }

        setTimeout(tick, 2000);
      } catch (e: any) {
        if (!alive) return;
        setMsg(e?.message || "Polling failed");
        setBusy(false);
      }
    }

    tick();
    return () => {
      alive = false;
    };
  }, [jobId]);

  async function download(type: ArtifactType) {
    if (!jobId) return;

    setMsg(null);
    setDownloading(type);

    try {
      const res = await fetch(
        `/api/clip/artifact?jobId=${encodeURIComponent(jobId)}&type=${encodeURIComponent(type)}`,
        { cache: "no-store" }
      );

      const text = await res.text();
      if (!res.ok) throw new Error(text || `Request failed (${res.status})`);

      const data = JSON.parse(text) as { url: string };
      if (!data?.url) throw new Error("No download URL returned");

      window.open(data.url, "_blank", "noopener,noreferrer");
    } catch (e: any) {
      setMsg(e?.message || "Download failed");
    } finally {
      setDownloading(null);
    }
  }

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
            <div className="mt-4 flex flex-wrap gap-2">
              <button
                onClick={() => download("video")}
                className="border rounded-md px-4 py-2 disabled:opacity-60"
                disabled={!!downloading}
              >
                {downloading === "video" ? "Preparing..." : "Download video"}
              </button>

              <button
                onClick={() => download("audio")}
                className="border rounded-md px-4 py-2 disabled:opacity-60"
                disabled={!!downloading}
              >
                {downloading === "audio" ? "Preparing..." : "Download audio"}
              </button>

              <button
                onClick={() => download("log")}
                className="border rounded-md px-4 py-2 disabled:opacity-60"
                disabled={!!downloading}
              >
                {downloading === "log" ? "Preparing..." : "Download log"}
              </button>
            </div>
          )}
        </div>
      )}

      {msg && <p className="mt-3 text-sm opacity-80 whitespace-pre-wrap">{msg}</p>}
    </div>
  );
}
