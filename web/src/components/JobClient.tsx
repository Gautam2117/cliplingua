"use client";

import { useEffect, useMemo, useState } from "react";

type Job = {
  id: string;
  url: string;
  status: "queued" | "running" | "done" | "error";
  error: string | null;
  video_path: string | null;
  audio_path: string | null;
  log_path: string | null;
  created_at: string;
  updated_at: string;
};

const WORKER_BASE_URL =
  process.env.NEXT_PUBLIC_WORKER_BASE_URL || process.env.WORKER_BASE_URL;

async function signArtifact(path: string): Promise<string> {
  const res = await fetch("/api/artifact", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path }),
  });
  if (!res.ok) throw new Error("Failed to sign url");
  const data = (await res.json()) as { url: string };
  return data.url;
}

export default function JobClient() {
  const [ytUrl, setYtUrl] = useState("");
  const [jobId, setJobId] = useState<string | null>(null);
  const [job, setJob] = useState<Job | null>(null);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const canSubmit = useMemo(() => ytUrl.trim().length > 0 && !busy, [ytUrl, busy]);

  async function createJob() {
    setMsg(null);
    setBusy(true);
    setJob(null);
    setJobId(null);

    try {
      const base = WORKER_BASE_URL || "https://cliplingua.onrender.com";
      const res = await fetch(`${base}/jobs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: ytUrl.trim() }),
      });

      if (!res.ok) {
        const t = await res.text();
        throw new Error(t || "Failed to create job");
      }

      const data = (await res.json()) as { jobId: string };
      setJobId(data.jobId);
      setMsg("Job created. Processing...");
    } catch (e: any) {
      setMsg(e?.message || "Something went wrong");
      setBusy(false);
    }
  }

  // poll
  useEffect(() => {
    if (!jobId) return;

    let alive = true;
    const base = WORKER_BASE_URL || "https://cliplingua.onrender.com";

    async function tick() {
      try {
        const res = await fetch(`${base}/jobs/${jobId}`, { cache: "no-store" });
        if (!res.ok) throw new Error("Failed to fetch job");
        const data = (await res.json()) as Job;

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

        // keep polling
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

  async function download(path: string) {
    setMsg(null);
    try {
      const url = await signArtifact(path);
      window.open(url, "_blank", "noopener,noreferrer");
    } catch (e: any) {
      setMsg(e?.message || "Failed to download");
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
            <p className="mt-2 text-sm text-red-700">{job.error}</p>
          )}

          {job.status === "done" && (
            <div className="mt-4 flex flex-wrap gap-2">
              {job.video_path && (
                <button
                  onClick={() => download(job.video_path!)}
                  className="border rounded-md px-4 py-2"
                >
                  Download video
                </button>
              )}
              {job.audio_path && (
                <button
                  onClick={() => download(job.audio_path!)}
                  className="border rounded-md px-4 py-2"
                >
                  Download audio
                </button>
              )}
              {job.log_path && (
                <button
                  onClick={() => download(job.log_path!)}
                  className="border rounded-md px-4 py-2"
                >
                  Download log
                </button>
              )}
            </div>
          )}
        </div>
      )}

      {msg && <p className="mt-3 text-sm opacity-80">{msg}</p>}
    </div>
  );
}
