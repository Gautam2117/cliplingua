"use client";

import { useEffect, useState } from "react";
import WaitlistForm from "@/components/WaitlistForm";

export default function Home() {
  const [url, setUrl] = useState("");
  const [jobId, setJobId] = useState<string | null>(null);
  const [job, setJob] = useState<any>(null);
  const [loading, setLoading] = useState(false);

  async function submit() {
    setLoading(true);
    setJob(null);
    setJobId(null);

    const r = await fetch("/api/clip/submit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
    });

    const data = await r.json();
    setLoading(false);

    if (!r.ok) {
      alert(data?.error || "submit failed");
      return;
    }

    setJobId(data.jobId);
  }

  useEffect(() => {
    if (!jobId) return;

    const t = setInterval(async () => {
      const r = await fetch(`/api/clip/status?jobId=${encodeURIComponent(jobId)}`, {
        cache: "no-store",
      });
      const data = await r.json();
      setJob(data);
      if (data?.status === "done" || data?.status === "error") clearInterval(t);
    }, 2000);

    return () => clearInterval(t);
  }, [jobId]);

  return (
    <main className="min-h-screen flex items-center justify-center px-6 py-14">
      <div className="max-w-2xl w-full">
        <p className="text-sm uppercase tracking-wider opacity-70">ClipLingua</p>

        <h1 className="text-4xl md:text-5xl font-bold mt-3 leading-tight">
          Turn any video into Shorts in 25 languages.
        </h1>

        <p className="mt-4 text-lg opacity-80">
          Paste a YouTube link. We download and extract audio now. Next we clip, dub, and burn captions.
        </p>

        <div className="mt-8 border rounded-xl p-4">
          <div className="flex gap-2">
            <input
              className="border rounded-md px-4 py-3 flex-1"
              placeholder="https://www.youtube.com/watch?v=..."
              value={url}
              onChange={(e) => setUrl(e.target.value)}
            />
            <button
              onClick={submit}
              disabled={loading}
              className="rounded-md px-5 py-3 bg-black text-white disabled:opacity-50"
            >
              {loading ? "Submitting..." : "Run"}
            </button>
          </div>

          {jobId && (
            <div className="mt-3 text-sm opacity-80">
              Job: <span className="font-mono">{jobId}</span>
            </div>
          )}

          {job?.status && (
            <div className="mt-3 text-sm">
              Status: <span className="font-semibold">{job.status}</span>
              {job.status === "done" && job.artifacts?.audio_path && (
                <div className="mt-2 opacity-80">
                  Audio extracted on worker: <span className="font-mono">{job.artifacts.audio_path}</span>
                </div>
              )}
              {job.status === "error" && (
                <div className="mt-2 text-red-700">
                  Error: {job.error || "failed"}
                </div>
              )}
            </div>
          )}
        </div>

        <div className="mt-10">
          <p className="text-sm uppercase tracking-wider opacity-70">Waitlist</p>
          <div className="mt-3">
            <WaitlistForm />
          </div>
        </div>

        <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-3 text-sm opacity-80">
          <div className="border rounded-lg p-4">Auto-clip 15 to 60s</div>
          <div className="border rounded-lg p-4">Voice + captions</div>
          <div className="border rounded-lg p-4">Bulk exports for agencies</div>
        </div>
      </div>
    </main>
  );
}
