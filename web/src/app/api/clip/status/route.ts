import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getWorkerBaseUrl() {
  const base =
    process.env.WORKER_BASE_URL?.trim() ||
    process.env.NEXT_PUBLIC_WORKER_BASE_URL?.trim() ||
    process.env.WORKER_URL?.trim() ||
    "";
  if (!base) throw new Error("WORKER_BASE_URL not set");
  return base.replace(/\/+$/, "");
}

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

export async function GET(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const { data: row } = await sb
      .from("user_jobs")
      .select("worker_job_id,status")
      .eq("worker_job_id", jobId)
      .limit(1)
      .maybeSingle();

    if (!row) return NextResponse.json({ error: "job not found" }, { status: 404 });

    const base = getWorkerBaseUrl();
    const r = await fetch(`${base}/jobs/${encodeURIComponent(jobId)}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(25_000),
    });

    const text = await r.text();
    if (!r.ok) return new NextResponse(text, { status: r.status });

    const workerJob = JSON.parse(text);

    // best effort: keep DB status synced
    try {
      if (typeof workerJob?.status === "string" && workerJob.status !== row.status) {
        await sb.from("user_jobs").update({ status: workerJob.status }).eq("worker_job_id", jobId);
      }
    } catch {
      // ignore
    }

    return NextResponse.json(workerJob);
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";
    return NextResponse.json({ error: isAbort ? "Worker timeout" : e?.message || "Status failed" }, { status: 500 });
  }
}
