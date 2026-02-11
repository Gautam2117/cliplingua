import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function jsonError(message: string, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

function getWorkerBaseUrl() {
  const base =
    process.env.WORKER_BASE_URL?.trim() ||
    process.env.NEXT_PUBLIC_WORKER_BASE_URL?.trim() ||
    process.env.WORKER_URL?.trim() ||
    "";
  if (!base) return jsonError("WORKER_BASE_URL missing", 500);

  return base.replace(/\/+$/, "");
}

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal, cache: "no-store" });
  } finally {
    clearTimeout(t);
  }
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });

    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const sb = supabaseAuthed(token);
    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    // ensure the job belongs to this user (RLS + explicit filter)
    const { data: row, error: jErr } = await sb
      .from("user_jobs")
      .select("status")
      .eq("worker_job_id", jobId)
      .limit(1)
      .maybeSingle();

    if (jErr) return NextResponse.json({ error: jErr.message }, { status: 500 });
    if (!row) return NextResponse.json({ error: "job not found" }, { status: 404 });

    const workerBase = getWorkerBaseUrl();
    const r = await fetchWithTimeout(`${workerBase}/jobs/${encodeURIComponent(jobId)}`, {}, 20_000);

    const text = await r.text();
    let body: any = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { raw: text };
    }

    if (!r.ok) {
      const msg = body?.error || body?.detail || text || "Worker error";
      const status = r.status === 404 ? 404 : 502;
      return NextResponse.json({ error: msg, workerStatus: r.status }, { status });
    }

    // Update DB status (best-effort)
    const wStatus = String(body?.status || "");
    if (wStatus && wStatus !== row.status) {
      await sb.from("user_jobs").update({ status: wStatus }).eq("worker_job_id", jobId);
    }

    return NextResponse.json(body);
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Status failed" }, { status: 500 });
  }
}
