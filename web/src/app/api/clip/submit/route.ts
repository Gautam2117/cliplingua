import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";

function getWorkerBaseUrl() {
  const base =
    process.env.WORKER_BASE_URL?.trim() ||
    process.env.NEXT_PUBLIC_WORKER_BASE_URL?.trim() ||
    "";
  if (!base) throw new Error("Missing WORKER_BASE_URL (or NEXT_PUBLIC_WORKER_BASE_URL)");
  return base.replace(/\/+$/, "");
}

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

export async function POST(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const body = await req.json();
    const url = String(body?.url || "").trim();
    if (!url) return NextResponse.json({ error: "Missing url" }, { status: 400 });

    const sb = supabaseAuthed(token);

    // Ensure token is valid and get user id
    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) {
      return NextResponse.json({ error: "Invalid session" }, { status: 401 });
    }

    const COST = 1;

    // Reserve credits first (atomic)
    const { error: reserveErr } = await sb.rpc("reserve_credits", { amount: COST });
    if (reserveErr) {
      const msg =
        reserveErr.message?.toLowerCase().includes("insufficient")
          ? "Insufficient credits"
          : reserveErr.message;
      return NextResponse.json({ error: msg }, { status: 402 });
    }

    const workerBase = getWorkerBaseUrl();

    // Create job on worker
    const r = await fetch(`${workerBase}/jobs`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ url }),
      signal: AbortSignal.timeout(30_000),
      cache: "no-store",
    });

    const text = await r.text();
    let worker: any = null;
    try {
      worker = JSON.parse(text);
    } catch {
      worker = { raw: text };
    }

    if (!r.ok || !worker?.jobId) {
      // Refund if worker failed
      await sb.rpc("refund_credits", { amount: COST }).catch(() => null);
      return NextResponse.json(
        { error: "Worker create job failed", workerStatus: r.status, workerBody: worker },
        { status: 502 }
      );
    }

    const workerJobId = worker.jobId as string;

    // Insert job record owned by user (RLS enforced)
    const { error: insErr } = await sb.from("user_jobs").insert({
      user_id: u.user.id,
      youtube_url: url,
      worker_job_id: workerJobId,
      status: "submitted",
      credits_spent: COST,
    });

    if (insErr) {
      // Refund if DB insert failed
      await sb.rpc("refund_credits", { amount: COST }).catch(() => null);
      return NextResponse.json({ error: insErr.message }, { status: 500 });
    }

    return NextResponse.json({ jobId: workerJobId });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Submit failed" }, { status: 500 });
  }
}
