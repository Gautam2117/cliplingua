import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 120;

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

async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs: number) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
}

async function safeRefund(sb: ReturnType<typeof supabaseAuthed>, amount: number) {
  try {
    await sb.rpc("refund_credits", { amount });
  } catch {
    // ignore
  }
}

export async function POST(req: Request) {
  const COST = 1;
  let reserved = false;

  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json();
    const url = String(body?.url || "").trim();
    if (!url) return NextResponse.json({ error: "Missing url" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const { error: reserveErr } = await sb.rpc("reserve_credits", { amount: COST });
    if (reserveErr) {
      const msg = reserveErr.message?.toLowerCase().includes("insufficient")
        ? "Insufficient credits"
        : reserveErr.message;
      return NextResponse.json({ error: msg }, { status: 402 });
    }
    reserved = true;

    const workerBase = getWorkerBaseUrl();

    // warmup worker (Render cold start)
    try {
      await fetchWithTimeout(`${workerBase}/health`, { cache: "no-store" }, 10_000);
    } catch {
      // ignore
    }

    const r = await fetchWithTimeout(
      `${workerBase}/jobs`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ url }),
        cache: "no-store",
      },
      90_000
    );

    const text = await r.text();
    let worker: any = null;
    try {
      worker = JSON.parse(text);
    } catch {
      worker = { raw: text };
    }

    // robust: accept either jobId or id in case worker changes
    const workerJobId = String(worker?.jobId || worker?.id || "").trim();

    if (!r.ok || !workerJobId) {
      if (reserved) await safeRefund(sb, COST);
      return NextResponse.json(
        { error: "Worker create job failed", workerStatus: r.status, workerBody: worker },
        { status: 502 }
      );
    }

    const { error: insErr } = await sb.from("user_jobs").insert({
      user_id: u.user.id,
      youtube_url: url,
      worker_job_id: workerJobId,
      status: "submitted",
      credits_spent: COST,
    });

    if (insErr) {
      if (reserved) await safeRefund(sb, COST);
      return NextResponse.json({ error: insErr.message }, { status: 500 });
    }

    return NextResponse.json({ jobId: workerJobId });
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";
    return NextResponse.json({ error: isAbort ? "Worker timeout" : e?.message || "Submit failed" }, { status: 500 });
  }
}
