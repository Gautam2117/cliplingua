// src/app/api/clip/submit/route.ts
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

async function getActiveOrgId(sb: ReturnType<typeof supabaseAuthed>, userId: string) {
  const { data: prof, error: profErr } = await sb
    .from("profiles")
    .select("active_org_id")
    .eq("id", userId)
    .single();

  if (profErr) throw new Error(`Failed to load profile: ${profErr.message}`);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { error: bootErr } = await sb.rpc("bootstrap_org");
    if (bootErr) throw new Error(`bootstrap_org failed: ${bootErr.message}`);

    const { data: prof2, error: profErr2 } = await sb
      .from("profiles")
      .select("active_org_id")
      .eq("id", userId)
      .single();

    if (profErr2) throw new Error(`Failed to reload profile: ${profErr2.message}`);

    orgId = (prof2 as any)?.active_org_id as string | null;
  }

  if (!orgId) throw new Error("active_org_id missing after bootstrap");
  return orgId;
}

async function reserveCredits(sb: ReturnType<typeof supabaseAuthed>, workerJobId: string, amount: number) {
  // Prefer job-tied RPC (org-level). If missing, fallback to reserve_credits (org-aware in your Day6 SQL).
  const r1 = await sb.rpc("reserve_job_credits", { worker_job_id: workerJobId, amount });
  if (!r1.error) return { method: "job" as const };

  const msg = (r1.error.message || "").toLowerCase();
  const fnMissing = msg.includes("could not find the function") || msg.includes("pgrst202");
  if (!fnMissing) return { method: "job" as const, error: r1.error };

  const r2 = await sb.rpc("reserve_credits", { amount });
  if (!r2.error) return { method: "org" as const };

  return { method: "org" as const, error: r2.error };
}

async function refundCredits(
  sb: ReturnType<typeof supabaseAuthed>,
  workerJobId: string,
  amount: number,
  method: "job" | "org"
) {
  try {
    if (method === "job") {
      await sb.rpc("refund_job_credits", { worker_job_id: workerJobId, amount });
    } else {
      await sb.rpc("refund_credits", { amount });
    }
  } catch {
    // ignore
  }
}

export async function POST(req: Request) {
  const COST = 1;

  let workerJobId = "";
  let reserved: { method: "job" | "org" } | null = null;

  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json();
    const url = String(body?.url || "").trim();
    if (!url) return NextResponse.json({ error: "Missing url" }, { status: 400 });

    const sb = supabaseAuthed(token);

    // IMPORTANT: store user in const so TS narrowing persists
    const { data: userRes, error: uErr } = await sb.auth.getUser();
    const user = userRes.user;
    if (uErr || !user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const userId = user.id;
    const orgId = await getActiveOrgId(sb, userId);
    const workerBase = getWorkerBaseUrl();

    // Warmup worker (Render cold start)
    try {
      await fetchWithTimeout(`${workerBase}/health`, { cache: "no-store" }, 10_000);
    } catch {
      // ignore
    }

    // Create worker job first
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

    workerJobId = String(worker?.jobId || worker?.id || "").trim();

    if (!r.ok || !workerJobId) {
      return NextResponse.json(
        { error: "Worker create job failed", workerStatus: r.status, workerBody: worker },
        { status: 502 }
      );
    }

    // Insert job row first with credits_spent = 0 (so it appears even if credits fail)
    const { error: insErr } = await sb.from("user_jobs").insert({
      user_id: userId,
      org_id: orgId,
      youtube_url: url,
      worker_job_id: workerJobId,
      status: "submitted",
      credits_spent: 0,
    });

    if (insErr) {
      return NextResponse.json({ error: insErr.message }, { status: 500 });
    }

    // Reserve credits tied to workerJobId (or fallback to org-aware reserve_credits)
    const res = await reserveCredits(sb, workerJobId, COST);
    if ((res as any).error) {
      const e = (res as any).error;
      const msg = (e.message || "").toLowerCase().includes("insufficient") ? "Insufficient credits" : e.message;

      await sb
        .from("user_jobs")
        .update({ status: "error" })
        .eq("worker_job_id", workerJobId)
        .eq("org_id", orgId);

      return NextResponse.json({ error: msg, workerJobId }, { status: 402 });
    }

    reserved = { method: res.method };

    // Update credits_spent now that reservation succeeded
    const { error: upErr } = await sb
      .from("user_jobs")
      .update({ credits_spent: COST })
      .eq("worker_job_id", workerJobId)
      .eq("org_id", orgId);

    if (upErr) {
      await refundCredits(sb, workerJobId, COST, reserved.method);
      return NextResponse.json({ error: upErr.message }, { status: 500 });
    }

    return NextResponse.json({ jobId: workerJobId, orgId });
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";

    // Best-effort refund if we already reserved
    try {
      const token = getBearer(req);
      if (token && workerJobId && reserved) {
        const sb = supabaseAuthed(token);
        await refundCredits(sb, workerJobId, COST, reserved.method);
      }
    } catch {
      // ignore
    }

    return NextResponse.json(
      { error: isAbort ? "Worker timeout" : e?.message || "Submit failed" },
      { status: 500 }
    );
  }
}
