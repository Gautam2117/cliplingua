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

// For safety: refund reserved credits if something fails after reserving
async function safeRefundJob(sb: ReturnType<typeof supabaseAuthed>, workerJobId: string, amount: number) {
  try {
    await sb.rpc("refund_job_credits", { worker_job_id: workerJobId, amount });
  } catch {
    // ignore
  }
}

async function ensureActiveOrg(sb: ReturnType<typeof supabaseAuthed>, userId: string) {
  const { data: prof, error: profErr } = await sb
    .from("profiles")
    .select("active_org_id")
    .eq("id", userId)
    .single();

  if (profErr) throw new Error(`Failed to load profile: ${profErr.message}`);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { data: boot, error: bootErr } = await sb.rpc("bootstrap_org");
    if (bootErr) throw new Error(`bootstrap_org failed: ${bootErr.message}`);
    orgId = String(boot || "").trim();
    if (!orgId) throw new Error("bootstrap_org returned empty org id");
  }

  return orgId;
}

export async function POST(req: Request) {
  const COST = 1;

  // We reserve credits AFTER we have workerJobId so we can tie reservation to job
  let workerJobId = "";
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

    // Ensure org exists (auto bootstrap)
    const orgId = await ensureActiveOrg(sb, u.user.id);

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

    // Reserve credits against this specific worker job (org-level)
    const { error: reserveErr } = await sb.rpc("reserve_job_credits", {
      worker_job_id: workerJobId,
      amount: COST,
    });

    if (reserveErr) {
      const msg = reserveErr.message?.toLowerCase().includes("insufficient")
        ? "Insufficient credits"
        : reserveErr.message;

      // job exists in worker but credits failed -> respond 402
      // You can optionally cancel worker job here, but not necessary
      return NextResponse.json({ error: msg, workerJobId }, { status: 402 });
    }

    reserved = true;

    // Insert job row WITH org_id
    const { error: insErr } = await sb.from("user_jobs").insert({
      user_id: u.user.id,
      org_id: orgId,
      youtube_url: url,
      worker_job_id: workerJobId,
      status: "submitted",
      credits_spent: COST,
    });

    if (insErr) {
      if (reserved) await safeRefundJob(sb, workerJobId, COST);
      return NextResponse.json({ error: insErr.message }, { status: 500 });
    }

    return NextResponse.json({ jobId: workerJobId, orgId });
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";

    // If we reserved for a specific worker job and something blew up, refund it
    // (Only if we already have workerJobId)
    // Best-effort.
    try {
      const token = getBearer(req);
      if (token && workerJobId && reserved) {
        const sb = supabaseAuthed(token);
        await safeRefundJob(sb, workerJobId, COST);
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
