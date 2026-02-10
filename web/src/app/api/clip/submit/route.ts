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

async function reserveJobCredits(sb: ReturnType<typeof supabaseAuthed>, workerJobId: string, amount: number) {
  // Prefer reserve_job_credits if present, fallback to reserve_credits
  const r1 = await sb.rpc("reserve_job_credits", { worker_job_id: workerJobId, amount });
  if (!r1.error) return { ok: true as const, method: "job" as const };

  const msg = (r1.error.message || "").toLowerCase();
  const fnMissing = msg.includes("could not find the function") || msg.includes("pgrst202");
  if (!fnMissing) return { ok: false as const, method: "job" as const, error: r1.error };

  const r2 = await sb.rpc("reserve_credits", { amount });
  if (!r2.error) return { ok: true as const, method: "org" as const };

  return { ok: false as const, method: "org" as const, error: r2.error };
}

async function refundCredits(
  sb: ReturnType<typeof supabaseAuthed>,
  workerJobId: string,
  amount: number,
  method: "job" | "org"
) {
  try {
    if (method === "job") {
      const r1 = await sb.rpc("refund_job_credits", { worker_job_id: workerJobId, amount });
      if (!r1.error) return;
    }
    await sb.rpc("refund_credits", { amount });
  } catch {
    // ignore
  }
}

export async function POST(req: Request) {
  const COST = 1;

  let workerJobId = "";
  let reservedMethod: "job" | "org" | null = null;

  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json();
    const url = String(body?.url || "").trim();
    if (!url) return NextResponse.json({ error: "Missing url" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const userId = u.user.id;
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

    const raw = await r.text();
    let worker: any = null;
    try {
      worker = JSON.parse(raw);
    } catch {
      worker = { raw };
    }

    workerJobId = String(worker?.jobId || worker?.id || "").trim();
    if (!r.ok || !workerJobId) {
      return NextResponse.json(
        { error: "Worker create job failed", workerStatus: r.status, workerBody: worker },
        { status: 502 }
      );
    }

    // Insert job row first so UI can show it even if credits fail
    const { error: insErr } = await sb.from("user_jobs").insert({
      user_id: userId,
      org_id: orgId,
      youtube_url: url,
      worker_job_id: workerJobId,
      status: "submitted",
      credits_spent: 0,
    });

    if (insErr) return NextResponse.json({ error: insErr.message }, { status: 500 });

    // Reserve credits
    const res = await reserveJobCredits(sb, workerJobId, COST);
    if (!res.ok) {
      const msg = (res.error?.message || "").toLowerCase().includes("insufficient")
        ? "Insufficient credits"
        : res.error?.message || "Credit reserve failed";

      await sb.from("user_jobs").update({ status: "error" }).eq("worker_job_id", workerJobId).eq("org_id", orgId);

      return NextResponse.json({ error: msg, workerJobId }, { status: 402 });
    }

    reservedMethod = res.method;

    // Update credits_spent now that reserve succeeded
    const { error: upErr } = await sb
      .from("user_jobs")
      .update({ credits_spent: COST })
      .eq("worker_job_id", workerJobId)
      .eq("org_id", orgId);

    if (upErr) {
      await refundCredits(sb, workerJobId, COST, reservedMethod);
      return NextResponse.json({ error: upErr.message }, { status: 500 });
    }

    return NextResponse.json({ jobId: workerJobId, orgId });
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";

    try {
      const token = getBearer(req);
      if (token && workerJobId && reservedMethod) {
        const sb = supabaseAuthed(token);
        await refundCredits(sb, workerJobId, COST, reservedMethod);
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
