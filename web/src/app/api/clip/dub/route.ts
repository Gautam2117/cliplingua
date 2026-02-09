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
  const { error: e1 } = await sb.rpc("reserve_job_credits", { worker_job_id: workerJobId, amount });
  if (!e1) return { method: "job" as const };

  const msg = (e1.message || "").toLowerCase();
  const fnMissing = msg.includes("could not find the function") || msg.includes("pgrst202");
  if (!fnMissing) return { method: "job" as const, error: e1 };

  const { error: e2 } = await sb.rpc("reserve_credits", { amount });
  if (!e2) return { method: "org" as const };

  return { method: "org" as const, error: e2 };
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

  const token = getBearer(req);
  if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

  let reserved: { method: "job" | "org" } | null = null;

  try {
    const body = await req.json();
    const jobId = String(body?.jobId || "").trim();
    const lang = String(body?.lang || "").trim();
    const captionStyle = String(body?.captionStyle || "clean").trim();

    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });
    if (!lang) return NextResponse.json({ error: "Missing lang" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const orgId = await getActiveOrgId(sb, u.user.id);

    // Safety: ensure this job belongs to the active org
    const { data: jobRow, error: jobErr } = await sb
      .from("user_jobs")
      .select("worker_job_id")
      .eq("org_id", orgId)
      .eq("worker_job_id", jobId)
      .single();

    if (jobErr || !jobRow) {
      return NextResponse.json({ error: "Job not found in your org" }, { status: 404 });
    }

    const res = await reserveCredits(sb, jobId, COST);
    if ((res as any).error) {
      const e = (res as any).error;
      const msg = (e.message || "").toLowerCase().includes("insufficient") ? "Insufficient credits" : e.message;
      return NextResponse.json({ error: msg }, { status: 402 });
    }
    reserved = { method: res.method };

    const base = getWorkerBaseUrl();
    const r = await fetch(`${base}/jobs/${encodeURIComponent(jobId)}/dub`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ lang, captionStyle }),
      cache: "no-store",
      signal: AbortSignal.timeout(90_000),
    });

    const text = await r.text().catch(() => "");
    let payload: any = null;
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      payload = { raw: text };
    }

    if (!r.ok) {
      await refundCredits(sb, jobId, COST, reserved.method);
      return NextResponse.json(
        { error: "Worker dub failed", workerStatus: r.status, workerBody: payload },
        { status: 502 }
      );
    }

    return NextResponse.json({ ok: true, jobId, lang, ...payload });
  } catch (e: any) {
    // refund only if reserved and we can
    try {
      if (reserved) {
        const sb = supabaseAuthed(token);
        // best effort: we do not know jobId here safely unless request parsing succeeded
      }
    } catch {
      // ignore
    }
    return NextResponse.json({ error: e?.message || "Dub failed" }, { status: 500 });
  }
}
