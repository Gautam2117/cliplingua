import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

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

async function ensureActiveOrg(sb: ReturnType<typeof supabaseAuthed>, userId: string) {
  const { data: prof, error } = await sb
    .from("profiles")
    .select("active_org_id")
    .eq("id", userId)
    .single();

  if (error) throw new Error(error.message);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { data: boot, error: bootErr } = await sb.rpc("bootstrap_org");
    if (bootErr) throw new Error(bootErr.message);

    const maybeOrg = String(boot || "").trim();
    if (maybeOrg) orgId = maybeOrg;

    if (!orgId) {
      const { data: prof2, error: error2 } = await sb
        .from("profiles")
        .select("active_org_id")
        .eq("id", userId)
        .single();
      if (error2) throw new Error(error2.message);
      orgId = (prof2 as any)?.active_org_id as string | null;
    }
  }

  if (!orgId) throw new Error("active_org_id missing");
  return orgId;
}

async function safeRefund(sb: ReturnType<typeof supabaseAuthed>, workerJobId: string, amount: number) {
  try {
    await sb.rpc("refund_job_credits", { worker_job_id: workerJobId, amount });
  } catch {
    // ignore
  }
}

export async function POST(req: Request) {
  const COST = 1;

  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    // IMPORTANT: store in constants so nested functions do not lose TS narrowing
    const userId = u.user.id;

    const body = await req.json();
    const urls: string[] = Array.isArray(body?.urls) ? body.urls : [];
    const concurrency = Math.min(Math.max(Number(body?.concurrency || 3), 1), 6);

    const clean = urls.map((x) => String(x || "").trim()).filter(Boolean);

    if (clean.length === 0) return NextResponse.json({ error: "Provide urls: string[]" }, { status: 400 });
    if (clean.length > 50) return NextResponse.json({ error: "Max 50 URLs per request" }, { status: 400 });

    const orgId = await ensureActiveOrg(sb, userId);
    const workerBase = getWorkerBaseUrl();

    const results: any[] = [];
    let idx = 0;

    async function runOne(url: string) {
      let workerJobId = "";
      let reserved = false;

      try {
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

        const txt = await r.text();
        let parsed: any = null;
        try {
          parsed = JSON.parse(txt);
        } catch {
          parsed = { raw: txt };
        }

        workerJobId = String(parsed?.jobId || parsed?.id || "").trim();
        if (!r.ok || !workerJobId) {
          return { url, ok: false, error: "Worker create failed", workerStatus: r.status, workerBody: parsed };
        }

        const { error: reserveErr } = await sb.rpc("reserve_job_credits", {
          worker_job_id: workerJobId,
          amount: COST,
        });

        if (reserveErr) {
          const msg = reserveErr.message?.toLowerCase().includes("insufficient")
            ? "Insufficient credits"
            : reserveErr.message;
          return { url, ok: false, error: msg, workerJobId, status: 402 };
        }

        reserved = true;

        const { error: insErr } = await sb.from("user_jobs").insert({
          user_id: userId,
          org_id: orgId,
          youtube_url: url,
          worker_job_id: workerJobId,
          status: "submitted",
          credits_spent: COST,
        });

        if (insErr) {
          if (reserved) await safeRefund(sb, workerJobId, COST);
          return { url, ok: false, error: insErr.message, workerJobId };
        }

        return { url, ok: true, workerJobId };
      } catch (e: any) {
        if (workerJobId && reserved) await safeRefund(sb, workerJobId, COST);
        const isAbort = e?.name === "AbortError";
        return { url, ok: false, error: isAbort ? "Worker timeout" : e?.message || "Failed" };
      }
    }

    async function workerLoop() {
      while (idx < clean.length) {
        const current = clean[idx++];
        const res = await runOne(current);
        results.push(res);
      }
    }

    const workers = Array.from({ length: Math.min(concurrency, clean.length) }, () => workerLoop());
    await Promise.all(workers);

    return NextResponse.json({ ok: true, orgId, count: clean.length, results });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Bulk submit failed" }, { status: 500 });
  }
}
