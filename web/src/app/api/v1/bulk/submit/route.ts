import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-admin";

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

function getApiKey(req: Request) {
  return (req.headers.get("x-api-key") || "").trim() || null;
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

export async function POST(req: Request) {
  const COST_PER_JOB = 1;

  try {
    const apiKey = getApiKey(req);
    if (!apiKey) return NextResponse.json({ error: "Missing x-api-key" }, { status: 401 });

    const body = await req.json();
    const urls: string[] = Array.isArray(body?.urls) ? body.urls : [];
    const concurrency = Math.min(Math.max(Number(body?.concurrency || 3), 1), 6);

    const clean = urls.map((x) => String(x || "").trim()).filter(Boolean);
    if (clean.length === 0) return NextResponse.json({ error: "Provide urls: string[]" }, { status: 400 });
    if (clean.length > 50) return NextResponse.json({ error: "Max 50 URLs per request" }, { status: 400 });

    // 1) Verify key (also checks Agency gating)
    const v = await supabaseAdmin.rpc("verify_org_api_key", { api_key: apiKey });
    if (v.error) return NextResponse.json({ error: v.error.message }, { status: 401 });

    const row = Array.isArray(v.data) ? v.data[0] : v.data;
    const orgId = String(row?.org_id || "").trim();
    const prefix = String(row?.prefix || "").trim();
    const apiRpm = Number(row?.api_rpm || 30);
    const apiDaily = Number(row?.api_daily_cap || 2000);

    if (!orgId || !prefix) return NextResponse.json({ error: "Invalid key verify result" }, { status: 401 });

    // 2) Rate limit (per key)
    const lim = await supabaseAdmin.rpc("consume_api_limit", {
      p_org_id: orgId,
      p_prefix: prefix,
      p_rpm: apiRpm,
      p_daily: apiDaily,
    });
    if (lim.error) {
      const msg = lim.error.message || "Rate limit exceeded";
      return NextResponse.json({ error: msg }, { status: 429 });
    }

    const workerBase = getWorkerBaseUrl();

    const results: any[] = [];
    let idx = 0;

    async function runOne(url: string) {
      let workerJobId = "";

      // Create worker job
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
      let parsed: any = null;
      try {
        parsed = JSON.parse(raw);
      } catch {
        parsed = { raw };
      }

      workerJobId = String(parsed?.jobId || parsed?.id || "").trim();
      if (!r.ok || !workerJobId) {
        return { url, ok: false, error: "Worker create failed", workerStatus: r.status, workerBody: parsed };
      }

      // Reserve credits on org
      const resv = await supabaseAdmin.rpc("reserve_org_credits", { p_org_id: orgId, amount: COST_PER_JOB });
      if (resv.error) {
        const msg = resv.error.message?.toLowerCase().includes("insufficient") ? "Insufficient credits" : resv.error.message;
        return { url, ok: false, error: msg, workerJobId, status: 402 };
      }

      // Insert user_jobs row (user_id can be null for API calls if your schema allows it)
      const ins = await supabaseAdmin.from("user_jobs").insert({
        org_id: orgId,
        youtube_url: url,
        worker_job_id: workerJobId,
        status: "submitted",
        credits_spent: COST_PER_JOB,
      });

      if (ins.error) {
        await supabaseAdmin.rpc("refund_org_credits", { p_org_id: orgId, amount: COST_PER_JOB });
        return { url, ok: false, error: ins.error.message, workerJobId };
      }

      return { url, ok: true, workerJobId };
    }

    async function workerLoop() {
      while (idx < clean.length) {
        const current = clean[idx++];
        try {
          const res = await runOne(current);
          results.push(res);
        } catch (e: any) {
          results.push({ url: current, ok: false, error: e?.message || "Failed" });
        }
      }
    }

    const workers = Array.from({ length: Math.min(concurrency, clean.length) }, () => workerLoop());
    await Promise.all(workers);

    return NextResponse.json({ ok: true, orgId, count: clean.length, results });
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";
    return NextResponse.json({ error: isAbort ? "Worker timeout" : e?.message || "Bulk submit failed" }, { status: 500 });
  }
}
