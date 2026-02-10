export const runtime = "nodejs";

import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-admin";
import { requireApiKey } from "@/lib/api-key-auth";

function workerBase() {
  const base =
    process.env.WORKER_URL ||
    process.env.NEXT_PUBLIC_WORKER_URL ||
    process.env.CLIPLINGUA_WORKER_URL;
  if (!base) throw new Error("Missing WORKER_URL");
  return base.replace(/\/+$/, "");
}

async function createWorkerJob(url: string) {
  const res = await fetch(`${workerBase()}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(j?.detail || j?.error || `Worker error ${res.status}`);
  return j?.jobId as string;
}

export async function POST(req: Request) {
  try {
    const auth = await requireApiKey(req);

    const body = await req.json().catch(() => ({}));
    const urls: string[] = Array.isArray(body?.urls) ? body.urls : [];

    const cleaned = urls.map((u) => String(u || "").trim()).filter(Boolean);
    if (cleaned.length === 0) {
      return NextResponse.json({ error: "urls[] required" }, { status: 400 });
    }
    if (cleaned.length > 100) {
      return NextResponse.json({ error: "Too many urls (max 100 per call)" }, { status: 400 });
    }

    // 1) API limits (atomic)
    const { error: limErr } = await supabaseAdmin.rpc("consume_api", {
      p_org_id: auth.orgId,
      p_jobs: cleaned.length,
    });
    if (limErr) {
      const msg = limErr.message || "API limit exceeded";
      return NextResponse.json({ error: msg }, { status: 429 });
    }

    // 2) Charge credits upfront (refund failures later)
    const { error: chargeErr } = await supabaseAdmin.rpc("charge_org_credits", {
      p_org_id: auth.orgId,
      p_amount: cleaned.length,
    });
    if (chargeErr) {
      return NextResponse.json({ error: chargeErr.message || "Insufficient credits" }, { status: 402 });
    }

    const results: Array<{ url: string; ok: boolean; jobId?: string; error?: string }> = [];
    let failed = 0;

    for (const url of cleaned) {
      try {
        const workerJobId = await createWorkerJob(url);

        const { error: insErr } = await supabaseAdmin.from("user_jobs").insert({
          org_id: auth.orgId,
          user_id: null,
          youtube_url: url,
          worker_job_id: workerJobId,
          status: "queued",
          credits_spent: 1,
          created_via: "api",
          api_key_prefix: auth.prefix,
        });

        if (insErr) throw new Error(insErr.message);

        results.push({ url, ok: true, jobId: workerJobId });
      } catch (e: any) {
        failed += 1;
        results.push({ url, ok: false, error: e?.message || "Failed" });
      }
    }

    // refund failed credits once
    if (failed > 0) {
      await supabaseAdmin.rpc("refund_org_credits", {
        p_org_id: auth.orgId,
        p_amount: failed,
      });
    }

    return NextResponse.json({
      ok: true,
      created: cleaned.length - failed,
      failed,
      results,
    });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Unauthorized" }, { status: 401 });
  }
}
