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

async function startDub(jobId: string, lang: string, captionStyle: string) {
  const res = await fetch(`${workerBase()}/jobs/${jobId}/dub`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ lang, captionStyle }),
  });
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(j?.detail || j?.error || `Worker error ${res.status}`);
  return j;
}

export async function POST(req: Request) {
  try {
    const auth = await requireApiKey(req);

    const body = await req.json().catch(() => ({}));
    const items: Array<{ jobId: string; lang: string; captionStyle?: string }> =
      Array.isArray(body?.items) ? body.items : [];

    const cleaned = items
      .map((x) => ({
        jobId: String(x?.jobId || "").trim(),
        lang: String(x?.lang || "").trim().toLowerCase(),
        captionStyle: String(x?.captionStyle || "clean").trim(),
      }))
      .filter((x) => x.jobId && x.lang);

    if (cleaned.length === 0) {
      return NextResponse.json({ error: "items[] required" }, { status: 400 });
    }
    if (cleaned.length > 100) {
      return NextResponse.json({ error: "Too many items (max 100 per call)" }, { status: 400 });
    }

    // Verify ownership: jobIds must belong to org
    const ids = cleaned.map((x) => x.jobId);
    const { data: rows, error: qErr } = await supabaseAdmin
      .from("user_jobs")
      .select("worker_job_id, org_id")
      .in("worker_job_id", ids);

    if (qErr) return NextResponse.json({ error: qErr.message }, { status: 500 });

    const allowed = new Set((rows || []).filter((r) => r.org_id === auth.orgId).map((r) => r.worker_job_id));
    const forbidden = ids.filter((id) => !allowed.has(id));
    if (forbidden.length > 0) {
      return NextResponse.json({ error: "Some jobIds are not in this org", forbidden }, { status: 403 });
    }

    // Count dubs against API limits too (simple, works well)
    const { error: limErr } = await supabaseAdmin.rpc("consume_api", {
      p_org_id: auth.orgId,
      p_jobs: cleaned.length,
    });
    if (limErr) {
      return NextResponse.json({ error: limErr.message || "API limit exceeded" }, { status: 429 });
    }

    const results: Array<{ jobId: string; ok: boolean; error?: string }> = [];

    for (const it of cleaned) {
      try {
        await startDub(it.jobId, it.lang, it.captionStyle);
        results.push({ jobId: it.jobId, ok: true });
      } catch (e: any) {
        results.push({ jobId: it.jobId, ok: false, error: e?.message || "Failed" });
      }
    }

    return NextResponse.json({ ok: true, results });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Unauthorized" }, { status: 401 });
  }
}
