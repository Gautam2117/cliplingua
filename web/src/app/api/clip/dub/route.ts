import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 120;

type Body = {
  jobId?: string;
  lang?: string;
  captionStyle?: string;
};

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

function allowedLangs() {
  return new Set(["hi", "en", "es"]);
}

async function safeRefundCredits(sb: ReturnType<typeof supabaseAuthed>, amount: number) {
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

    const body = (await req.json().catch(() => ({}))) as Body;
    const jobId = String(body.jobId || "").trim();
    const lang = String(body.lang || "").trim();
    const captionStyle = String(body.captionStyle || "clean").trim();

    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });
    if (!lang) return NextResponse.json({ error: "Missing lang" }, { status: 400 });
    if (!allowedLangs().has(lang)) return NextResponse.json({ error: "Unsupported lang" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const { data: row, error: ownErr } = await sb
      .from("user_jobs")
      .select("worker_job_id")
      .eq("worker_job_id", jobId)
      .limit(1)
      .maybeSingle();

    if (ownErr) return NextResponse.json({ error: ownErr.message }, { status: 500 });
    if (!row) return NextResponse.json({ error: "job not found" }, { status: 404 });

    const { error: reserveErr } = await sb.rpc("reserve_credits", { amount: COST });
    if (reserveErr) {
      const msg = reserveErr.message?.toLowerCase().includes("insufficient")
        ? "Insufficient credits"
        : reserveErr.message;
      return NextResponse.json({ error: msg }, { status: 402 });
    }
    reserved = true;

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
      if (reserved) await safeRefundCredits(sb, COST);
      return NextResponse.json(
        { error: "Worker dub failed", workerStatus: r.status, workerBody: payload },
        { status: 502 }
      );
    }

    return NextResponse.json({ ok: true, jobId, lang, ...payload });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Dub failed" }, { status: 500 });
  }
}
