import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const BASE_TYPES = new Set(["video", "audio", "log"] as const);
const DUB_TYPES = new Set(["dub_video", "dub_audio", "dub_log"] as const);

type BaseType = "video" | "audio" | "log";
type DubType = "dub_video" | "dub_audio" | "dub_log";
type ArtifactType = BaseType | DubType;

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

function isArtifactType(v: string): v is ArtifactType {
  return BASE_TYPES.has(v as any) || DUB_TYPES.has(v as any);
}

export async function GET(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    const type = String(searchParams.get("type") || "").trim();
    const lang = String(searchParams.get("lang") || "").trim();

    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });
    if (!type || !isArtifactType(type)) return NextResponse.json({ error: "Invalid type" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const { data: row } = await sb
      .from("user_jobs")
      .select("worker_job_id")
      .eq("worker_job_id", jobId)
      .limit(1)
      .maybeSingle();

    if (!row) return NextResponse.json({ error: "job not found" }, { status: 404 });

    const workerBase = getWorkerBaseUrl();

    // Verify job exists and is done
    const jr = await fetch(`${workerBase}/jobs/${encodeURIComponent(jobId)}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(25_000),
    });

    const jrText = await jr.text();
    if (!jr.ok) return new NextResponse(jrText, { status: jr.status });

    const job = JSON.parse(jrText);

    // Base artifacts
    if (BASE_TYPES.has(type as any)) {
      if (job.status !== "done") {
        return NextResponse.json({ error: `Job not done (status=${job.status})` }, { status: 409 });
      }
      const url = `${workerBase}/jobs/${encodeURIComponent(jobId)}/${type}`;
      return NextResponse.json({ url });
    }

    // Dub artifacts
    if (!lang) return NextResponse.json({ error: "Missing lang for dub artifact" }, { status: 400 });
    if (!allowedLangs().has(lang)) return NextResponse.json({ error: "Unsupported lang" }, { status: 400 });

    const dub = job?.dub_status?.[lang];
    const dubStatus = typeof dub === "string" ? dub : dub?.status;

    if (dubStatus !== "done") {
      return NextResponse.json({ error: `Dub not done (status=${dubStatus || "not_started"})` }, { status: 409 });
    }

    const dubKind = type.replace("dub_", "");
    const url = `${workerBase}/jobs/${encodeURIComponent(jobId)}/dubs/${encodeURIComponent(lang)}/${dubKind}`;
    return NextResponse.json({ url });
  } catch (e: any) {
    const isAbort = e?.name === "AbortError";
    return NextResponse.json({ error: isAbort ? "Worker timeout" : e?.message || "Artifact failed" }, { status: 500 });
  }
}
