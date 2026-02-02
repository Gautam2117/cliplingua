import { NextResponse } from "next/server";
import { supabaseAuthed, supabaseServer } from "@/lib/supabase-server";

export const runtime = "nodejs";

type Body = {
  jobId?: string;
  type?: "audio" | "video" | "log";
};

function getWorkerBaseUrl() {
  const base =
    process.env.WORKER_BASE_URL?.trim() ||
    process.env.NEXT_PUBLIC_WORKER_BASE_URL?.trim() ||
    process.env.WORKER_URL?.trim() ||
    "";
  if (!base) throw new Error("Missing WORKER_BASE_URL (or NEXT_PUBLIC_WORKER_BASE_URL)");
  return base.replace(/\/+$/, "");
}

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

function artifactMeta(jobId: string, type: "audio" | "video" | "log") {
  if (type === "audio") {
    return {
      workerPath: `/jobs/${jobId}/audio`,
      contentType: "audio/wav",
      storagePath: `jobs/${jobId}/audio.wav`,
    };
  }
  if (type === "video") {
    return {
      workerPath: `/jobs/${jobId}/video`,
      contentType: "video/mp4",
      storagePath: `jobs/${jobId}/video.mp4`,
    };
  }
  return {
    workerPath: `/jobs/${jobId}/log`,
    contentType: "text/plain",
    storagePath: `jobs/${jobId}/log.txt`,
  };
}

export async function POST(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const { jobId, type } = (await req.json()) as Body;

    if (!jobId) return NextResponse.json({ error: "missing jobId" }, { status: 400 });
    if (!type) return NextResponse.json({ error: "missing type" }, { status: 400 });

    const sb = supabaseAuthed(token);

    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    // Ensure this job belongs to the user (RLS + explicit filter)
    const { data: row, error: jErr } = await sb
      .from("user_jobs")
      .select("worker_job_id")
      .eq("worker_job_id", jobId)
      .limit(1)
      .maybeSingle();

    if (jErr) return NextResponse.json({ error: jErr.message }, { status: 500 });
    if (!row) return NextResponse.json({ error: "job not found" }, { status: 404 });

    const workerBase = getWorkerBaseUrl();

    // Ensure job is done on worker
    const jr = await fetch(`${workerBase}/jobs/${jobId}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(20_000),
    });
    if (!jr.ok) return NextResponse.json({ error: "job not found" }, { status: jr.status });

    const job = await jr.json();
    if (job.status !== "done") {
      return NextResponse.json({ error: `job not done (status=${job.status})` }, { status: 409 });
    }

    const meta = artifactMeta(jobId, type);

    // Download artifact from worker
    const fr = await fetch(`${workerBase}${meta.workerPath}`, {
      cache: "no-store",
      signal: AbortSignal.timeout(60_000),
    });
    if (!fr.ok) {
      const t = await fr.text().catch(() => "");
      return NextResponse.json(
        { error: `failed to fetch artifact (${type})`, detail: t.slice(0, 2000) },
        { status: 502 }
      );
    }

    const buf = Buffer.from(await fr.arrayBuffer());

    // Upload to Supabase Storage (server role recommended)
    const admin = supabaseServer();
    const bucket = "artifacts";

    const { error: upErr } = await admin.storage.from(bucket).upload(meta.storagePath, buf, {
      contentType: meta.contentType,
      upsert: true,
    });

    if (upErr) return NextResponse.json({ error: upErr.message }, { status: 500 });

    // Signed url (10 min)
    const { data: signed, error: signErr } = await admin.storage
      .from(bucket)
      .createSignedUrl(meta.storagePath, 60 * 10);

    if (signErr || !signed?.signedUrl) {
      return NextResponse.json(
        { error: signErr?.message || "failed to sign url" },
        { status: 500 }
      );
    }

    return NextResponse.json({ url: signed.signedUrl, path: meta.storagePath });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "download failed" }, { status: 500 });
  }
}
