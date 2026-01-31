import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase-server";

export const runtime = "nodejs";

type Body = {
  jobId?: string;
  type?: "audio" | "video" | "log";
};

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
  const { jobId, type } = (await req.json()) as Body;

  if (!jobId) return NextResponse.json({ error: "missing jobId" }, { status: 400 });
  if (!type) return NextResponse.json({ error: "missing type" }, { status: 400 });

  const worker = process.env.WORKER_URL;
  if (!worker) return NextResponse.json({ error: "WORKER_URL not set" }, { status: 500 });

  // 1) Ensure job is done
  const jr = await fetch(`${worker}/jobs/${jobId}`, { cache: "no-store" });
  if (!jr.ok) return NextResponse.json({ error: "job not found" }, { status: jr.status });

  const job = await jr.json();
  if (job.status !== "done") {
    return NextResponse.json(
      { error: `job not done (status=${job.status})` },
      { status: 409 }
    );
  }

  const meta = artifactMeta(jobId, type);

  // 2) Download from Render worker
  const fr = await fetch(`${worker}${meta.workerPath}`, { cache: "no-store" });
  if (!fr.ok) {
    const t = await fr.text().catch(() => "");
    return NextResponse.json(
      { error: `failed to fetch artifact (${type})`, detail: t.slice(0, 2000) },
      { status: 502 }
    );
  }

  const buf = Buffer.from(await fr.arrayBuffer());

  // 3) Upload to Supabase Storage
  const supabase = supabaseServer();
  const bucket = "artifacts";

  const { error: upErr } = await supabase.storage.from(bucket).upload(meta.storagePath, buf, {
    contentType: meta.contentType,
    upsert: true,
  });

  if (upErr) {
    return NextResponse.json({ error: upErr.message }, { status: 500 });
  }

  // 4) Return signed url (10 min)
  const { data: signed, error: signErr } = await supabase.storage
    .from(bucket)
    .createSignedUrl(meta.storagePath, 60 * 10);

  if (signErr || !signed?.signedUrl) {
    return NextResponse.json(
      { error: signErr?.message || "failed to sign url" },
      { status: 500 }
    );
  }

  return NextResponse.json({ url: signed.signedUrl, path: meta.storagePath });
}
