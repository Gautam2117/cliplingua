import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

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

type ArtifactType = "video" | "audio" | "log" | "dub_video" | "dub_audio" | "dub_log";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    const type = String(searchParams.get("type") || "").trim() as ArtifactType;
    const lang = String(searchParams.get("lang") || "").trim();

    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });
    if (!type) return NextResponse.json({ error: "Missing type" }, { status: 400 });

    // keep it authenticated (matches your client)
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const workerBase = getWorkerBaseUrl();

    // check job status
    const jr = await fetch(`${workerBase}/jobs/${encodeURIComponent(jobId)}`, { cache: "no-store" });
    const jText = await jr.text();
    let job: any = {};
    try {
      job = jText ? JSON.parse(jText) : {};
    } catch {
      job = { raw: jText };
    }

    if (!jr.ok) return NextResponse.json({ error: "Job not found" }, { status: jr.status });

    // Base artifacts require base job done
    if (type === "video" || type === "audio" || type === "log") {
      if (job.status !== "done") {
        return NextResponse.json({ error: `Job not done (status=${job.status})` }, { status: 409 });
      }

      const url = `${workerBase}/jobs/${encodeURIComponent(jobId)}/${type}`;
      return NextResponse.json({ url });
    }

    // Dub artifacts require lang and dub done
    if (!lang) return NextResponse.json({ error: "Missing lang for dub artifact" }, { status: 400 });

    const ds = job?.dub_status?.[lang];
    const dubStatus =
      typeof ds === "string" ? ds : typeof ds === "object" && ds ? String((ds as any).status || "") : "";

    if (dubStatus !== "done") {
      return NextResponse.json({ error: `Dub not done (status=${dubStatus || "not_started"})` }, { status: 409 });
    }

    const kind = type.replace("dub_", ""); // video|audio|log
    const url = `${workerBase}/jobs/${encodeURIComponent(jobId)}/dubs/${encodeURIComponent(lang)}/${kind}`;
    return NextResponse.json({ url });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Artifact failed" }, { status: 500 });
  }
}
