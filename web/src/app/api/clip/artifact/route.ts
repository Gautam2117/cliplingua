import { NextResponse } from "next/server";

const TYPE_MAP: Record<string, string> = {
  video: "video",
  audio: "audio",
  log: "log",
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

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    const type = String(searchParams.get("type") || "").trim();

    if (!jobId) return new NextResponse("Missing jobId", { status: 400 });
    if (!TYPE_MAP[type]) return new NextResponse("Invalid type", { status: 400 });

    const workerBase = getWorkerBaseUrl();

    // Check job status (do not use HEAD on artifact endpoints)
    const jr = await fetch(`${workerBase}/jobs/${encodeURIComponent(jobId)}`, {
      cache: "no-store",
    });

    if (!jr.ok) return new NextResponse("Job not found", { status: jr.status });

    const job = await jr.json();
    if (job.status !== "done") {
      return new NextResponse(`Job not done (status=${job.status})`, { status: 409 });
    }

    const url = `${workerBase}/jobs/${encodeURIComponent(jobId)}/${TYPE_MAP[type]}`;
    return NextResponse.json({ url });
  } catch (e: any) {
    return new NextResponse(e?.message || "Artifact failed", { status: 500 });
  }
}
