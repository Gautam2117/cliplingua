import { NextResponse } from "next/server";

const TYPE_MAP: Record<string, string> = {
  video: "video",
  audio: "audio",
  log: "log",
};

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    const type = String(searchParams.get("type") || "").trim();

    if (!jobId) return new NextResponse("Missing jobId", { status: 400 });
    if (!TYPE_MAP[type]) return new NextResponse("Invalid type", { status: 400 });

    const workerBase = process.env.WORKER_BASE_URL || process.env.NEXT_PUBLIC_WORKER_BASE_URL;
    if (!workerBase) return new NextResponse("WORKER_BASE_URL not set", { status: 500 });

    const base = workerBase.replace(/\/$/, "");
    const url = `${base}/jobs/${encodeURIComponent(jobId)}/${TYPE_MAP[type]}`;

    // Optional readiness check
    const head = await fetch(url, { method: "HEAD", cache: "no-store" });
    if (!head.ok) return new NextResponse("Artifact not ready", { status: 404 });

    return NextResponse.json({ url });
  } catch (e: any) {
    return new NextResponse(e?.message || "Artifact failed", { status: 500 });
  }
}
