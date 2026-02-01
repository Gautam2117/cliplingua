import { NextResponse } from "next/server";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const jobId = String(searchParams.get("jobId") || "").trim();
    if (!jobId) return new NextResponse("Missing jobId", { status: 400 });

    const workerBase = process.env.WORKER_BASE_URL || process.env.NEXT_PUBLIC_WORKER_BASE_URL;
    if (!workerBase) return new NextResponse("WORKER_BASE_URL not set", { status: 500 });

    const r = await fetch(`${workerBase.replace(/\/$/, "")}/jobs/${encodeURIComponent(jobId)}`, {
      cache: "no-store",
    });

    const text = await r.text();
    return new NextResponse(text, { status: r.status, headers: { "Content-Type": "application/json" } });
  } catch (e: any) {
    return new NextResponse(e?.message || "Status failed", { status: 500 });
  }
}
