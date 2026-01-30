import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const jobId = searchParams.get("jobId");
  if (!jobId) return NextResponse.json({ error: "missing jobId" }, { status: 400 });

  const worker = process.env.WORKER_URL!;
  const r = await fetch(`${worker}/jobs/${jobId}`, { cache: "no-store" });
  const data = await r.json();
  return NextResponse.json(data, { status: r.status });
}
