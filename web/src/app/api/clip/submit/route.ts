import { NextResponse } from "next/server";

export async function POST(req: Request) {
  const { url } = await req.json();
  if (!url) return NextResponse.json({ error: "missing url" }, { status: 400 });

  const worker = process.env.WORKER_URL!;
  const r = await fetch(`${worker}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
    cache: "no-store",
  });

  const data = await r.json();
  return NextResponse.json(data, { status: r.status });
}
