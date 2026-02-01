import { NextResponse } from "next/server";

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const url = String(body?.url || "").trim();
    if (!url) return new NextResponse("Missing url", { status: 400 });

    const workerBase = process.env.WORKER_BASE_URL || process.env.NEXT_PUBLIC_WORKER_BASE_URL;
    if (!workerBase) return new NextResponse("WORKER_BASE_URL not set", { status: 500 });

    const r = await fetch(`${workerBase.replace(/\/$/, "")}/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url }),
      cache: "no-store",
    });

    const text = await r.text();
    return new NextResponse(text, { status: r.status, headers: { "Content-Type": "application/json" } });
  } catch (e: any) {
    return new NextResponse(e?.message || "Submit failed", { status: 500 });
  }
}
