import { NextResponse } from "next/server";
import { getUserFromBearer } from "@/lib/supabaseUserFromBearer";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function workerBase() {
  const base =
    process.env.WORKER_BASE_URL ||
    process.env.RENDER_BASE_URL ||
    process.env.CLIPLINGUA_WORKER_BASE_URL ||
    "";
  return base.replace(/\/+$/, "");
}

export async function POST(req: Request) {
  try {
    const { user, token } = await getUserFromBearer(req);
    if (!user || !token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json().catch(() => ({}));
    const jobId = String(body?.jobId || "").trim();
    const lang = String(body?.lang || "").trim().toLowerCase();

    if (!jobId) return NextResponse.json({ error: "Missing jobId" }, { status: 400 });
    if (!lang) return NextResponse.json({ error: "Missing lang" }, { status: 400 });

    const base = workerBase();
    if (!base) return NextResponse.json({ error: "Missing WORKER_BASE_URL" }, { status: 500 });

    const upstream = await fetch(`${base}/api/youtube/upload`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        jobId,
        lang,
        title: body?.title ?? null,
        description: body?.description ?? null,
        privacyStatus: body?.privacyStatus ?? null,
      }),
    });

    const text = await upstream.text();
    return new NextResponse(text, {
      status: upstream.status,
      headers: { "Content-Type": upstream.headers.get("content-type") || "application/json; charset=utf-8" },
    });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Upload failed" }, { status: 500 });
  }
}
