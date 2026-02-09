import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 300;

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

export async function POST(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json();
    const items = Array.isArray(body?.items) ? body.items : [];
    const concurrency = Math.min(Math.max(Number(body?.concurrency || 3), 1), 6);

    if (items.length === 0) {
      return NextResponse.json({ error: "Provide items: [{ jobId, lang, captions }]" }, { status: 400 });
    }
    if (items.length > 50) {
      return NextResponse.json({ error: "Max 50 items per request" }, { status: 400 });
    }

    const origin = new URL(req.url).origin;

    const clean = items.map((x: any) => ({
      jobId: String(x?.jobId || "").trim(),
      lang: String(x?.lang || "").trim(),
      captions: String(x?.captions || "Clean").trim(),
    })).filter((x: any) => x.jobId && x.lang);

    if (clean.length === 0) {
      return NextResponse.json({ error: "Each item needs jobId and lang" }, { status: 400 });
    }

    const results: any[] = [];
    let idx = 0;

    async function runOne(it: any) {
      try {
        const r = await fetch(`${origin}/api/clip/dub`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            jobId: it.jobId,
            lang: it.lang,
            captions: it.captions,
          }),
          cache: "no-store",
        });

        const j = await r.json().catch(() => ({}));
        if (!r.ok) return { ...it, ok: false, error: j?.error || "Dub failed" };
        return { ...it, ok: true, data: j };
      } catch (e: any) {
        return { ...it, ok: false, error: e?.message || "Dub failed" };
      }
    }

    async function workerLoop() {
      while (idx < clean.length) {
        const current = clean[idx++];
        const res = await runOne(current);
        results.push(res);
      }
    }

    const workers = Array.from({ length: Math.min(concurrency, clean.length) }, () => workerLoop());
    await Promise.all(workers);

    return NextResponse.json({ ok: true, count: clean.length, results });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Bulk dub failed" }, { status: 500 });
  }
}
