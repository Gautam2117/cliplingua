// src/app/api/org/api-keys/route.ts
import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

async function ensureActiveOrg(sb: ReturnType<typeof supabaseAuthed>, userId: string) {
  const { data: prof, error } = await sb.from("profiles").select("active_org_id").eq("id", userId).single();
  if (error) throw new Error(error.message);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { error: bootErr } = await sb.rpc("bootstrap_org");
    if (bootErr) throw new Error(bootErr.message);

    const { data: prof2, error: err2 } = await sb.from("profiles").select("active_org_id").eq("id", userId).single();
    if (err2) throw new Error(err2.message);

    orgId = (prof2 as any)?.active_org_id as string | null;
  }

  if (!orgId) throw new Error("active_org_id missing");
  return orgId;
}

async function rpcTry(sb: any, names: string[], args: any) {
  let last: any = null;
  for (const n of names) {
    const res = await sb.rpc(n, args);
    if (!res?.error) return res;
    last = res.error;
  }
  throw new Error(last?.message || "RPC failed");
}

export async function GET(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const sb = supabaseAuthed(token);

    // IMPORTANT: const user for TS narrowing
    const { data: userRes, error: uErr } = await sb.auth.getUser();
    const user = userRes.user;
    if (uErr || !user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const orgId = await ensureActiveOrg(sb, user.id);

    const { data, error } = await sb
      .from("org_api_keys")
      .select("prefix,name,created_at,revoked_at")
      .eq("org_id", orgId)
      .order("created_at", { ascending: false });

    if (error) return NextResponse.json({ error: error.message }, { status: 400 });

    return NextResponse.json({ keys: data || [] });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Failed" }, { status: 500 });
  }
}

export async function POST(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json();
    const name = String(body?.name || "").trim();
    if (!name) return NextResponse.json({ error: "Missing name" }, { status: 400 });

    const sb = supabaseAuthed(token);

    // IMPORTANT: const user for TS narrowing
    const { data: userRes, error: uErr } = await sb.auth.getUser();
    const user = userRes.user;
    if (uErr || !user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    // Your patched SQL guarantees create_org_api_key exists
    const res = await rpcTry(sb, ["create_org_api_key", "create_api_key"], { name });

    const apiKey = String((res.data as any)?.api_key || (res.data as any)?.apiKey || "").trim();
    if (!apiKey) return NextResponse.json({ error: "Key create returned empty" }, { status: 500 });

    return NextResponse.json({ apiKey });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Create failed" }, { status: 500 });
  }
}

export async function DELETE(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const body = await req.json();
    const prefix = String(body?.prefix || "").trim();
    if (!prefix) return NextResponse.json({ error: "Missing prefix" }, { status: 400 });

    const sb = supabaseAuthed(token);

    // IMPORTANT: const user for TS narrowing
    const { data: userRes, error: uErr } = await sb.auth.getUser();
    const user = userRes.user;
    if (uErr || !user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    await rpcTry(sb, ["revoke_org_api_key", "revoke_api_key"], { prefix });

    return NextResponse.json({ ok: true });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Revoke failed" }, { status: 500 });
  }
}
