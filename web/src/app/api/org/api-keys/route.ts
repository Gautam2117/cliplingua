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
  const { data: prof, error } = await sb
    .from("profiles")
    .select("active_org_id")
    .eq("id", userId)
    .single();

  if (error) throw new Error(error.message);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { error: bootErr } = await sb.rpc("bootstrap_org");
    if (bootErr) throw new Error(bootErr.message);

    const { data: prof2, error: error2 } = await sb
      .from("profiles")
      .select("active_org_id")
      .eq("id", userId)
      .single();

    if (error2) throw new Error(error2.message);

    orgId = (prof2 as any)?.active_org_id as string | null;
  }

  if (!orgId) throw new Error("active_org_id missing");
  return orgId;
}

async function enforceApiEnabled(sb: ReturnType<typeof supabaseAuthed>, orgId: string) {
  const { data: orgRow, error: orgErr } = await sb
    .from("organizations")
    .select("api_enabled,plan")
    .eq("id", orgId)
    .single();

  if (orgErr) {
    return { ok: false as const, status: 400, message: orgErr.message };
  }

  if (!orgRow?.api_enabled) {
    return {
      ok: false as const,
      status: 403,
      message: "Plan does not include Bulk API (upgrade to Agency)",
    };
  }

  return { ok: true as const };
}

export async function GET(req: Request) {
  try {
    const token = getBearer(req);
    if (!token) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

    const sb = supabaseAuthed(token);
    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const orgId = await ensureActiveOrg(sb, u.user.id);

    // GET can remain ungated: show keys if any (or empty list)
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
    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const orgId = await ensureActiveOrg(sb, u.user.id);

    // Agency gating (Bulk API)
    const gate = await enforceApiEnabled(sb, orgId);
    if (!gate.ok) return NextResponse.json({ error: gate.message }, { status: gate.status });

    // Your SQL patch provides create_org_api_key
    const res = await sb.rpc("create_org_api_key", { name });
    if (res.error) return NextResponse.json({ error: res.error.message }, { status: 400 });

    const apiKey = String((res.data as any)?.api_key || "") || String((res.data as any)?.apiKey || "");
    if (!apiKey.trim()) return NextResponse.json({ error: "Key create returned empty" }, { status: 500 });

    return NextResponse.json({ apiKey: apiKey.trim() });
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
    const { data: u, error: uErr } = await sb.auth.getUser();
    if (uErr || !u.user) return NextResponse.json({ error: "Invalid session" }, { status: 401 });

    const orgId = await ensureActiveOrg(sb, u.user.id);

    // Agency gating (Bulk API)
    const gate = await enforceApiEnabled(sb, orgId);
    if (!gate.ok) return NextResponse.json({ error: gate.message }, { status: gate.status });

    const res = await sb.rpc("revoke_org_api_key", { prefix });
    if (res.error) return NextResponse.json({ error: res.error.message }, { status: 400 });

    return NextResponse.json({ ok: true });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message || "Revoke failed" }, { status: 500 });
  }
}
