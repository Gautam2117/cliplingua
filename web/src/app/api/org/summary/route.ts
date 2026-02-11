import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";
import { getUserFromBearerToken } from "@/lib/supabaseUserFromBearer";

export const runtime = "nodejs";

function jsonError(message: string, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

function minuteKey(d: Date) {
  // UTC minute key like "2026-02-12T10:34"
  return d.toISOString().slice(0, 16);
}

export async function GET(req: Request) {
  try {
    const auth = req.headers.get("authorization") || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
    if (!token) return jsonError("Missing bearer token", 401);

    const user = await getUserFromBearerToken(token);
    if (!user) return jsonError("Not authenticated", 401);

    const sb = supabaseAuthed(token);

    const { data: profile, error: pErr } = await sb
      .from("profiles")
      .select("active_org_id")
      .eq("id", user.id)
      .single();

    if (pErr) return jsonError(pErr.message, 400);

    const orgId = profile?.active_org_id;
    if (!orgId) return jsonError("No active org", 400);

    const { data: org, error: oErr } = await sb
      .from("organizations")
      .select("id,name,plan,seats_purchased,api_enabled,api_rpm,api_daily_jobs,max_api_keys,credits")
      .eq("id", orgId)
      .single();

    if (oErr) return jsonError(oErr.message, 400);

    const { count: membersCount, error: mErr } = await sb
      .from("org_members")
      .select("user_id", { count: "exact", head: true })
      .eq("org_id", orgId);

    if (mErr) return jsonError(mErr.message, 400);

    // Correct column names based on your schema:
    // org_id, minute_bucket, minute_count, day, day_count, updated_at
    const { data: usageRow, error: uErr } = await sb
      .from("org_api_usage")
      .select("minute_bucket,minute_count,day,day_count,updated_at")
      .eq("org_id", orgId)
      .maybeSingle();

    if (uErr) return jsonError(uErr.message, 400);

    const now = new Date();
    const todayISO = now.toISOString().slice(0, 10);

    const curMinKey = minuteKey(now);
    const rowMinKey = usageRow?.minute_bucket ? minuteKey(new Date(usageRow.minute_bucket)) : null;

    const minuteUsed = rowMinKey === curMinKey ? Number(usageRow?.minute_count || 0) : 0;
    const dailyUsed = usageRow?.day === todayISO ? Number(usageRow?.day_count || 0) : 0;

    return NextResponse.json({
      org,
      seats: {
        used: membersCount || 0,
        purchased: org?.seats_purchased ?? 0,
      },
      api: {
        enabled: !!org?.api_enabled,
        rpmLimit: org?.api_rpm ?? 0,
        dailyJobsLimit: org?.api_daily_jobs ?? 0,
        maxKeys: org?.max_api_keys ?? 0,
        minuteUsed,
        dailyUsed,
      },
    });
  } catch (e: any) {
    return jsonError(e?.message || "Server error", 500);
  }
}
