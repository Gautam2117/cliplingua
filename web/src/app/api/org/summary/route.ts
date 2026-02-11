import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";
import { getUserFromBearer } from "@/lib/supabaseUserFromBearer";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function isSameMinute(a: string, now: Date) {
  const ta = new Date(a).getTime();
  const tn = new Date(now);
  tn.setSeconds(0, 0);
  return Math.floor(ta / 60000) === Math.floor(tn.getTime() / 60000);
}

export async function GET(req: Request) {
  const { user, token } = await getUserFromBearer(req);
  if (!token) return NextResponse.json({ error: "Missing bearer token" }, { status: 401 });
  if (!user) return NextResponse.json({ error: "Not authenticated" }, { status: 401 });

  const sb = supabaseAuthed(token);

  const { data: profile, error: pErr } = await sb
    .from("profiles")
    .select("id, active_org_id")
    .eq("id", user.id)
    .single();

  if (pErr) return NextResponse.json({ error: pErr.message }, { status: 400 });
  if (!profile?.active_org_id) return NextResponse.json({ error: "No active org" }, { status: 400 });

  const orgId = profile.active_org_id;

  const { data: org, error: oErr } = await sb
    .from("organizations")
    .select("id,name,credits,invite_code,plan,seats_purchased,api_enabled,api_rpm,api_daily_jobs,max_api_keys")
    .eq("id", orgId)
    .single();

  if (oErr) return NextResponse.json({ error: oErr.message }, { status: 400 });

  const { count: membersCount, error: mErr } = await sb
    .from("org_members")
    .select("user_id", { count: "exact", head: true })
    .eq("org_id", orgId);

  if (mErr) return NextResponse.json({ error: mErr.message }, { status: 400 });

  const { data: usageRow, error: uErr } = await sb
    .from("org_api_usage")
    .select("day,day_count,minute_bucket,minute_count,updated_at")
    .eq("org_id", orgId)
    .maybeSingle();

  if (uErr) return NextResponse.json({ error: uErr.message }, { status: 400 });

  const now = new Date();
  const todayISO = now.toISOString().slice(0, 10);

  const minuteUsed =
    usageRow?.minute_bucket && isSameMinute(usageRow.minute_bucket, now)
      ? Number(usageRow.minute_count || 0)
      : 0;

  const dailyUsed = usageRow?.day === todayISO ? Number(usageRow.day_count || 0) : 0;

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
}
