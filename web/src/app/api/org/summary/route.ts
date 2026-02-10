import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";
import { getUserFromBearer } from "@/lib/supabaseUserFromBearer";

function sameMinute(a: string, b: Date) {
  return new Date(a).getTime() === new Date(b.toISOString()).getTime();
}

export async function GET(req: Request) {
  const auth = req.headers.get("authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
  if (!token) return NextResponse.json({ error: "Missing bearer token" }, { status: 401 });

  const user = await getUserFromBearer(token);
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
    .select("id,name,plan,seats_purchased,api_enabled,api_rpm,api_daily_jobs,max_api_keys")
    .eq("id", orgId)
    .single();

  if (oErr) return NextResponse.json({ error: oErr.message }, { status: 400 });

  const { count: membersCount, error: mErr } = await sb
    .from("org_members")
    .select("user_id", { count: "exact", head: true })
    .eq("org_id", orgId);

  if (mErr) return NextResponse.json({ error: mErr.message }, { status: 400 });

  const { data: usageRow } = await sb
    .from("org_api_usage")
    .select("day,daily_jobs_used,minute_bucket,minute_reqs_used")
    .eq("org_id", orgId)
    .maybeSingle();

  const now = new Date();
  const curMinute = new Date(now);
  curMinute.setSeconds(0, 0);

  const minuteUsed =
    usageRow?.minute_bucket && sameMinute(usageRow.minute_bucket, curMinute)
      ? usageRow.minute_reqs_used
      : 0;

  const todayISO = now.toISOString().slice(0, 10);
  const dailyUsed = usageRow?.day === todayISO ? usageRow.daily_jobs_used : 0;

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
