import { supabase } from "@/lib/supabase";

export async function requireAuthedSession() {
  const { data: sess, error: sessErr } = await supabase.auth.getSession();
  if (sessErr) throw new Error(sessErr.message);

  const token = sess.session?.access_token;
  if (!token) throw new Error("No session");

  const { data: userData, error: userErr } = await supabase.auth.getUser();
  if (userErr || !userData.user) throw new Error("Invalid session");

  return { token, user: userData.user };
}

export async function ensureActiveOrg(userId: string) {
  const { data: prof, error: profErr } = await supabase
    .from("profiles")
    .select("id,email,active_org_id")
    .eq("id", userId)
    .single();

  if (profErr) throw new Error(profErr.message);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { data: boot, error: bootErr } = await supabase.rpc("bootstrap_org");
    if (bootErr) throw new Error(bootErr.message);

    // Some people return orgId from RPC, some just set profile. Support both.
    const maybeOrgId = String(boot || "").trim();
    if (maybeOrgId) orgId = maybeOrgId;

    if (!orgId) {
      const { data: prof2, error: profErr2 } = await supabase
        .from("profiles")
        .select("id,email,active_org_id")
        .eq("id", userId)
        .single();
      if (profErr2) throw new Error(profErr2.message);
      orgId = (prof2 as any)?.active_org_id as string | null;
    }
  }

  if (!orgId) throw new Error("active_org_id missing");
  return orgId;
}
// src/lib/org-client.ts
import { supabase } from "@/lib/supabase";

export async function requireAuthedSession(): Promise<{ user: { id: string }; token: string }> {
  const { data: sess, error: sessErr } = await supabase.auth.getSession();
  if (sessErr) throw new Error(sessErr.message);

  const token = sess.session?.access_token || null;
  if (!token) throw new Error("No session");

  const { data: userData, error: userErr } = await supabase.auth.getUser();
  if (userErr) throw new Error(userErr.message);
  if (!userData.user?.id) throw new Error("Invalid session");

  return { user: { id: userData.user.id }, token };
}

export async function ensureActiveOrg(userId: string): Promise<string> {
  const { data: prof, error: profErr } = await supabase
    .from("profiles")
    .select("active_org_id")
    .eq("id", userId)
    .single();

  if (profErr) throw new Error(profErr.message);

  let orgId = (prof as any)?.active_org_id as string | null;

  if (!orgId) {
    const { error: bootErr } = await supabase.rpc("bootstrap_org");
    if (bootErr) throw new Error(bootErr.message);

    const { data: prof2, error: profErr2 } = await supabase
      .from("profiles")
      .select("active_org_id")
      .eq("id", userId)
      .single();

    if (profErr2) throw new Error(profErr2.message);

    orgId = (prof2 as any)?.active_org_id as string | null;
  }

  if (!orgId) throw new Error("active_org_id missing after bootstrap");
  return orgId;
}

export type OrgPlan = "free" | "creator" | "agency" | "agency_plus";

export type OrgRow = {
  id: string;
  name: string;
  credits: number;
  invite_code: string;
  plan: OrgPlan | null;
  seats_purchased: number | null;
  api_enabled: boolean | null;
  api_rpm: number | null;
  api_daily_jobs: number | null;
  max_api_keys: number | null;
};

export type ApiUsageView = {
  minuteUsed: number;
  minuteLimit: number;
  dailyUsed: number;
  dailyLimit: number;
};

export type OrgContext = {
  org: OrgRow;
  membersUsed: number;
  seatsPurchased: number;
  isAdmin: boolean;
  api: ApiUsageView & { enabled: boolean; maxKeys: number };
};

function minuteKey(d: Date): string {
  return d.toISOString().slice(0, 16); // YYYY-MM-DDTHH:MM
}

export async function loadOrgContext(orgId: string): Promise<OrgContext> {
  const { data: orgRow, error: orgErr } = await supabase
    .from("organizations")
    .select("id,name,credits,invite_code,plan,seats_purchased,api_enabled,api_rpm,api_daily_jobs,max_api_keys")
    .eq("id", orgId)
    .single();

  if (orgErr) throw new Error(orgErr.message);

  const org = orgRow as any as OrgRow;

  const { count: membersCount, error: mErr } = await supabase
    .from("org_members")
    .select("user_id", { count: "exact", head: true })
    .eq("org_id", orgId);

  if (mErr) throw new Error(mErr.message);

  const seatsPurchased = Number(org.seats_purchased || 0);
  const membersUsed = Number(membersCount || 0);

  const { data: isAdminVal, error: aErr } = await supabase.rpc("is_org_admin", { p_org_id: orgId });
  if (aErr) throw new Error(aErr.message);
  const isAdmin = !!isAdminVal;

  const { data: usageRow, error: uErr } = await supabase
    .from("org_api_usage")
    .select("day,daily_jobs_used,minute_bucket,minute_reqs_used")
    .eq("org_id", orgId)
    .maybeSingle();

  if (uErr) throw new Error(uErr.message);

  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  const curMinute = new Date(now);
  curMinute.setSeconds(0, 0);

  const minuteUsed =
    usageRow?.minute_bucket && minuteKey(new Date(usageRow.minute_bucket)) === minuteKey(curMinute)
      ? Number(usageRow.minute_reqs_used || 0)
      : 0;

  const dailyUsed = usageRow?.day === today ? Number(usageRow.daily_jobs_used || 0) : 0;

  const minuteLimit = Number(org.api_rpm || 0);
  const dailyLimit = Number(org.api_daily_jobs || 0);

  return {
    org,
    membersUsed,
    seatsPurchased,
    isAdmin,
    api: {
      enabled: !!org.api_enabled,
      maxKeys: Number(org.max_api_keys || 0),
      minuteUsed,
      minuteLimit,
      dailyUsed,
      dailyLimit,
    },
  };
}
