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
