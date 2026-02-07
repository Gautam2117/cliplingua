// src/lib/supabase-admin.ts
import { createClient, type SupabaseClient } from "@supabase/supabase-js";

let adminClient: SupabaseClient | null = null;

function getAdminClient() {
  if (adminClient) return adminClient;

  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
  const service = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!url) throw new Error("Missing SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL)");
  if (!service) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

  adminClient = createClient(url, service, {
    auth: { persistSession: false },
  });

  return adminClient;
}

type SupabaseAdminDual = (() => SupabaseClient) & SupabaseClient;

// supabaseAdmin can be used as:
// - supabaseAdmin().from(...)
// - supabaseAdmin.from(...)
export const supabaseAdmin: SupabaseAdminDual = new Proxy(
  (function () {
    return getAdminClient();
  }) as any,
  {
    apply() {
      return getAdminClient();
    },
    get(_target, prop) {
      const c: any = getAdminClient();
      const v = c[prop];
      return typeof v === "function" ? v.bind(c) : v;
    },
  }
) as any;
