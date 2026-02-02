import { createClient } from "@supabase/supabase-js";

function getSupabaseUrl() {
  return process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
}

function getAnonKey() {
  return process.env.SUPABASE_ANON_KEY || process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
}

export function supabaseAuthed(accessToken: string) {
  const url = getSupabaseUrl();
  const anon = getAnonKey();

  if (!url) throw new Error("Missing SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL)");
  if (!anon) throw new Error("Missing SUPABASE_ANON_KEY (or NEXT_PUBLIC_SUPABASE_ANON_KEY)");

  return createClient(url, anon, {
    auth: { persistSession: false },
    global: {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    },
  });
}

/**
 * Server-side client.
 * Uses Service Role key if present (preferred for Storage upload/sign).
 * Falls back to anon key if service key is missing (limited permissions).
 */
export function supabaseServer() {
  const url = getSupabaseUrl();
  if (!url) throw new Error("Missing SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL)");

  const service = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
  const anon = getAnonKey();

  const key = service || anon;
  if (!key) {
    throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY and SUPABASE_ANON_KEY");
  }

  return createClient(url, key, {
    auth: { persistSession: false },
  });
}
