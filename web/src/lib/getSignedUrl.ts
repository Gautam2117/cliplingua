// lib/getSignedUrl.ts
import { createClient } from "@supabase/supabase-js";

const sb = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

export async function getSignedUrl(storageKey: string, expires = 3600) {
  if (!storageKey) return null;
  const { data } = await sb.storage
    .from("clips-out")
    .createSignedUrl(storageKey, expires);
  return data?.signedUrl ?? null;
}
