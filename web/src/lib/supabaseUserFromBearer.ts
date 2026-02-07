import { createClient } from "@supabase/supabase-js";

function getBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || null;
}

export async function getUserFromBearer(req: Request) {
  const token = getBearer(req);
  if (!token) return { user: null, token: null };

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const anon = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || "";
  if (!url || !anon) throw new Error("Missing Supabase public env");

  const supabase = createClient(url, anon, { auth: { persistSession: false } });
  const { data, error } = await supabase.auth.getUser(token);
  if (error || !data?.user) return { user: null, token: null };

  return { user: data.user, token };
}
