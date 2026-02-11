import type { User } from "@supabase/supabase-js";
import { supabaseAdmin } from "@/lib/supabase-admin";

export async function getUserFromBearerToken(token: string): Promise<User | null> {
  if (!token) return null;
  const { data, error } = await supabaseAdmin.auth.getUser(token);
  if (error) return null;
  return data.user ?? null;
}

/**
 * Backward-compatible helper:
 * - pass Request -> extracts Bearer token
 * - pass string  -> uses it as token
 */
export async function getUserFromBearer(
  reqOrToken: Request | string
): Promise<{ user: User | null; token: string | null }> {
  const token =
    typeof reqOrToken === "string"
      ? reqOrToken
      : (() => {
          const auth = reqOrToken.headers.get("authorization") || "";
          return auth.startsWith("Bearer ") ? auth.slice(7) : null;
        })();

  if (!token) return { user: null, token: null };

  const user = await getUserFromBearerToken(token);
  return { user, token };
}
