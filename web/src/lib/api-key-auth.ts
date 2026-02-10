import crypto from "crypto";
import { supabaseAdmin } from "@/lib/supabase-admin";

export type ApiKeyAuth = {
  orgId: string;
  prefix: string;
};

export function readBearer(req: Request) {
  const h = req.headers.get("authorization") || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m?.[1]?.trim() || "";
}

export function parseCliplinguaApiKey(apiKey: string) {
  // format: clp_<PREFIX>_<SECRET>
  if (!apiKey.startsWith("clp_")) return null;
  const parts = apiKey.split("_");
  if (parts.length < 3) return null;
  const prefix = parts[1]?.trim();
  if (!prefix || prefix.length < 6) return null;
  return { prefix };
}

export async function requireApiKey(req: Request): Promise<ApiKeyAuth> {
  const apiKey = readBearer(req);
  const parsed = parseCliplinguaApiKey(apiKey);
  if (!apiKey || !parsed) throw new Error("Missing or invalid API key");

  const hash = crypto.createHash("sha256").update(apiKey).digest("hex");

  const { data, error } = await supabaseAdmin
    .from("org_api_keys")
    .select("org_id, prefix, revoked_at")
    .eq("key_hash", hash)
    .is("revoked_at", null)
    .maybeSingle();

  if (error || !data?.org_id) throw new Error("Invalid API key");

  return { orgId: data.org_id, prefix: data.prefix };
}
