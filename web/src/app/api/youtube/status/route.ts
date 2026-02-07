import { NextResponse } from "next/server";
import { getUserFromBearer } from "@/lib/supabaseUserFromBearer";
import { supabaseAdmin } from "@/lib/supabase-admin";
 
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(req: Request) {
  const { user } = await getUserFromBearer(req);
  if (!user) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

  const admin = supabaseAdmin();
  const { data } = await admin
    .from("user_oauth_tokens")
    .select("refresh_token")
    .eq("user_id", user.id)
    .eq("provider", "youtube")
    .maybeSingle();

  const connected = !!data?.refresh_token;
  return NextResponse.json({ connected });
}
