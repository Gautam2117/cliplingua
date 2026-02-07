import { NextResponse } from "next/server";
import crypto from "crypto";
import { OAuth2Client } from "google-auth-library";
import { getUserFromBearer } from "@/lib/supabaseUserFromBearer";
import { signYTState } from "@/lib/ytState";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function oauthClient() {
  const clientId = process.env.GOOGLE_CLIENT_ID || "";
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET || "";
  const redirectUri = process.env.GOOGLE_REDIRECT_URI || "";
  if (!clientId || !clientSecret || !redirectUri) throw new Error("Missing Google OAuth env");
  return new OAuth2Client({ clientId, clientSecret, redirectUri });
}

export async function GET(req: Request) {
  const { user } = await getUserFromBearer(req);
  if (!user) return NextResponse.json({ error: "Unauthorized" }, { status: 401 });

  const secret = process.env.YT_STATE_SECRET || "";
  if (!secret) return NextResponse.json({ error: "Missing YT_STATE_SECRET" }, { status: 500 });

  const nonce = crypto.randomBytes(16).toString("hex");
  const exp = Math.floor(Date.now() / 1000) + 10 * 60; // 10 min
  const state = signYTState({ uid: user.id, exp, nonce }, secret);

  const client = oauthClient();
  const url = client.generateAuthUrl({
    access_type: "offline",
    prompt: "consent",
    scope: ["https://www.googleapis.com/auth/youtube.upload"],
    state,
  });

  return NextResponse.json({ url });
}
