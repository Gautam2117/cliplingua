import { NextResponse } from "next/server";
import { OAuth2Client } from "google-auth-library";
import { supabaseAdmin } from "@/lib/supabaseAdmin";
import { verifyYTState } from "@/lib/ytState";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function oauthClient() {
  const clientId = process.env.GOOGLE_CLIENT_ID || "";
  const clientSecret = process.env.GOOGLE_CLIENT_SECRET || "";
  const redirectUri = process.env.GOOGLE_REDIRECT_URI || "";
  if (!clientId || !clientSecret || !redirectUri) throw new Error("Missing Google OAuth env");
  return new OAuth2Client({ clientId, clientSecret, redirectUri });
}

function htmlClose(success: boolean, msg: string) {
  const safeMsg = msg.replace(/</g, "&lt;").replace(/>/g, "&gt;");
  return `<!doctype html>
<html>
  <head><meta charset="utf-8"><title>YouTube Connect</title></head>
  <body style="font-family: system-ui; padding: 24px;">
    <p>${safeMsg}</p>
    <script>
      try {
        if (window.opener) {
          window.opener.postMessage({ type: "YT_OAUTH_DONE", success: ${success ? "true" : "false"} }, window.location.origin);
        }
      } catch (e) {}
      try { window.close(); } catch (e) {}
    </script>
    <p>You can close this tab.</p>
  </body>
</html>`;
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const code = String(searchParams.get("code") || "");
    const state = String(searchParams.get("state") || "");
    const err = String(searchParams.get("error") || "");

    if (err) {
      return new NextResponse(htmlClose(false, `OAuth error: ${err}`), {
        status: 200,
        headers: { "Content-Type": "text/html; charset=utf-8" },
      });
    }

    if (!code || !state) {
      return NextResponse.json({ error: "Missing code/state" }, { status: 400 });
    }

    const secret = process.env.YT_STATE_SECRET || "";
    if (!secret) return NextResponse.json({ error: "Missing YT_STATE_SECRET" }, { status: 500 });

    const payload = verifyYTState(state, secret);
    const userId = payload.uid;

    const client = oauthClient();
    const { tokens } = await client.getToken(code);

    const refreshToken = tokens.refresh_token || null;
    const accessToken = tokens.access_token || null;
    const expiryTs = tokens.expiry_date ? Math.floor(tokens.expiry_date / 1000) : null;

    const admin = supabaseAdmin();

    // If Google does not return refresh_token on reconnect, keep existing refresh_token
    let finalRefresh = refreshToken;
    if (!finalRefresh) {
      const { data: existing } = await admin
        .from("user_oauth_tokens")
        .select("refresh_token")
        .eq("user_id", userId)
        .eq("provider", "youtube")
        .maybeSingle();
      finalRefresh = existing?.refresh_token || null;
    }

    await admin.from("user_oauth_tokens").upsert(
      {
        user_id: userId,
        provider: "youtube",
        refresh_token: finalRefresh,
        access_token: accessToken,
        expiry_ts: expiryTs,
      },
      { onConflict: "user_id,provider" }
    );

    return new NextResponse(htmlClose(true, "YouTube connected successfully."), {
      status: 200,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  } catch (e: any) {
    return new NextResponse(htmlClose(false, e?.message || "Callback failed"), {
      status: 200,
      headers: { "Content-Type": "text/html; charset=utf-8" },
    });
  }
}
