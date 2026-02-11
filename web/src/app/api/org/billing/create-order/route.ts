import { NextResponse } from "next/server";
import { supabaseAuthed } from "@/lib/supabase-server";
import { supabaseAdmin } from "@/lib/supabase-admin";
import { getUserFromBearerToken } from "@/lib/supabaseUserFromBearer";

export const runtime = "nodejs";

type Body = { kind: "seats" | "api"; seatsDelta?: number };

function jsonError(message: string, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

export async function POST(req: Request) {
  try {
    const auth = req.headers.get("authorization") || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
    if (!token) return jsonError("Missing bearer token", 401);

    const user = await getUserFromBearerToken(token);
    if (!user) return jsonError("Not authenticated", 401);

    const body = (await req.json().catch(() => null)) as Body | null;
    const kind = body?.kind;
    if (kind !== "seats" && kind !== "api") return jsonError("Invalid kind", 400);

    const sb = supabaseAuthed(token);

    const { data: profile, error: pErr } = await sb
      .from("profiles")
      .select("active_org_id")
      .eq("id", user.id)
      .single();

    if (pErr) return jsonError(pErr.message, 400);

    const orgId = profile?.active_org_id;
    if (!orgId) return jsonError("No active org", 400);

    const { data: isAdmin, error: aErr } = await sb.rpc("is_org_admin", { p_org_id: orgId });
    if (aErr) return jsonError(aErr.message, 400);
    if (!isAdmin) return jsonError("Admin required", 403);

    const seatPriceInr = Number.parseInt(process.env.SEAT_PRICE_INR || "299", 10);
    const apiPriceInr = Number.parseInt(process.env.API_ENABLE_PRICE_INR || "999", 10);

    const seatsDeltaRaw = kind === "seats" ? Number(body?.seatsDelta || 0) : 0;
    const seatsDelta = Math.max(0, Math.floor(seatsDeltaRaw));

    if (kind === "seats") {
      if (seatsDelta <= 0) return jsonError("seatsDelta must be >= 1", 400);
      if (seatsDelta > 100) return jsonError("seatsDelta too large", 400);
    }

    if (kind === "api") {
      const { data: orgRow, error: oErr } = await sb
        .from("organizations")
        .select("api_enabled")
        .eq("id", orgId)
        .single();
      if (oErr) return jsonError(oErr.message, 400);
      if (orgRow?.api_enabled) return jsonError("API already enabled", 400);
    }

    const amountInr = kind === "seats" ? seatsDelta * seatPriceInr : apiPriceInr;
    if (!Number.isFinite(amountInr) || amountInr <= 0) return jsonError("Invalid amount", 400);

    const keyId = process.env.RAZORPAY_KEY_ID || "";
    const keySecret = process.env.RAZORPAY_KEY_SECRET || "";
    if (!keyId || !keySecret) return jsonError("Razorpay keys missing", 500);

    const amountPaise = amountInr * 100;

    const rp = await fetch("https://api.razorpay.com/v1/orders", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Basic " + Buffer.from(`${keyId}:${keySecret}`).toString("base64"),
      },
      body: JSON.stringify({
        amount: amountPaise,
        currency: "INR",
        receipt: `org_${orgId}_${Date.now()}`,
        payment_capture: 1,
        notes: { orgId, kind, seatsDelta: String(seatsDelta) },
      }),
    });

    const order = await rp.json().catch(() => null);
    if (!rp.ok) {
      const desc = order?.error?.description || order?.message || "Razorpay error";
      return jsonError(desc, 400);
    }

    const { data: created, error: insErr } = await supabaseAdmin
      .from("org_orders")
      .insert({
        org_id: orgId,
        kind,
        seats_delta: seatsDelta,
        amount_paise: amountPaise,
        provider: "razorpay",
        provider_order_id: order.id,
        status: "created",
        created_by: user.id,
      })
      .select("id")
      .single();

    if (insErr) return jsonError(insErr.message, 400);

    return NextResponse.json({
      razorpayKeyId: keyId,
      amount: amountPaise,
      currency: "INR",
      razorpayOrderId: order.id,
      orgOrderId: created.id,
    });
  } catch (e: any) {
    return jsonError(e?.message || "Server error", 500);
  }
}
