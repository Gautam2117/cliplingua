// src/app/api/billing/verify/route.ts
import { NextResponse } from "next/server";
import crypto from "crypto";
import { supabaseAuthed } from "@/lib/supabase-server";
import { supabaseAdmin } from "@/lib/supabase-admin";
import { getUserFromBearer } from "@/lib/supabaseUserFromBearer";

export const runtime = "nodejs";

type Body = {
  orgOrderId: string;
  razorpay_order_id: string;
  razorpay_payment_id: string;
  razorpay_signature: string;
};

function jsonError(message: string, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

function safeEqualHex(a: string, b: string) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

export async function POST(req: Request) {
  try {
    const auth = req.headers.get("authorization") || "";
    const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;
    if (!token) return jsonError("Missing bearer token", 401);

    const user = await getUserFromBearer(token);
    if (!user) return jsonError("Not authenticated", 401);

    const body = (await req.json().catch(() => null)) as Body | null;
    if (!body?.orgOrderId) return jsonError("Missing orgOrderId", 400);
    if (!body.razorpay_order_id || !body.razorpay_payment_id || !body.razorpay_signature) {
      return jsonError("Missing Razorpay fields", 400);
    }

    const keySecret = process.env.RAZORPAY_KEY_SECRET || "";
    if (!keySecret) return jsonError("Razorpay key secret missing", 500);

    const { data: orderRow, error: oErr } = await supabaseAdmin
      .from("org_orders")
      .select("*")
      .eq("id", body.orgOrderId)
      .single();

    if (oErr) return jsonError(oErr.message, 400);
    if (!orderRow) return jsonError("Order not found", 404);

    // Strong binding: verify the orgOrderId is for the same Razorpay order
    if (String(orderRow.provider_order_id) !== String(body.razorpay_order_id)) {
      return jsonError("Order mismatch", 400);
    }

    // Admin check for the org in this order
    const sb = supabaseAuthed(token);
    const { data: isAdmin, error: aErr } = await sb.rpc("is_org_admin", { p_org_id: orderRow.org_id });
    if (aErr) return jsonError(aErr.message, 400);
    if (!isAdmin) return jsonError("Admin required", 403);

    if (orderRow.status === "paid") return NextResponse.json({ ok: true });

    const payload = `${body.razorpay_order_id}|${body.razorpay_payment_id}`;
    const expected = crypto.createHmac("sha256", keySecret).update(payload).digest("hex");

    if (!safeEqualHex(expected, body.razorpay_signature)) {
      return jsonError("Invalid signature", 400);
    }

    // Mark paid
    const { error: updErr } = await supabaseAdmin
      .from("org_orders")
      .update({
        status: "paid",
        payment_id: body.razorpay_payment_id,
        paid_at: new Date().toISOString(),
      })
      .eq("id", body.orgOrderId)
      .neq("status", "paid");

    if (updErr) return jsonError(updErr.message, 400);

    // Apply entitlements
    if (orderRow.kind === "seats") {
      await supabaseAdmin.rpc("set_org_seats_delta", {
        p_org_id: orderRow.org_id,
        p_delta: orderRow.seats_delta,
      });
    } else if (orderRow.kind === "api") {
      await supabaseAdmin.from("organizations").update({ api_enabled: true }).eq("id", orderRow.org_id);
    }

    return NextResponse.json({ ok: true });
  } catch (e: any) {
    return jsonError(e?.message || "Server error", 500);
  }
}
