import { NextResponse } from "next/server";
import crypto from "crypto";
import { supabaseAdmin } from "@/lib/supabase-admin";

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

export async function POST(req: Request) {
  try {
    const body = (await req.json().catch(() => null)) as Body | null;
    if (!body?.orgOrderId) return jsonError("Missing orgOrderId", 400);

    const keySecret = process.env.RAZORPAY_KEY_SECRET || "";
    if (!keySecret) return jsonError("Razorpay key secret missing", 500);

    const payload = `${body.razorpay_order_id}|${body.razorpay_payment_id}`;
    const expected = crypto.createHmac("sha256", keySecret).update(payload).digest("hex");
    if (expected !== body.razorpay_signature) return jsonError("Invalid signature", 400);

    const { data: orderRow, error: oErr } = await supabaseAdmin
      .from("org_orders")
      .select("*")
      .eq("id", body.orgOrderId)
      .single();

    if (oErr) return jsonError(oErr.message, 400);

    // Make sure the payment is for the same Razorpay order we created
    if (orderRow.provider_order_id !== body.razorpay_order_id) return jsonError("Order mismatch", 400);

    if (orderRow.status === "paid") return NextResponse.json({ ok: true });

    await supabaseAdmin
      .from("org_orders")
      .update({
        status: "paid",
        payment_id: body.razorpay_payment_id,
        paid_at: new Date().toISOString(),
      })
      .eq("id", body.orgOrderId);

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
