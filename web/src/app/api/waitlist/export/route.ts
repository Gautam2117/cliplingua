import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabase-admin";

function toCSV(rows: { email: string; created_at: string }[]) {
  const header = "email,created_at\n";
  const body = rows
    .map((r) => {
      const email = `"${String(r.email).replace(/"/g, '""')}"`;
      const createdAt = `"${String(r.created_at).replace(/"/g, '""')}"`;
      return `${email},${createdAt}`;
    })
    .join("\n");
  return header + body + "\n";
}

export async function GET() {
  const { data, error } = await supabaseAdmin
    .from("waitlist")
    .select("email,created_at")
    .order("created_at", { ascending: false })
    .limit(5000);

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  const csv = toCSV(data ?? []);
  return new NextResponse(csv, {
    status: 200,
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": 'attachment; filename="cliplingua-waitlist.csv"',
      "Cache-Control": "no-store",
    },
  });
}
