import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase-server";

export async function POST(req: Request) {
  const { path } = (await req.json()) as { path?: string };

  if (!path) {
    return NextResponse.json({ error: "Missing path" }, { status: 400 });
  }

  const supabase = supabaseServer();

  // 10 minutes signed url
  const { data, error } = await supabase.storage
    .from("artifacts")
    .createSignedUrl(path, 60 * 10);

  if (error || !data?.signedUrl) {
    return NextResponse.json(
      { error: error?.message || "Failed to sign url" },
      { status: 500 }
    );
  }

  return NextResponse.json({ url: data.signedUrl });
}
