import { createClient } from "@supabase/supabase-js";
import { getSignedUrl } from "./getSignedUrl";   // you added earlier

const sb = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

export async function fetchClips(jobId: string) {
  const { data, error } = await sb
    .from("clip_segments")
    .select("id,start_sec,end_sec,storage_key")
    .eq("job_id", jobId)
    .order("start_sec");

  if (error) throw error;

  const clips = await Promise.all(
    (data ?? []).map(async (row) => ({
      ...row,
      signedUrl: await getSignedUrl(row.storage_key)
    }))
  );

  return clips;
}
