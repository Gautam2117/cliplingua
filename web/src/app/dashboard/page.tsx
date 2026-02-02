"use client";

import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";
import { useRouter } from "next/navigation";
import JobClient from "@/components/JobClient";

type Profile = {
  id: string;
  email: string | null;
  credits: number;
};

type JobRow = {
  id: string;
  youtube_url: string;
  worker_job_id: string;
  status: string;
  credits_spent: number;
  created_at: string;
};

export default function DashboardPage() {
  const router = useRouter();
  const [profile, setProfile] = useState<Profile | null>(null);
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [msg, setMsg] = useState<string | null>(null);

  async function load() {
    setMsg(null);

    const { data: sess } = await supabase.auth.getSession();
    if (!sess.session) {
      router.replace("/login");
      return;
    }

    const { data: userData, error: userErr } = await supabase.auth.getUser();
    if (userErr || !userData.user) {
      router.replace("/login");
      return;
    }

    const uid = userData.user.id;

    const { data: prof, error: profErr } = await supabase
      .from("profiles")
      .select("id,email,credits")
      .eq("id", uid)
      .single();

    if (profErr) {
      setMsg(profErr.message);
      return;
    }

    setProfile(prof as any);

    const { data: jobRows, error: jobsErr } = await supabase
      .from("user_jobs")
      .select("id,youtube_url,worker_job_id,status,credits_spent,created_at")
      .order("created_at", { ascending: false })
      .limit(20);

    if (jobsErr) {
      setMsg(jobsErr.message);
      return;
    }

    setJobs((jobRows || []) as any);
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function signOut() {
    await supabase.auth.signOut();
    router.replace("/login");
  }

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="max-w-3xl mx-auto">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-bold">Dashboard</h1>
            <p className="text-sm opacity-70 mt-1">
              {profile ? `Credits: ${profile.credits}` : "Loading..."}
            </p>
          </div>

          <button onClick={signOut} className="border rounded-md px-4 py-2">
            Sign out
          </button>
        </div>

        <div className="mt-8 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Create a job</h2>
          <p className="text-sm opacity-70 mt-1">
            Each job costs 1 credit for now.
          </p>
          <div className="mt-4">
            <JobClient onJobCreated={load} onCreditsChanged={load} />
          </div>
        </div>

        <div className="mt-8 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Recent jobs</h2>

          {jobs.length === 0 ? (
            <p className="text-sm opacity-70 mt-3">No jobs yet.</p>
          ) : (
            <div className="mt-4 space-y-3">
              {jobs.map((j) => (
                <div key={j.id} className="border rounded-lg p-4">
                  <p className="text-sm">
                    <span className="font-semibold">Status:</span> {j.status}
                  </p>
                  <p className="text-sm mt-1 break-all">
                    <span className="font-semibold">URL:</span> {j.youtube_url}
                  </p>
                  <p className="text-xs opacity-70 mt-2">
                    Worker job id: <span className="font-mono">{j.worker_job_id}</span>
                  </p>
                </div>
              ))}
            </div>
          )}
        </div>

        {msg && <p className="mt-5 text-sm opacity-80 whitespace-pre-wrap">{msg}</p>}
      </div>
    </main>
  );
}
