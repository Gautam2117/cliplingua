"use client";

export const dynamic = "force-dynamic";
export const revalidate = 0;

import { Suspense, useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";
import { useRouter, useSearchParams } from "next/navigation";
import JobClient from "@/components/JobClient";

type Profile = {
  id: string;
  email: string | null;
  active_org_id: string | null;
};

type Org = {
  id: string;
  name: string;
  credits: number;
  invite_code: string;
};

type JobRow = {
  id: string;
  youtube_url: string;
  worker_job_id: string;
  status: string;
  credits_spent: number;
  created_at: string;
  org_id: string;
};

function DashboardInner() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [profile, setProfile] = useState<Profile | null>(null);
  const [org, setOrg] = useState<Org | null>(null);
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [msg, setMsg] = useState<string | null>(null);

  const [joinCode, setJoinCode] = useState("");
  const [joining, setJoining] = useState(false);

  async function syncRecentStatuses(token: string, rows: JobRow[]) {
    const candidates = rows.filter((r) => r.status !== "done" && r.status !== "error").slice(0, 10);

    await Promise.allSettled(
      candidates.map(async (r) => {
        await fetch(`/api/clip/status?jobId=${encodeURIComponent(r.worker_job_id)}`, {
          cache: "no-store",
          headers: { Authorization: `Bearer ${token}` },
        });
      })
    );
  }

  async function ensureOrgActive(userId: string) {
    const { data: prof, error: profErr } = await supabase
      .from("profiles")
      .select("id,email,active_org_id")
      .eq("id", userId)
      .single();

    if (profErr) throw new Error(profErr.message);

    let activeOrgId = (prof as any)?.active_org_id as string | null;

    if (!activeOrgId) {
      const { error: bootErr } = await supabase.rpc("bootstrap_org");
      if (bootErr) throw new Error(bootErr.message);

      const { data: prof2, error: profErr2 } = await supabase
        .from("profiles")
        .select("id,email,active_org_id")
        .eq("id", userId)
        .single();

      if (profErr2) throw new Error(profErr2.message);

      setProfile(prof2 as any);
      activeOrgId = (prof2 as any)?.active_org_id as string | null;
    } else {
      setProfile(prof as any);
    }

    if (!activeOrgId) throw new Error("active_org_id missing after bootstrap");
    return activeOrgId;
  }

  async function load() {
    setMsg(null);

    const joinError = searchParams?.get("join_error");
    if (joinError) setMsg(decodeURIComponent(joinError));

    const { data: sess } = await supabase.auth.getSession();
    const token = sess.session?.access_token;

    if (!sess.session || !token) {
      router.replace("/login");
      return;
    }

    const { data: userData, error: userErr } = await supabase.auth.getUser();
    if (userErr || !userData.user) {
      router.replace("/login");
      return;
    }

    const uid = userData.user.id;

    let orgId = "";
    try {
      orgId = await ensureOrgActive(uid);
    } catch (e: any) {
      setMsg(e?.message || "Failed to ensure organization");
      return;
    }

    const { data: orgRow, error: orgErr } = await supabase
      .from("organizations")
      .select("id,name,credits,invite_code")
      .eq("id", orgId)
      .single();

    if (orgErr) {
      setMsg(orgErr.message);
      return;
    }
    setOrg(orgRow as any);

    const { data: jobRows, error: jobsErr } = await supabase
      .from("user_jobs")
      .select("id,youtube_url,worker_job_id,status,credits_spent,created_at,org_id")
      .eq("org_id", orgId)
      .order("created_at", { ascending: false })
      .limit(20);

    if (jobsErr) {
      setMsg(jobsErr.message);
      return;
    }

    const rows = (jobRows || []) as any as JobRow[];
    setJobs(rows);

    try {
      await syncRecentStatuses(token, rows);

      const { data: fresh } = await supabase
        .from("user_jobs")
        .select("id,youtube_url,worker_job_id,status,credits_spent,created_at,org_id")
        .eq("org_id", orgId)
        .order("created_at", { ascending: false })
        .limit(20);

      if (fresh) setJobs(fresh as any);
    } catch {
      // ignore
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function signOut() {
    await supabase.auth.signOut();
    router.replace("/login");
  }

  async function joinOrg() {
    setMsg(null);
    const code = joinCode.trim();
    if (!code) {
      setMsg("Enter an invite code");
      return;
    }

    setJoining(true);
    try {
      const { error } = await supabase.rpc("join_org", { invite_code: code });
      if (error) {
        setMsg(error.message);
        return;
      }
      setJoinCode("");
      await load();
    } catch (e: any) {
      setMsg(e?.message || "Join failed");
    } finally {
      setJoining(false);
    }
  }

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="max-w-3xl mx-auto">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-bold">Dashboard</h1>

            <div className="mt-2 text-sm opacity-80 space-y-1">
              {org ? (
                <>
                  <p>
                    Org: <span className="font-semibold">{org.name}</span> · Credits:{" "}
                    <span className="font-semibold">{org.credits}</span>
                  </p>
                  <p className="text-xs opacity-80">
                    Invite code: <span className="font-mono">{org.invite_code}</span>
                  </p>
                </>
              ) : profile ? (
                <p>Loading org...</p>
              ) : (
                <p>Loading...</p>
              )}
            </div>
          </div>

          <button onClick={signOut} className="border rounded-md px-4 py-2">
            Sign out
          </button>
        </div>

        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Agency mode</h2>
          <p className="text-sm opacity-70 mt-1">Join an organization using an invite code.</p>

          <div className="mt-4 flex gap-2">
            <input
              value={joinCode}
              onChange={(e) => setJoinCode(e.target.value)}
              placeholder="Enter invite code"
              className="border rounded-md px-3 py-2 w-full"
            />
            <button onClick={joinOrg} disabled={joining} className="border rounded-md px-4 py-2">
              {joining ? "Joining..." : "Join"}
            </button>
          </div>
        </div>

        <div className="mt-8 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Create a job</h2>
          <p className="text-sm opacity-70 mt-1">Each job costs 1 credit for now (charged to org).</p>
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

export default function DashboardPage() {
  return (
    <Suspense fallback={<div className="min-h-screen px-6 py-10">Loading...</div>}>
      <DashboardInner />
    </Suspense>
  );
}
