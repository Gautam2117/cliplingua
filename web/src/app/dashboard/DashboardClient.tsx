"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { supabase } from "@/lib/supabase";
import { ensureActiveOrg, requireAuthedSession } from "@/lib/org-client";
import { loadRazorpayScript } from "@/lib/razorpay-client";
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

  plan?: string | null;
  seats_purchased?: number | null;
  api_enabled?: boolean | null;
  api_rpm?: number | null;
  api_daily_jobs?: number | null;
  max_api_keys?: number | null;
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

type ApiUsageRow = {
  minute_used?: number | null;
  day_used?: number | null;
  minute_window_start?: string | null;
  day_date?: string | null;
  updated_at?: string | null;
};

function planLabel(p?: string | null) {
  const v = (p || "free").toLowerCase();
  if (v === "agency_plus") return "Agency Plus";
  if (v === "agency") return "Agency";
  if (v === "creator") return "Creator";
  return "Free";
}

export default function DashboardClient() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [profile, setProfile] = useState<Profile | null>(null);
  const [org, setOrg] = useState<Org | null>(null);
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [msg, setMsg] = useState<string | null>(null);

  const [membersCount, setMembersCount] = useState<number>(0);
  const [usage, setUsage] = useState<ApiUsageRow | null>(null);

  const [joinCode, setJoinCode] = useState("");
  const [joining, setJoining] = useState(false);

  const [seatsToAdd, setSeatsToAdd] = useState<number>(1);
  const [billingBusy, setBillingBusy] = useState(false);

  const seatsPurchased = org?.seats_purchased ?? 1;
  const apiEnabled = !!org?.api_enabled;
  const apiRpm = org?.api_rpm ?? null;
  const apiDailyJobs = org?.api_daily_jobs ?? null;
  const maxApiKeys = org?.max_api_keys ?? null;

  const seatUsageText = useMemo(() => {
    return `${membersCount} / ${seatsPurchased}`;
  }, [membersCount, seatsPurchased]);

  async function authedFetch(path: string, init?: RequestInit) {
    const { token } = await requireAuthedSession();
    return fetch(path, {
      ...init,
      headers: {
        ...(init?.headers || {}),
        Authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      cache: "no-store",
    });
  }

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

  async function load() {
    setMsg(null);

    const joinError = searchParams?.get("join_error");
    if (joinError) {
      try {
        setMsg(decodeURIComponent(joinError));
      } catch {
        setMsg(joinError);
      }
    }

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
      orgId = await ensureActiveOrg(uid);
    } catch (e: any) {
      setMsg(e?.message || "Failed to ensure organization");
      return;
    }

    const { data: profRow, error: profErr } = await supabase
      .from("profiles")
      .select("id,email,active_org_id")
      .eq("id", uid)
      .single();

    if (!profErr) setProfile(profRow as any);

    const { data: orgRow, error: orgErr } = await supabase
      .from("organizations")
      .select("id,name,credits,invite_code,plan,seats_purchased,api_enabled,api_rpm,api_daily_jobs,max_api_keys")
      .eq("id", orgId)
      .single();

    if (orgErr) {
      setMsg(orgErr.message);
      return;
    }
    setOrg(orgRow as any);

    // Seats usage
    const { count, error: cErr } = await supabase
      .from("org_members")
      .select("user_id", { count: "exact", head: true })
      .eq("org_id", orgId);

    if (!cErr) setMembersCount(count || 0);

    // API usage widget (optional)
    try {
      const { data: uRow } = await supabase
        .from("org_api_usage")
        .select("minute_bucket,minute_count,day,day_count,updated_at")
        .eq("org_id", orgId)
        .maybeSingle();

      if (uRow) setUsage(uRow as any);
      else setUsage(null);
    } catch {
      setUsage(null);
    }

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

  async function startCheckout(kind: "seats" | "api") {
    setMsg(null);

    const seatsDelta = Math.max(1, Math.floor(seatsToAdd || 1));
    if (kind === "seats" && seatsDelta > 100) {
      setMsg("Too many seats at once (max 100)");
      return;
    }

    setBillingBusy(true);
    try {
      const ok = await loadRazorpayScript();
      if (!ok) throw new Error("Failed to load Razorpay checkout");

      const r = await authedFetch("/api/org/billing/create-order", {
        method: "POST",
        body: JSON.stringify(kind === "seats" ? { kind, seatsDelta } : { kind }),
      });
      const j = await r.json().catch(() => ({}));
      if (!r.ok) throw new Error(j?.error || "Failed to create order");

      const { razorpayKeyId, razorpayOrderId, amount, currency, orgOrderId } = j;

      const RazorpayCtor = (window as any).Razorpay;
      if (!RazorpayCtor) throw new Error("Razorpay not available");

      const description =
        kind === "seats" ? `Add ${seatsDelta} seat(s)` : "Enable API for this org";

      const rz = new RazorpayCtor({
        key: razorpayKeyId,
        amount,
        currency,
        order_id: razorpayOrderId,
        name: "ClipLingua",
        description,
        handler: async (resp: any) => {
          const vr = await authedFetch("/api/org/billing/verify", {
            method: "POST",
            body: JSON.stringify({
              orgOrderId,
              razorpay_order_id: resp.razorpay_order_id,
              razorpay_payment_id: resp.razorpay_payment_id,
              razorpay_signature: resp.razorpay_signature,
            }),
          });
          const vj = await vr.json().catch(() => ({}));
          if (!vr.ok) throw new Error(vj?.error || "Verify failed");

          setMsg("Payment successful. Updated org settings.");
          await load();
        },
        modal: {
          ondismiss: () => {
            setBillingBusy(false);
          },
        },
        prefill: profile?.email ? { email: profile.email } : undefined,
      });

      rz.open();
    } catch (e: any) {
      setMsg(e?.message || "Checkout failed");
    } finally {
      setBillingBusy(false);
    }
  }

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="max-w-3xl mx-auto">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-bold">Dashboard</h1>

            <div className="mt-4 flex gap-3 text-sm">
              <Link className="underline" href="/dashboard">Dashboard</Link>
              <Link className="underline" href="/dashboard/team">Team</Link>
              <Link className="underline" href="/dashboard/api-keys">API keys</Link>
            </div>

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
              ) : (
                <p>Loading...</p>
              )}
            </div>
          </div>

          <button onClick={signOut} className="border rounded-md px-4 py-2">
            Sign out
          </button>
        </div>

        {/* PLAN + SEATS + API */}
        <div className="mt-6 border rounded-xl p-6">
          <div className="flex items-start justify-between gap-4">
            <div>
              <h2 className="text-lg font-semibold">Plan</h2>
              <p className="text-sm opacity-70 mt-1">
                {org ? (
                  <>
                    <span className="font-semibold">{planLabel(org.plan)}</span>{" "}
                    · Seats: <span className="font-semibold">{seatUsageText}</span>{" "}
                    · API: <span className="font-semibold">{apiEnabled ? "Enabled" : "Disabled"}</span>
                  </>
                ) : (
                  "Loading plan..."
                )}
              </p>

              {org && (
                <p className="text-xs opacity-70 mt-2">
                  Limits: RPM {apiRpm ?? "-"} · Daily jobs {apiDailyJobs ?? "-"} · Max API keys {maxApiKeys ?? "-"}
                </p>
              )}
            </div>

            <div className="flex flex-col gap-2">
              <div className="flex gap-2">
                <input
                  type="number"
                  min={1}
                  max={100}
                  value={seatsToAdd}
                  onChange={(e) => setSeatsToAdd(Number(e.target.value))}
                  className="border rounded-md px-3 py-2 w-28"
                />
                <button
                  disabled={billingBusy}
                  onClick={() => startCheckout("seats")}
                  className="border rounded-md px-4 py-2"
                >
                  {billingBusy ? "Working..." : "Upgrade seats"}
                </button>
              </div>

              <button
                disabled={billingBusy || apiEnabled}
                onClick={() => startCheckout("api")}
                className="border rounded-md px-4 py-2 disabled:opacity-60"
              >
                {apiEnabled ? "API enabled" : billingBusy ? "Working..." : "Enable API"}
              </button>
            </div>
          </div>

          {usage && org && (
            <div className="mt-4 border rounded-lg p-4">
              <p className="text-sm font-semibold">API usage</p>
              <p className="text-sm opacity-80 mt-1">
                Minute: <span className="font-semibold">{usage.minute_used ?? 0}</span> / {apiRpm ?? "-"}
              </p>
              <p className="text-sm opacity-80">
                Daily: <span className="font-semibold">{usage.day_used ?? 0}</span> / {apiDailyJobs ?? "-"}
              </p>
              {usage.updated_at && (
                <p className="text-xs opacity-70 mt-2">Updated: {new Date(usage.updated_at).toLocaleString()}</p>
              )}
            </div>
          )}
        </div>

        {/* JOIN ORG */}
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

        {/* CREATE JOB */}
        <div className="mt-8 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Create a job</h2>
          <p className="text-sm opacity-70 mt-1">Each job costs 1 credit for now (charged to org).</p>
          <div className="mt-4">
            <JobClient onJobCreated={load} onCreditsChanged={load} />
          </div>
        </div>

        {/* JOBS */}
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
