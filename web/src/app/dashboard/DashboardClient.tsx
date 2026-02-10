"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { supabase } from "@/lib/supabase";
import JobClient from "@/components/JobClient";
import { ensureActiveOrg, loadOrgContext, requireAuthedSession, type OrgContext, type OrgPlan } from "@/lib/org-client";

type JobRow = {
  id: string;
  youtube_url: string;
  worker_job_id: string;
  status: string;
  credits_spent: number;
  created_at: string;
  org_id: string;
};

function planLabel(p: OrgPlan | null) {
  if (!p) return "free";
  return p;
}

function planBadgeClasses(p: OrgPlan | null) {
  const base = "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-semibold";
  if (p === "agency_plus") return `${base} bg-black text-white border-black`;
  if (p === "agency") return `${base} bg-white text-black border-black`;
  if (p === "creator") return `${base} bg-white text-black border-gray-300`;
  return `${base} bg-white text-black border-gray-200`;
}

function clampPct(n: number) {
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(100, n));
}

declare global {
  interface Window {
    Razorpay?: any;
  }
}

async function loadRazorpayScript(): Promise<boolean> {
  if (typeof window === "undefined") return false;
  if (window.Razorpay) return true;

  await new Promise<void>((resolve, reject) => {
    const s = document.createElement("script");
    s.src = "https://checkout.razorpay.com/v1/checkout.js";
    s.async = true;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error("Failed to load Razorpay"));
    document.body.appendChild(s);
  });

  return !!window.Razorpay;
}

export default function DashboardClient() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [ctx, setCtx] = useState<OrgContext | null>(null);
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [msg, setMsg] = useState<string | null>(null);

  const [joinCode, setJoinCode] = useState("");
  const [joining, setJoining] = useState(false);

  const seatsUsed = ctx?.membersUsed ?? 0;
  const seatsPurchased = ctx?.seatsPurchased ?? 0;
  const seatsPct = useMemo(() => {
    if (!seatsPurchased) return 0;
    return clampPct((seatsUsed / seatsPurchased) * 100);
  }, [seatsUsed, seatsPurchased]);

  const apiMinutePct = useMemo(() => {
    const used = ctx?.api.minuteUsed ?? 0;
    const limit = ctx?.api.minuteLimit ?? 0;
    if (!limit) return 0;
    return clampPct((used / limit) * 100);
  }, [ctx]);

  const apiDailyPct = useMemo(() => {
    const used = ctx?.api.dailyUsed ?? 0;
    const limit = ctx?.api.dailyLimit ?? 0;
    if (!limit) return 0;
    return clampPct((used / limit) * 100);
  }, [ctx]);

  const syncRecentStatuses = useCallback(async (token: string, rows: JobRow[]) => {
    const candidates = rows.filter((r) => r.status !== "done" && r.status !== "error").slice(0, 10);

    await Promise.allSettled(
      candidates.map(async (r) => {
        await fetch(`/api/clip/status?jobId=${encodeURIComponent(r.worker_job_id)}`, {
          cache: "no-store",
          headers: { Authorization: `Bearer ${token}` },
        });
      })
    );
  }, []);

  const load = useCallback(async () => {
    setMsg(null);

    const joinError = searchParams?.get("join_error");
    if (joinError) {
      try {
        setMsg(decodeURIComponent(joinError));
      } catch {
        setMsg(joinError);
      }
    }

    let token = "";
    let uid = "";
    try {
      const s = await requireAuthedSession();
      token = s.token;
      uid = s.user.id;
    } catch {
      router.replace("/login");
      return;
    }

    let orgId = "";
    try {
      orgId = await ensureActiveOrg(uid);
    } catch (e: any) {
      setMsg(e?.message || "Failed to ensure organization");
      return;
    }

    try {
      const c = await loadOrgContext(orgId);
      setCtx(c);
    } catch (e: any) {
      setMsg(e?.message || "Failed to load org");
      return;
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
  }, [router, searchParams, syncRecentStatuses]);

  useEffect(() => {
    load();
  }, [load]);

  async function signOut() {
    await supabase.auth.signOut();
    router.replace("/login");
  }

  async function copyInvite() {
    const code = ctx?.org.invite_code || "";
    if (!code) return;

    try {
      await navigator.clipboard.writeText(code);
      setMsg("Invite code copied");
      setTimeout(() => setMsg(null), 1500);
    } catch {
      setMsg("Copy failed (clipboard blocked)");
    }
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
      if (error) throw new Error(error.message);
      setJoinCode("");
      await load();
    } catch (e: any) {
      setMsg(e?.message || "Join failed");
    } finally {
      setJoining(false);
    }
  }

  // Optional billing wiring:
  // - POST /api/billing/create-order { kind: "seats", seatsDelta: number } or { kind: "api" }
  // - POST /api/billing/verify with razorpay response
  async function startSeatUpgrade(seatsDelta: number) {
    setMsg(null);

    const ok = await loadRazorpayScript().catch((e) => {
      setMsg(e?.message || "Razorpay failed to load");
      return false;
    });
    if (!ok) return;

    let token = "";
    try {
      token = (await requireAuthedSession()).token;
    } catch {
      router.replace("/login");
      return;
    }

    const r = await fetch("/api/billing/create-order", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify({ kind: "seats", seatsDelta }),
    });

    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      setMsg(String(j?.error || "Failed to create order"));
      return;
    }

    const rz = new window.Razorpay({
      key: j.razorpayKeyId,
      amount: j.amount,
      currency: j.currency || "INR",
      order_id: j.razorpayOrderId,
      name: "ClipLingua",
      description: `Upgrade seats (+${seatsDelta})`,
      handler: async (resp: any) => {
        const vr = await fetch("/api/billing/verify", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
          body: JSON.stringify({
            orgOrderId: j.orgOrderId,
            razorpay_order_id: resp.razorpay_order_id,
            razorpay_payment_id: resp.razorpay_payment_id,
            razorpay_signature: resp.razorpay_signature,
          }),
        });

        const vj = await vr.json().catch(() => ({}));
        if (!vr.ok) {
          setMsg(String(vj?.error || "Payment verify failed"));
          return;
        }
        setMsg("Seats upgraded.");
        await load();
      },
      modal: { ondismiss: () => setMsg("Payment cancelled") },
    });

    rz.open();
  }

  async function enableApiPaid() {
    setMsg(null);

    const ok = await loadRazorpayScript().catch((e) => {
      setMsg(e?.message || "Razorpay failed to load");
      return false;
    });
    if (!ok) return;

    let token = "";
    try {
      token = (await requireAuthedSession()).token;
    } catch {
      router.replace("/login");
      return;
    }

    const r = await fetch("/api/billing/create-order", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: JSON.stringify({ kind: "api" }),
    });

    const j = await r.json().catch(() => ({}));
    if (!r.ok) {
      setMsg(String(j?.error || "Failed to create order"));
      return;
    }

    const rz = new window.Razorpay({
      key: j.razorpayKeyId,
      amount: j.amount,
      currency: j.currency || "INR",
      order_id: j.razorpayOrderId,
      name: "ClipLingua",
      description: "Enable Bulk API",
      handler: async (resp: any) => {
        const vr = await fetch("/api/billing/verify", {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
          body: JSON.stringify({
            orgOrderId: j.orgOrderId,
            razorpay_order_id: resp.razorpay_order_id,
            razorpay_payment_id: resp.razorpay_payment_id,
            razorpay_signature: resp.razorpay_signature,
          }),
        });

        const vj = await vr.json().catch(() => ({}));
        if (!vr.ok) {
          setMsg(String(vj?.error || "Payment verify failed"));
          return;
        }
        setMsg("API enabled.");
        await load();
      },
      modal: { ondismiss: () => setMsg("Payment cancelled") },
    });

    rz.open();
  }

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-bold">Dashboard</h1>

            <div className="mt-4 flex gap-3 text-sm">
              <Link className="underline" href="/dashboard">Dashboard</Link>
              <Link className="underline" href="/dashboard/team">Team</Link>
              <Link className="underline" href="/dashboard/api-keys">API keys</Link>
            </div>

            <div className="mt-3 text-sm opacity-85 space-y-1">
              {ctx ? (
                <>
                  <div className="flex flex-wrap items-center gap-2">
                    <p>
                      Org: <span className="font-semibold">{ctx.org.name}</span>
                    </p>
                    <span className={planBadgeClasses(ctx.org.plan)}>{planLabel(ctx.org.plan)}</span>
                    {ctx.isAdmin && (
                      <span className="text-xs opacity-70 border rounded-full px-2.5 py-1">admin</span>
                    )}
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    <p>
                      Credits: <span className="font-semibold">{ctx.org.credits}</span>
                    </p>

                    <span className="text-xs opacity-80">
                      Invite code: <span className="font-mono">{ctx.org.invite_code}</span>
                    </span>

                    <button onClick={copyInvite} className="border rounded-md px-3 py-1 text-xs">
                      Copy
                    </button>
                  </div>
                </>
              ) : (
                <p>Loading org...</p>
              )}
            </div>
          </div>

          <button onClick={signOut} className="border rounded-md px-4 py-2">
            Sign out
          </button>
        </div>

        {/* PLAN / SEATS / API */}
        <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="border rounded-xl p-5">
            <p className="text-sm opacity-70">Plan</p>
            <p className="mt-2 text-xl font-semibold">{ctx ? planLabel(ctx.org.plan) : "-"}</p>
            <p className="mt-1 text-xs opacity-70">Pricing lever: seats + API limits</p>
          </div>

          <div className="border rounded-xl p-5">
            <div className="flex items-center justify-between gap-2">
              <p className="text-sm opacity-70">Seats</p>
              {ctx?.isAdmin && (
                <button
                  onClick={() => startSeatUpgrade(1)}
                  className="border rounded-md px-3 py-1 text-xs"
                  disabled={!ctx}
                >
                  Upgrade +1
                </button>
              )}
            </div>

            <p className="mt-2 text-xl font-semibold">
              {ctx ? `${seatsUsed} / ${Math.max(1, seatsPurchased)}` : "-"}
            </p>

            <div className="mt-3 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
              <div className="h-full bg-black" style={{ width: `${seatsPurchased ? seatsPct : 0}%` }} />
            </div>

            <p className="mt-2 text-xs opacity-70">Members count / seats purchased</p>
          </div>

          <div className="border rounded-xl p-5">
            <div className="flex items-center justify-between gap-2">
              <p className="text-sm opacity-70">Bulk API</p>
              {ctx?.isAdmin && !ctx.api.enabled && (
                <button onClick={enableApiPaid} className="border rounded-md px-3 py-1 text-xs">
                  Enable
                </button>
              )}
              {ctx?.api.enabled && <span className="text-xs opacity-70 border rounded-full px-2.5 py-1">enabled</span>}
            </div>

            {ctx ? (
              <>
                <div className="mt-3">
                  <div className="flex items-center justify-between text-xs opacity-80">
                    <span>Minute</span>
                    <span className="font-mono">
                      {ctx.api.minuteUsed}/{ctx.api.minuteLimit || "-"}
                    </span>
                  </div>
                  <div className="mt-2 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
                    <div className="h-full bg-black" style={{ width: `${ctx.api.minuteLimit ? apiMinutePct : 0}%` }} />
                  </div>
                </div>

                <div className="mt-4">
                  <div className="flex items-center justify-between text-xs opacity-80">
                    <span>Daily jobs</span>
                    <span className="font-mono">
                      {ctx.api.dailyUsed}/{ctx.api.dailyLimit || "-"}
                    </span>
                  </div>
                  <div className="mt-2 h-2 w-full rounded-full bg-gray-100 overflow-hidden">
                    <div className="h-full bg-black" style={{ width: `${ctx.api.dailyLimit ? apiDailyPct : 0}%` }} />
                  </div>
                </div>

                <p className="mt-2 text-xs opacity-70">Live counters from org_api_usage</p>
              </>
            ) : (
              <p className="mt-2 text-sm opacity-70">Loading...</p>
            )}
          </div>
        </div>

        {/* JOIN ORG */}
        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Join an organization</h2>
          <p className="text-sm opacity-70 mt-1">Switch your active org by invite code.</p>

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
        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Create a job</h2>
          <p className="text-sm opacity-70 mt-1">Each job costs 1 credit for now (charged to org).</p>
          <div className="mt-4">
            <JobClient onJobCreated={load} onCreditsChanged={load} />
          </div>
        </div>

        {/* RECENT JOBS */}
        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Recent jobs</h2>

          {jobs.length === 0 ? (
            <p className="text-sm opacity-70 mt-3">No jobs yet.</p>
          ) : (
            <div className="mt-4 space-y-3">
              {jobs.map((j) => (
                <div key={j.id} className="border rounded-lg p-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-sm">
                      <span className="font-semibold">Status:</span> {j.status}
                    </p>
                    <p className="text-xs opacity-70">{new Date(j.created_at).toLocaleString()}</p>
                  </div>

                  <p className="text-sm mt-2 break-all">
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
