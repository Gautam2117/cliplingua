"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabase";
import { ensureActiveOrg, requireAuthedSession } from "@/lib/org-client";

type Org = {
  id: string;
  name: string;
  credits: number;
  invite_code: string;
};

type MemberRow = {
  user_id: string;
  role: string;
  profiles?: { email: string | null } | null;
};

export default function TeamClient() {
  const router = useRouter();

  const [org, setOrg] = useState<Org | null>(null);
  const [members, setMembers] = useState<MemberRow[]>([]);
  const [msg, setMsg] = useState<string | null>(null);

  const [joinCode, setJoinCode] = useState("");
  const [joining, setJoining] = useState(false);

  const inviteCode = useMemo(() => org?.invite_code || "", [org]);

  async function load() {
    setMsg(null);

    try {
      const { user } = await requireAuthedSession();
      const orgId = await ensureActiveOrg(user.id);

      const { data: orgRow, error: orgErr } = await supabase
        .from("organizations")
        .select("id,name,credits,invite_code")
        .eq("id", orgId)
        .single();

      if (orgErr) throw new Error(orgErr.message);
      setOrg(orgRow as any);

      // Member list (requires RLS to allow org members to read org_members + profiles)
      // If your FK relation name differs, adjust the select alias.
      const { data: memRows, error: memErr } = await supabase
        .from("org_members")
        .select("user_id,role,profiles:profiles(email)")
        .eq("org_id", orgId);

      if (memErr) throw new Error(memErr.message);
      setMembers((memRows || []) as any);
    } catch (e: any) {
      const m = e?.message || "Failed to load team";
      setMsg(m);
      if (String(m).toLowerCase().includes("no session") || String(m).toLowerCase().includes("invalid session")) {
        router.replace("/login");
      }
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

  async function copyInvite() {
    try {
      await navigator.clipboard.writeText(inviteCode);
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

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="max-w-3xl mx-auto">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-bold">Team</h1>
            <p className="text-sm opacity-70 mt-1">Organizations, members, and invite codes.</p>

            <div className="mt-4 flex gap-3 text-sm">
              <Link className="underline" href="/dashboard">Dashboard</Link>
              <Link className="underline" href="/dashboard/team">Team</Link>
              <Link className="underline" href="/dashboard/api-keys">API keys</Link>
            </div>

            <div className="mt-4 text-sm opacity-85 space-y-1">
              {org ? (
                <>
                  <p>
                    Org: <span className="font-semibold">{org.name}</span> · Credits:{" "}
                    <span className="font-semibold">{org.credits}</span>
                  </p>
                  <div className="flex items-center gap-2">
                    <p className="text-xs opacity-80">
                      Invite code: <span className="font-mono">{org.invite_code}</span>
                    </p>
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

        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Join an organization</h2>
          <p className="text-sm opacity-70 mt-1">Paste an invite code to switch your active org.</p>

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

        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Members</h2>
          {members.length === 0 ? (
            <p className="text-sm opacity-70 mt-3">No members found.</p>
          ) : (
            <div className="mt-4 space-y-2">
              {members.map((m) => (
                <div key={m.user_id} className="border rounded-lg p-3 flex items-center justify-between">
                  <div className="text-sm">
                    <p className="font-medium">{m.profiles?.email || m.user_id}</p>
                    <p className="text-xs opacity-70">{m.user_id}</p>
                  </div>
                  <span className="text-xs border rounded-md px-2 py-1">{m.role}</span>
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
