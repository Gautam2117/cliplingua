"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { supabase } from "@/lib/supabase";
import { ensureActiveOrg, loadOrgContext, requireAuthedSession, type OrgContext } from "@/lib/org-client";

type ApiKeyRow = {
  prefix: string;
  name: string;
  created_at: string;
  revoked_at: string | null;
};

export default function ApiKeysClient() {
  const router = useRouter();

  const [ctx, setCtx] = useState<OrgContext | null>(null);
  const [rows, setRows] = useState<ApiKeyRow[]>([]);
  const [name, setName] = useState("");
  const [newKey, setNewKey] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const activeKeysCount = useMemo(() => rows.filter((r) => !r.revoked_at).length, [rows]);
  const canCreate = useMemo(() => {
    if (!ctx) return false;
    if (!ctx.isAdmin) return false;
    if (!ctx.api.enabled) return false;
    if (!ctx.api.maxKeys) return false;
    return activeKeysCount < ctx.api.maxKeys;
  }, [ctx, activeKeysCount]);

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

  async function load() {
    setMsg(null);
    setNewKey(null);

    try {
      const { user } = await requireAuthedSession();
      const orgId = await ensureActiveOrg(user.id);

      const c = await loadOrgContext(orgId);
      setCtx(c);

      const r = await authedFetch("/api/org/api-keys", { method: "GET" });
      const j = await r.json();
      if (!r.ok) throw new Error(j?.error || "Failed to load keys");

      setRows((j?.keys || []) as ApiKeyRow[]);
    } catch (e: any) {
      const m = e?.message || "Failed to load";
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

  async function createKey() {
    setMsg(null);
    setNewKey(null);

    const n = name.trim();
    if (!n) {
      setMsg("Enter a name (example: Zapier, Client A, Internal)");
      return;
    }

    if (!ctx?.isAdmin) {
      setMsg("Admin required to create API keys.");
      return;
    }

    if (!ctx?.api.enabled) {
      setMsg("API is disabled for this org.");
      return;
    }

    if (!ctx?.api.maxKeys || activeKeysCount >= ctx.api.maxKeys) {
      setMsg("Max API keys reached. Upgrade required.");
      return;
    }

    setBusy(true);
    try {
      const r = await authedFetch("/api/org/api-keys", {
        method: "POST",
        body: JSON.stringify({ name: n }),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j?.error || "Create failed");

      setNewKey(String(j.apiKey || ""));
      setName("");
      await load();
    } catch (e: any) {
      setMsg(e?.message || "Create failed");
    } finally {
      setBusy(false);
    }
  }

  async function revoke(prefix: string) {
    setMsg(null);
    setNewKey(null);

    setBusy(true);
    try {
      const r = await authedFetch("/api/org/api-keys", {
        method: "DELETE",
        body: JSON.stringify({ prefix }),
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j?.error || "Revoke failed");
      await load();
    } catch (e: any) {
      setMsg(e?.message || "Revoke failed");
    } finally {
      setBusy(false);
    }
  }

  async function signOut() {
    await supabase.auth.signOut();
    router.replace("/login");
  }

  return (
    <main className="min-h-screen px-6 py-10">
      <div className="max-w-4xl mx-auto">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h1 className="text-3xl font-bold">API keys</h1>
            <p className="text-sm opacity-70 mt-1">Used for Bulk API access (Agency automation).</p>

            <div className="mt-4 flex gap-3 text-sm">
              <Link className="underline" href="/dashboard">Dashboard</Link>
              <Link className="underline" href="/dashboard/team">Team</Link>
              <Link className="underline" href="/dashboard/api-keys">API keys</Link>
            </div>

            {ctx && (
              <div className="mt-4 text-sm opacity-85 space-y-1">
                <p>
                  Org: <span className="font-semibold">{ctx.org.name}</span> · Plan:{" "}
                  <span className="font-mono">{ctx.org.plan || "free"}</span>
                </p>
                <p className="text-xs opacity-70">
                  API enabled: <span className="font-mono">{ctx.api.enabled ? "true" : "false"}</span> · Max keys:{" "}
                  <span className="font-mono">{ctx.api.maxKeys || 0}</span> · Active keys:{" "}
                  <span className="font-mono">{activeKeysCount}</span>
                </p>
              </div>
            )}
          </div>

          <button onClick={signOut} className="border rounded-md px-4 py-2">
            Sign out
          </button>
        </div>

        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Create a new key</h2>

          {!ctx ? (
            <p className="text-sm opacity-70 mt-3">Loading...</p>
          ) : !ctx.isAdmin ? (
            <p className="text-sm opacity-70 mt-3">Only org admins can create or revoke keys.</p>
          ) : !ctx.api.enabled ? (
            <p className="text-sm opacity-70 mt-3">API is disabled for this org. Enable it to create keys.</p>
          ) : (
            <p className="text-sm opacity-70 mt-3">
              Active keys: {activeKeysCount} / {ctx.api.maxKeys || 0}
            </p>
          )}

          <div className="mt-4 flex gap-2">
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Key name (example: Zapier)"
              className="border rounded-md px-3 py-2 w-full"
              disabled={busy || !ctx}
            />
            <button disabled={busy || !canCreate} onClick={createKey} className="border rounded-md px-4 py-2">
              {busy ? "Working..." : "Create"}
            </button>
          </div>

          {newKey && (
            <div className="mt-4 border rounded-lg p-4">
              <p className="text-sm font-semibold">Copy this now. You will not see it again.</p>
              <p className="mt-2 font-mono text-sm break-all">{newKey}</p>
            </div>
          )}

          {ctx && ctx.isAdmin && ctx.api.enabled && ctx.api.maxKeys > 0 && activeKeysCount >= ctx.api.maxKeys && (
            <p className="mt-3 text-xs opacity-70">Max keys reached. Upgrade required.</p>
          )}
        </div>

        <div className="mt-6 border rounded-xl p-6">
          <h2 className="text-lg font-semibold">Existing keys</h2>

          {rows.length === 0 ? (
            <p className="text-sm opacity-70 mt-3">No keys created yet.</p>
          ) : (
            <div className="mt-4 space-y-2">
              {rows.map((k) => (
                <div key={k.prefix} className="border rounded-lg p-4 flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <p className="text-sm font-medium">{k.name}</p>
                    <p className="text-xs opacity-70">
                      Prefix: <span className="font-mono">{k.prefix}</span>
                    </p>
                    <p className="text-xs opacity-70">Revoked: {k.revoked_at ? "Yes" : "No"}</p>
                  </div>

                  <button
                    disabled={busy || !!k.revoked_at || !ctx?.isAdmin}
                    onClick={() => revoke(k.prefix)}
                    className="border rounded-md px-4 py-2 text-sm"
                  >
                    Revoke
                  </button>
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
