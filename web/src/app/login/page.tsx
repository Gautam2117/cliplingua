"use client";

import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const router = useRouter();
  const [mode, setMode] = useState<"login" | "signup">("login");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");

  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      if (data.session) router.replace("/dashboard");
    });
  }, [router]);

  async function submit() {
    setMsg(null);
    setBusy(true);
    try {
      if (!email.trim() || !password) {
        setMsg("Enter email and password");
        return;
      }

      if (mode === "signup") {
        const { error } = await supabase.auth.signUp({
          email: email.trim(),
          password,
        });
        if (error) throw error;
        setMsg("Signup success. Check email if confirmation is enabled. Then login.");
        setMode("login");
      } else {
        const { error } = await supabase.auth.signInWithPassword({
          email: email.trim(),
          password,
        });
        if (error) throw error;
        router.replace("/dashboard");
      }
    } catch (e: any) {
      setMsg(e?.message || "Auth failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="min-h-screen flex items-center justify-center px-6">
      <div className="w-full max-w-md border rounded-xl p-6">
        <h1 className="text-2xl font-bold">ClipLingua</h1>
        <p className="text-sm opacity-70 mt-1">
          {mode === "login" ? "Sign in to your dashboard" : "Create your account"}
        </p>

        <div className="mt-6 space-y-3">
          <input
            className="border rounded-md px-4 py-3 w-full"
            placeholder="Email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
          <input
            className="border rounded-md px-4 py-3 w-full"
            placeholder="Password"
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />

          <button
            onClick={submit}
            disabled={busy}
            className="w-full rounded-md px-4 py-3 bg-black text-white disabled:opacity-50"
          >
            {busy ? "Please wait..." : mode === "login" ? "Sign in" : "Sign up"}
          </button>

          <button
            onClick={() => setMode(mode === "login" ? "signup" : "login")}
            className="w-full border rounded-md px-4 py-3"
            disabled={busy}
          >
            {mode === "login" ? "Create account" : "Already have an account"}
          </button>

          {msg && <p className="text-sm opacity-80 whitespace-pre-wrap">{msg}</p>}
        </div>
      </div>
    </main>
  );
}
