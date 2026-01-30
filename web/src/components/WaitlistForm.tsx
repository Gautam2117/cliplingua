"use client";

import { useState } from "react";
import { supabase } from "@/lib/supabase";

export default function WaitlistForm() {
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState<"idle" | "ok" | "error">("idle");

  async function join() {
    setStatus("idle");
    if (!email.trim()) return;

    const { error } = await supabase.from("waitlist").insert({ email });
    if (error) setStatus("error");
    else setStatus("ok");
  }

  return (
    <div className="w-full">
      <div className="flex gap-2">
        <input
          className="border rounded-md px-4 py-3 flex-1"
          type="email"
          placeholder="you@example.com"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
        <button onClick={join} className="rounded-md px-5 py-3 bg-black text-white">
          Join waitlist
        </button>
      </div>

      {status === "ok" && <p className="mt-2 text-sm text-green-700">You are in.</p>}
      {status === "error" && (
        <p className="mt-2 text-sm text-red-700">
          Something went wrong (or you already joined). Try again.
        </p>
      )}
    </div>
  );
}
