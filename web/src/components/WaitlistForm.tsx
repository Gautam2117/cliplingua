"use client";

import { useMemo, useState } from "react";
import { supabase } from "@/lib/supabase";

type Status = "idle" | "loading" | "ok" | "error";

export default function WaitlistForm() {
  const [email, setEmail] = useState("");
  const [status, setStatus] = useState<Status>("idle");
  const [message, setMessage] = useState<string>("");

  const cleanedEmail = useMemo(() => email.trim().toLowerCase(), [email]);

  const isValidEmail = useMemo(() => {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanedEmail);
  }, [cleanedEmail]);

  async function join() {
    setMessage("");

    if (!isValidEmail) {
      setStatus("error");
      setMessage("Enter a valid email.");
      return;
    }

    setStatus("loading");

    const { error } = await supabase.from("waitlist").insert({ email: cleanedEmail });

    if (!error) {
      setStatus("ok");
      setMessage("You are in.");
      return;
    }

    // Duplicate email in Supabase usually shows code 23505
    // We treat it as success to keep UX clean.
    const anyErr = error as unknown as { code?: string; message?: string };
    if (anyErr?.code === "23505") {
      setStatus("ok");
      setMessage("You are already on the waitlist.");
      return;
    }

    setStatus("error");
    setMessage("Something went wrong. Try again.");
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
          onKeyDown={(e) => {
            if (e.key === "Enter") join();
          }}
        />
        <button
          onClick={join}
          disabled={status === "loading"}
          className="rounded-md px-5 py-3 bg-black text-white disabled:opacity-60"
        >
          {status === "loading" ? "Joining..." : "Join waitlist"}
        </button>
      </div>

      {message && (
        <p
          className={[
            "mt-2 text-sm",
            status === "ok" ? "text-green-700" : "",
            status === "error" ? "text-red-700" : "",
          ].join(" ")}
        >
          {message}
        </p>
      )}
    </div>
  );
}
