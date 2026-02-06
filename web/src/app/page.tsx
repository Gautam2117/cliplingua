// src/app/page.tsx
"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import JobClient from "@/components/JobClient";
import { supabase } from "@/lib/supabase";

export default function Home() {
  const router = useRouter();

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      if (data.session) router.replace("/dashboard");
    });
  }, [router]);

  return (
    <main className="min-h-screen flex items-center justify-center px-6">
      <div className="max-w-2xl w-full">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm uppercase tracking-wider opacity-70">ClipLingua</p>
          <button
            onClick={() => router.push("/login")}
            className="border rounded-md px-4 py-2"
          >
            Login
          </button>
        </div>

        <h1 className="text-4xl md:text-5xl font-bold mt-3 leading-tight">
          Turn any video into Shorts in multiple languages.
        </h1>

        <p className="mt-4 text-lg opacity-80">
          Paste a YouTube link. We download, extract audio, generate artifacts, and can dub into supported languages.
        </p>

        <div className="mt-8">
          <JobClient />
        </div>
      </div>
    </main>
  );
}
