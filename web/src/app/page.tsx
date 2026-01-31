import JobClient from "@/components/JobClient";

export default function Home() {
  return (
    <main className="min-h-screen flex items-center justify-center px-6">
      <div className="max-w-2xl w-full">
        <p className="text-sm uppercase tracking-wider opacity-70">ClipLingua</p>

        <h1 className="text-4xl md:text-5xl font-bold mt-3 leading-tight">
          Turn any video into Shorts in 25 languages.
        </h1>

        <p className="mt-4 text-lg opacity-80">
          Paste a YouTube link. We download, extract audio, and generate artifacts.
        </p>

        <div className="mt-8">
          <JobClient />
        </div>
      </div>
    </main>
  );
}
