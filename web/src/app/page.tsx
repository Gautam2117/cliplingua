import WaitlistForm from "@/components/WaitlistForm";

export default function Home() {
  return (
    <main className="min-h-screen flex items-center justify-center px-6">
      <div className="max-w-2xl w-full">
        <p className="text-sm uppercase tracking-wider opacity-70">ClipLingua</p>

        <h1 className="text-4xl md:text-5xl font-bold mt-3 leading-tight">
          Turn any video into Shorts in 25 languages.
        </h1>

        <p className="mt-4 text-lg opacity-80">
          Paste a YouTube link or upload a video. We auto-clip, dub, and burn captions.
          Ready to publish to Reels, Shorts, TikTok.
        </p>

        <div className="mt-8">
          <WaitlistForm />
        </div>

        <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-3 text-sm opacity-80">
          <div className="border rounded-lg p-4">Auto-clip 15 to 60s</div>
          <div className="border rounded-lg p-4">Voice + captions</div>
          <div className="border rounded-lg p-4">Bulk exports for agencies</div>
        </div>
      </div>
    </main>
  );
}
