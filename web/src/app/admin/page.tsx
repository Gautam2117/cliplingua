export default function AdminPage() {
  return (
    <main className="min-h-screen px-6 py-12 flex items-start justify-center">
      <div className="w-full max-w-2xl">
        <h1 className="text-2xl font-bold">Admin</h1>
        <p className="mt-2 opacity-70 text-sm">
          Download your waitlist as CSV.
        </p>

        <div className="mt-6 border rounded-xl p-4">
          <a
            href="/api/waitlist/export"
            className="inline-flex items-center justify-center rounded-md px-4 py-2 bg-black text-white"
          >
            Download waitlist CSV
          </a>

          <p className="mt-3 text-xs opacity-60">
            Tip: This is protected by Basic Auth.
          </p>
        </div>
      </div>
    </main>
  );
}
