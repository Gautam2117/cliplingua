import { Suspense } from "react";
import DashboardClient from "./DashboardClient";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default function DashboardPage() {
  return (
    <Suspense fallback={<div className="min-h-screen px-6 py-10">Loading...</div>}>
      <DashboardClient />
    </Suspense>
  );
}
