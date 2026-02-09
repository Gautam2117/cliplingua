import ApiKeysClient from "./keys-client";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default function ApiKeysPage() {
  return <ApiKeysClient />;
}
