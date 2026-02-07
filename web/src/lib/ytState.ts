import crypto from "crypto";

function b64url(buf: Buffer) {
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function b64urlToBuf(s: string) {
  const pad = s.length % 4 === 0 ? "" : "=".repeat(4 - (s.length % 4));
  const base64 = s.replace(/-/g, "+").replace(/_/g, "/") + pad;
  return Buffer.from(base64, "base64");
}

export type YTStatePayload = {
  uid: string;
  exp: number; // unix seconds
  nonce: string;
};

export function signYTState(payload: YTStatePayload, secret: string) {
  const body = Buffer.from(JSON.stringify(payload), "utf8");
  const sig = crypto.createHmac("sha256", secret).update(body).digest();
  return `${b64url(body)}.${b64url(sig)}`;
}

export function verifyYTState(state: string, secret: string): YTStatePayload {
  const [bodyB64, sigB64] = state.split(".");
  if (!bodyB64 || !sigB64) throw new Error("Bad state format");

  const body = b64urlToBuf(bodyB64);
  const sig = b64urlToBuf(sigB64);

  const expected = crypto.createHmac("sha256", secret).update(body).digest();
  if (sig.length !== expected.length || !crypto.timingSafeEqual(sig, expected)) {
    throw new Error("Invalid state signature");
  }

  const payload = JSON.parse(body.toString("utf8")) as YTStatePayload;
  const now = Math.floor(Date.now() / 1000);
  if (!payload?.uid || !payload?.exp || payload.exp < now) throw new Error("State expired");

  return payload;
}
