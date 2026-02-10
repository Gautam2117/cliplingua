export async function loadRazorpayScript(): Promise<boolean> {
  if (typeof window === "undefined") return false;

  const existing = document.getElementById("razorpay-checkout-js");
  if (existing) return true;

  return await new Promise<boolean>((resolve) => {
    const script = document.createElement("script");
    script.id = "razorpay-checkout-js";
    script.src = "https://checkout.razorpay.com/v1/checkout.js";
    script.async = true;
    script.onload = () => resolve(true);
    script.onerror = () => resolve(false);
    document.body.appendChild(script);
  });
}
