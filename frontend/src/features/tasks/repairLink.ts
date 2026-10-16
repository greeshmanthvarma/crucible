export function repairLink(url: string | null | undefined) {
  if (!url) return undefined;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" && parsed.hostname === "github.com"
      ? parsed.href
      : undefined;
  } catch {
    return undefined;
  }
}
