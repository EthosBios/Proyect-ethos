// Cloudflare Pages Function: proxy /r/* -> Cloud Run (southamerica-east1).
// Cloud Run /r/{token} returns a 307 to /recording?token=..., so we must
// follow redirects server-side to avoid sending the browser to the CR domain.
const ORIGIN = "https://familia-pipeline-rxvtynuftq-rj.a.run.app";

export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  const target = ORIGIN + url.pathname + url.search;
  return fetch(target, {
    method: request.method,
    headers: request.headers,
    redirect: "follow",
  });
}
