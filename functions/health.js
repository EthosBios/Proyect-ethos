// Cloudflare Pages Function: proxy /health -> Cloud Run (southamerica-east1).
// Cloudflare Pages ignora vercel.json; el proxy a un origin externo requiere
// una Function (los _redirects no pueden proxear fuera del propio sitio).
const ORIGIN = "https://familia-pipeline-rxvtynuftq-rj.a.run.app";

export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  const target = ORIGIN + url.pathname + url.search;
  return fetch(new Request(target, request));
}
