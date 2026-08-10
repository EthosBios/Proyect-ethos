// Cloudflare Pages Function: proxy /auth/* -> Cloud Run (southamerica-east1).
const ORIGIN = "https://familia-pipeline-rxvtynuftq-rj.a.run.app";

export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  const target = ORIGIN + url.pathname + url.search;
  return fetch(new Request(target, request));
}
