// Cloudflare Pages Function: proxy /recording -> Cloud Run (southamerica-east1).
// Elimina la copia estática de recording.html en la raíz del repo (que divergía
// silenciosamente de pipeline/static/recording.html); ahora hay una sola fuente
// de verdad servida por Cloud Run.
const ORIGIN = "https://familia-pipeline-rxvtynuftq-rj.a.run.app";

export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  const target = ORIGIN + url.pathname + url.search;
  return fetch(new Request(target, request));
}
