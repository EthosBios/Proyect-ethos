// Cloudflare Pages Function: /r/{token} → redirect to /recording?token={token}
// on the same CF Pages domain so the browser URL has ?token= and the JS can read it.
// Cloud Run's own /r/{token} endpoint does the same redirect but to the CR domain,
// so we short-circuit here instead of proxying through CR.
export async function onRequest(context) {
  const { request } = context;
  const url = new URL(request.url);
  // pathname is /r/{token}; split off the token segment
  const token = url.pathname.replace(/^\/r\//, '').split('/')[0];
  if (!token) {
    return new Response('Token requerido', { status: 400 });
  }
  const dest = new URL('/recording', url.origin);
  dest.searchParams.set('token', token);
  return Response.redirect(dest.toString(), 307);
}
