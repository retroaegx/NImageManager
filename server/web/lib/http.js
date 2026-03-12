function _normalizeHeaders(headers){
  if(headers instanceof Headers) return new Headers(headers);
  return new Headers(headers || {});
}

export async function apiFetch(url, opts={}){
  const o = { credentials: "include", ...opts };
  const headers = _normalizeHeaders(o.headers);
  const hasBody = o.body !== undefined && o.body !== null;
  const isStringBody = typeof o.body === "string";
  if(hasBody && isStringBody && !headers.has("Content-Type")){
    headers.set("Content-Type", "application/json; charset=UTF-8");
  }
  o.headers = headers;
  const res = await fetch(url, o);
  if(res.status === 401){
    location.replace("/login.html");
    throw new Error("unauthorized");
  }
  if(res.status === 403){
    const target = (typeof window !== "undefined" && window.location && window.location.pathname !== "/") ? "/" : null;
    if(target) location.replace(target);
    throw new Error("forbidden");
  }
  return res;
}

export async function apiJson(res){
  const text = await res.text();
  if(!res.ok){
    const head = (text || "").slice(0, 140);
    throw new Error(`${res.status} ${head}`);
  }
  if(!text) return null;
  try{
    return JSON.parse(text);
  }catch(_e){
    const head = (text || "").slice(0, 140);
    throw new Error(`bad json: ${head}`);
  }
}

export async function safeJson(res){
  try{
    const text = await res.text();
    if(!text) return {};
    return JSON.parse(text) || {};
  }catch(_e){
    return {};
  }
}
