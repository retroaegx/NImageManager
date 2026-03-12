function normLines(s){
  return String(s || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function ensureTrailingComma(s){
  let t = String(s || "").trim();
  if(!t) return "";
  t = t.replace(/[,\s]+$/g, "").trim();
  return t ? `${t}, ` : "";
}

export function joinKeep(tags){
  if(!Array.isArray(tags) || !tags.length) return "";
  return tags.map(t => t?.raw_one || t?.canonical || t?.text || "").filter(Boolean).join(", ") + ", ";
}

export function joinPlain(tags){
  if(!Array.isArray(tags) || !tags.length) return "";
  return tags.map(t => t?.canonical || t?.text || t?.raw_one || "").filter(Boolean).join(", ") + ", ";
}

export function stripEmphasisFromPrompt(s){
  let t = normLines(s);
  if(!t) return "";
  const unwrap = (re) => {
    for(let i = 0; i < 10; i++){
      const nt = t.replace(re, "$1");
      if(nt === t) break;
      t = nt;
    }
  };
  unwrap(/\{([^{}]*)\}/g);
  unwrap(/\[([^\[\]]*)\]/g);
  unwrap(/\(([^()]*)\)/g);
  t = t.replace(/:\s*[-+]?\d+(?:\.\d+)?(?=\s*(,|$))/g, "");
  t = t.replace(/\s+/g, " ").replace(/\s*,\s*/g, ", ").trim();
  return t;
}

export function promptTextForCopyKeep(s){
  const t = normLines(s);
  if(!t) return "";
  const flat = t.split("\n").map(x => x.trim()).filter(Boolean).join(", ");
  const norm = flat.replace(/\s*,\s*/g, ", ").replace(/\s+/g, " ").trim();
  return ensureTrailingComma(norm);
}

export function promptTextForCopyPlain(s){
  return promptTextForCopyKeep(stripEmphasisFromPrompt(s));
}
