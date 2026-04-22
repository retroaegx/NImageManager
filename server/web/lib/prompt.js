function normLines(s){
  return String(s || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function ensureTrailingComma(s){
  let textValue = String(s || "").trim();
  if(!textValue) return "";
  textValue = textValue.replace(/[,\s]+$/g, "").trim();
  return textValue ? `${textValue}, ` : "";
}

export function joinKeep(tags){
  if(!Array.isArray(tags) || !tags.length) return "";
  return tags.map(tag => tag?.raw_one || "").filter(Boolean).join(", ") + ", ";
}

export function joinPlain(tags){
  if(!Array.isArray(tags) || !tags.length) return "";
  return tags.map(tag => tag?.text || "").filter(Boolean).join(", ") + ", ";
}

export function stripEmphasisFromPrompt(s){
  let textValue = normLines(s);
  if(!textValue) return "";
  const unwrap = (re) => {
    for(let i = 0; i < 10; i++){
      const nextText = textValue.replace(re, "$1");
      if(nextText === textValue) break;
      textValue = nextText;
    }
  };
  unwrap(/\{([^{}]*)\}/g);
  unwrap(/\[([^\[\]]*)\]/g);
  unwrap(/\(([^()]*)\)/g);
  textValue = textValue.replace(/:\s*[-+]?\d+(?:\.\d+)?(?=\s*(,|$))/g, "");
  textValue = textValue.replace(/\s+/g, " ").replace(/\s*,\s*/g, ", ").trim();
  return textValue;
}

export function promptTextForCopyKeep(s){
  const textValue = normLines(s);
  if(!textValue) return "";
  const flat = textValue.split("\n").map(x => x.trim()).filter(Boolean).join(", ");
  const norm = flat.replace(/\s*,\s*/g, ", ").replace(/\s+/g, " ").trim();
  return ensureTrailingComma(norm);
}

export function promptTextForCopyPlain(s){
  return promptTextForCopyKeep(stripEmphasisFromPrompt(s));
}
