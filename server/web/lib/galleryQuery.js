function addCommonFilters(params, preview){
  if(preview.creator) params.set("creator", preview.creator);
  if(preview.software) params.set("software", preview.software);
  if((preview.tags || []).length) params.set("tags", preview.tags.join(","));
  if((preview.tags_not || []).length) params.set("tags_not", preview.tags_not.join(","));
  if(preview.date_from) params.set("date_from", preview.date_from);
  if(preview.date_to) params.set("date_to", preview.date_to);
  if(preview.dedup_only) params.set("dedup_only", "1");
  if(preview.bm_any) params.set("bm_any", "1");
  if(preview.bm_list_id) params.set("bm_list_id", String(preview.bm_list_id));
  if(preview.sort) params.set("sort", preview.sort);
}

export function buildScrollQuery(preview, { cursor=null, includeTotal=1 }={}){
  const params = new URLSearchParams();
  addCommonFilters(params, preview);
  params.set("limit", String(preview.limit || 30));
  params.set("include_total", includeTotal ? "1" : "0");
  if(cursor) params.set("cursor", cursor);
  return params.toString();
}

export function buildPageQueryCore(preview, page){
  const params = new URLSearchParams();
  addCommonFilters(params, preview);
  params.set("page", String(page || 1));
  params.set("limit", "16");
  return params.toString();
}

export function buildPageQuery(preview, page, { includeTotal=1 }={}){
  const qs = buildPageQueryCore(preview, page);
  return `${qs}&include_total=${includeTotal ? 1 : 0}`;
}
