(() => {
  const REQUEST_TYPE = 'NIM_TRANSFER_FETCH_BLOB_REQUEST';
  const RESPONSE_TYPE = 'NIM_TRANSFER_FETCH_BLOB_RESPONSE';
  const READY_TYPE = 'NIM_TRANSFER_BRIDGE_READY';

  function postReady() {
    window.postMessage({ type: READY_TYPE, ok: true }, '*');
  }

  async function blobUrlToDataUrl(blobUrl) {
    const response = await fetch(String(blobUrl || ''), { credentials: 'include' });
    if (!response.ok) {
      throw new Error(`blob fetch failed (${response.status})`);
    }
    const blob = await response.blob();
    const dataUrl = await new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error('blob read failed'));
      reader.onload = () => resolve(String(reader.result || ''));
      reader.readAsDataURL(blob);
    });
    return { dataUrl, mimeType: blob.type || 'application/octet-stream' };
  }

  window.addEventListener('message', async (event) => {
    if (event.source !== window) return;
    const data = event.data || {};
    if (data.type === READY_TYPE && data.ping) {
      postReady();
      return;
    }
    if (data.type !== REQUEST_TYPE) return;
    const requestId = String(data.requestId || '');
    try {
      const res = await blobUrlToDataUrl(data.blobUrl || '');
      window.postMessage({ type: RESPONSE_TYPE, ok: true, requestId, dataUrl: res.dataUrl, mimeType: res.mimeType }, '*');
    } catch (error) {
      window.postMessage({ type: RESPONSE_TYPE, ok: false, requestId, message: String(error?.message || error || 'blob fetch failed') }, '*');
    }
  });

  postReady();
})();
