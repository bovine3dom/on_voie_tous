// Runs in the isolated extension world, not in ADIF's page context.
window.addEventListener('message', event => {
    const message = event.data;
    if (event.source !== window || event.origin !== location.origin ||
        message?.type !== 'on-voie-request' || typeof message.id !== 'string' ||
        !['/adif-web/stations', '/adif-web/predict'].includes(message.path)) return;
    chrome.runtime.sendMessage(message, result => {
        const failed = chrome.runtime.lastError;
        window.postMessage({type: 'on-voie-response', id: message.id, result: failed ? null : result}, location.origin);
    });
});
