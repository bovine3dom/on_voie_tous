chrome.runtime.onMessage.addListener((message, sender, reply) => {
    if (!sender.url?.startsWith('https://www.adif.es/w/') ||
        message?.type !== 'on-voie-request' ||
        !['/adif-web/stations', '/adif-web/predict'].includes(message.path)) return;
    const body = message.path === '/adif-web/predict' ? JSON.stringify(message.payload) : undefined;
    if (body && body.length > 1000000) return;
    fetch('https://compute.olie.science/on_voie_tous' + message.path, {
        method: body ? 'POST' : 'GET',
        headers: {'Content-Type': 'application/json'},
        body,
        credentials: 'omit',
        redirect: 'error',
        signal: AbortSignal.timeout(2000),
    }).then(response => response.ok ? response.json() : null).then(reply, () => reply(null));
    return true;
});
