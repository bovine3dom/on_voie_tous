// ==UserScript==
// @name         On Voie Tous
// @namespace    http://tampermonkey.net/
// @version      0.1
// @description  Exposes platform information on garesetconnexions.sncf
// @author       bovine3dom
// @match        https://www.garesetconnexions.sncf/*
// @run-at       document-start
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    const originalFetch = window.fetch;
    function makeTracksActive(obj) {
        if (!obj) return;
        if (Array.isArray(obj)) {
            obj.forEach(makeTracksActive);
        } else if (typeof obj === 'object') {
            for (const key in obj) {
                if (key === 'isTrackactive' && obj[key] === false) {
                    obj[key] = true;
                }
                makeTracksActive(obj[key]);
            }
        }
    }

    window.fetch = async function(...args) {
        const [resource] = args;
        const response = await originalFetch.apply(this, args);
        if (typeof resource === 'string' && resource.includes('/schedule-table/')) {
            const clonedResponse = response.clone();
            try {
                const data = await clonedResponse.json();
                makeTracksActive(data);

                return new Response(JSON.stringify(data), {
                    status: response.status,
                    statusText: response.statusText,
                    headers: response.headers
                });
            } catch (err) {
                return response;
            }
        }
        return response;
    };
})();
