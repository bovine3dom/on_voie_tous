// ==UserScript==
// @name         On Voie Tous
// @namespace    http://tampermonkey.net/
// @version      0.3
// @description  Predicts platforms on garesetconnexions.sncf
// @author       bovine3dom
// @match        https://www.garesetconnexions.sncf/*
// @run-at       document-start
// @updateURL    https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @downloadURL  https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @supportURL   https://github.com/bovine3dom/on_voie_tous/issues/
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    const PREDICT_SERVER_URL = 'https://compute.olie.science/on_voie_tous';
    const PREDICT_TIMEOUT_MS = 2000;
    const MIN_PROBABILITY = 0.1;
    const MAX_PLATFORMS = 2;

    function showBanner() {
        if (document.getElementById('on-voie-tous-banner')) return;

        const banner = document.createElement('div');
        banner.id = 'on-voie-tous-banner';
        banner.innerHTML = 'Platform predictions provided by <a href="https://github.com/bovine3dom/on_voie_tous">On Voie Tous</a>, an experimental extension unaffiliated with the SNCF.';
        banner.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            background: #f0ad4e;
            color: #333;
            padding: 8px 16px;
            text-align: center;
            font-size: 14px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            z-index: 999999;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        `;

        document.body.appendChild(banner);
    }

    function injectStyles() {
        if (document.getElementById('on-voie-tous-styles')) return;

        const style = document.createElement('style');
        style.id = 'on-voie-tous-styles';
        style.textContent = `
            .informationLine .wrapperLocation .contentLocation {
                padding: 1em !important;
            }
            @media screen and (min-width: 768px) {
                .informationLine .wrapperLocation .bothContentLocation {
                    display: flex !important;
                    flex-wrap: nowrap !important;
                    gap: 0.5em !important;
                    max-width: 100% !important;
                }
                .informationLine .wrapperLocation .contentLocation {
                    max-width: 50% !important;
                    overflow: hidden !important;
                    text-overflow: ellipsis !important;
                    white-space: nowrap !important;
                }
                .informationLine .wrapperLocation .contentLocation p {
                    overflow: hidden !important;
                    text-overflow: ellipsis !important;
                    white-space: nowrap !important;
                }
            }
        `;

        document.head.appendChild(style);
    }

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

    function isContiguous(a, b) {
        const numA = parseInt(a);
        const numB = parseInt(b);
        if (!isNaN(numA) && !isNaN(numB)) {
            return numA + 1 === numB;// || numA + 2 === numB; // some stations only have odd platforms. but it ruins other stations...
        }
        return a.charCodeAt(0) + 1 === b.charCodeAt(0);// || a.charCodeAt(0) + 2 === b.charCodeAt(0);
    }

    function formatPlatforms(probabilities) {
        const filtered = probabilities;

        const highProb = filtered.filter(p => p.prob >= 0.30);
        const lowProb = filtered.filter(p => p.prob < 0.30);

        lowProb.sort((a, b) => a.platform.localeCompare(b.platform, undefined, { numeric: true }));

        const grouped = [];
        let i = 0;
        while (i < lowProb.length) {
            const current = lowProb[i];
            let j = i + 1;
            while (j < lowProb.length && isContiguous(lowProb[j-1].platform, lowProb[j].platform)) {
                j++;
            }

            if (j > i + 1) {
                const group = lowProb.slice(i, j);
                const totalProb = group.reduce((sum, p) => sum + p.prob, 0);
                grouped.push({
                    platform: group[0].platform + '-' + group[group.length - 1].platform,
                    prob: totalProb
                });
            } else {
                grouped.push(current);
            }
            i = j;
        }

        const sorted = [...(highProb).sort((a, b) => b.prob - a.prob), ...(grouped).sort((a, b) => b.prob - a.prob)].filter(p => p.prob >= MIN_PROBABILITY);

        return sorted.map(p => `${p.platform} (${Math.round(p.prob * 100)}%)`).slice(0, MAX_PLATFORMS).join(', ');
    }

    async function callPredictServer(payload) {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), PREDICT_TIMEOUT_MS);

            const response = await fetch(`${PREDICT_SERVER_URL}/predict`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
                signal: controller.signal,
            });

            clearTimeout(timeoutId);

            if (!response.ok) {
                console.warn('[On Voie Tous] Predict server error:', response.status);
                return null;
            }

            return await response.json();
        } catch (err) {
            console.warn('[On Voie Tous] Predict server call failed:', err.message);
            return null;
        }
    }

    async function addPredictions(response) {
        let trains;
        if (Array.isArray(response)) {
            trains = response;
        } else if (response && response.data && Array.isArray(response.data)) {
            trains = response.data;
        } else {
            return;
        }

        if (trains.length === 0) {
            return;
        }

        const departureTrains = trains.filter(t => t.direction === 'Departure');
        if (departureTrains.length === 0) {
            return;
        }

        const uicGroups = {};
        for (const train of departureTrains) {
            const uic = train.uic;
            if (!uic) continue;
            if (!uicGroups[uic]) {
                uicGroups[uic] = [];
            }
            uicGroups[uic].push(train);
        }

        const ts = new Date().toISOString();
        const predictionsByUic = {};

        const results = [];
        for (const uic in uicGroups) {
            const group = uicGroups[uic];
            const payload = {
                ts: ts,
                station: uic,
                data: group,
            };

            results.push({uic, promise: callPredictServer(payload)});
        }
        await Promise.all(results.map(r => r.promise));
        for (const {uic, promise} of results) {
            const result = await promise;
            if (!result || !result.predictions) continue;
            predictionsByUic[uic] = result.predictions;
        }

        const hasPredictions = Object.keys(predictionsByUic).length > 0;
        if (!hasPredictions) {
            console.warn('[On Voie Tous] No predictions in response');
            return;
        }

        showBanner();
        injectStyles();

        for (const train of trains) {
            if (train.direction !== 'Departure') continue;
            if (!train.platform) continue;

            const uic = train.uic;
            if (!uic || !predictionsByUic[uic]) continue;

            const group = uicGroups[uic];
            const predIndex = group.indexOf(train);
            const prediction = predictionsByUic[uic][predIndex];
            if (!prediction) continue;

            const originalTrack = train.platform.track;
            const formattedPlatforms = formatPlatforms(prediction.probabilities);
            if (formattedPlatforms) {
                if (originalTrack && originalTrack !== formattedPlatforms) {
                    train.platform.track = `${originalTrack} | ${formattedPlatforms}`;
                } else {
                    train.platform.track = formattedPlatforms;
                }
                train.platform.isTrackactive = true;
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
                await addPredictions(data);

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
