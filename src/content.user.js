// ==UserScript==
// @name         On Voie Tous
// @namespace    http://tampermonkey.net/
// @version      0.2
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

    const PREDICT_SERVER_URL = window.PREDICT_SERVER_URL || 'http://localhost:8000';
    const PREDICT_TIMEOUT_MS = 2000;
    const MIN_PROBABILITY = 0.10;

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
            return numA + 1 === numB;
        }
        return a.charCodeAt(0) + 1 === b.charCodeAt(0);
    }

    function formatPlatforms(probabilities) {
        const filtered = probabilities.filter(p => p.prob >= MIN_PROBABILITY);

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

        const all = [...highProb, ...grouped];
        const sorted = all.sort((a, b) => b.prob - a.prob);

        return sorted.map(p => `${p.platform} (${Math.round(p.prob * 100)}%)`).join(', ');
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

    function getMostCommonUic(trains) {
        if (!trains || !Array.isArray(trains)) {
            return null;
        }
        const uicCounts = {};
        for (const train of trains) {
            const uic = train.uic;
            if (uic) {
                uicCounts[uic] = (uicCounts[uic] || 0) + 1;
            }
        }
        let mostCommon = null;
        let maxCount = 0;
        for (const uic in uicCounts) {
            if (uicCounts[uic] > maxCount) {
                maxCount = uicCounts[uic];
                mostCommon = uic;
            }
        }
        return mostCommon;
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

        const station = getMostCommonUic(departureTrains);
        if (!station) {
            return;
        }

        const ts = new Date().toISOString();

        const payload = {
            ts: ts,
            station: station,
            data: departureTrains,
        };

        const result = await callPredictServer(payload);

        if (!result || !result.predictions) {
            console.warn('[On Voie Tous] No predictions in response');
            return;
        }

        showBanner();
        injectStyles();

        let predIndex = 0;
        for (const train of trains) {
            if (train.direction !== 'Departure') continue;
            if (!train.platform) continue;

            const prediction = result.predictions[predIndex];
            predIndex++;
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
