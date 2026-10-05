// ==UserScript==
// @name         On Voie Tous
// @namespace    http://tampermonkey.net/
// @version      0.5
// @description  Predicts platforms on SNCF and RFI departure boards
// @author       bovine3dom
// @match        https://www.garesetconnexions.sncf/*
// @match        https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor*
// @run-at       document-start
// @updateURL    https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @downloadURL  https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @supportURL   https://github.com/bovine3dom/on_voie_tous/issues/
// @grant        none
// ==/UserScript==

(function() {
    'use strict';

    const PREDICT_SERVER_URL = window.ON_VOIE_TOUS_SERVER || window.ON_VOIE_RFI_SERVER || 'https://compute.olie.science/on_voie_tous';
    const PREDICT_TIMEOUT_MS = 2000;
    const MIN_PROBABILITY = 0.1;
    const MAX_PLATFORMS = 2;

    if (window.location.hostname === 'iechub.rfi.it') {
        startRfi();
        return;
    }
    if (window.location.hostname !== 'www.garesetconnexions.sncf') return;

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
        if (/^\d+$/.test(a) && /^\d+$/.test(b)) return Number(a) + 1 === Number(b);
        return /^[A-Z]$/.test(a) && /^[A-Z]$/.test(b) && a.charCodeAt(0) + 1 === b.charCodeAt(0);
    }

    function formatPlatforms(probabilities) {
        const filtered = probabilities.filter(p => p.prob > 0.05);
        const high = filtered.filter(p => p.prob >= 0.30).sort((a, b) => b.prob - a.prob);
        const low = filtered.filter(p => p.prob < 0.30)
            .sort((a, b) => a.platform.localeCompare(b.platform, undefined, {numeric: true}));
        const grouped = [];
        for (let i = 0; i < low.length;) {
            let end = i + 1;
            while (end < low.length && isContiguous(low[end - 1].platform, low[end].platform)) end++;
            grouped.push({
                platform: end > i + 1 ? low[i].platform + '-' + low[end - 1].platform : low[i].platform,
                prob: low.slice(i, end).reduce((sum, p) => sum + p.prob, 0),
            });
            i = end;
        }
        return [...high, ...grouped.sort((a, b) => b.prob - a.prob)]
            .filter(p => p.prob >= MIN_PROBABILITY).slice(0, MAX_PLATFORMS)
            .map(p => `${p.platform} (${Math.round(p.prob * 100)}%)`).join(', ');
    }

    async function request(path, payload) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), PREDICT_TIMEOUT_MS);
        try {
            const response = await fetch(PREDICT_SERVER_URL + path, {
                ...(payload ? {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)} : {}),
                signal: controller.signal,
            });
            return response.ok ? await response.json() : null;
        } catch {
            return null;
        } finally {
            clearTimeout(timeout);
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

            results.push({uic, promise: request('/predict', payload)});
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

    function startRfi() {
        const MARKER = 'on-voie-rfi-estimate';
        if (window.__onVoieRfiActive) return;
        const url = new URL(window.location.href);
        const station = url.searchParams.get('placeId');
        if (!station || !/^\d+$/.test(station) || url.searchParams.get('arrivals')?.toLowerCase() === 'true') return;

        window.__onVoieRfiActive = true;
        const clean = text => (text || '').replace(/\s+/g, ' ').trim();
        const known = text => !['', '-', '--', '—', '?', 'N.D.', 'ND', 'NON DISPONIBILE'].includes(clean(text).toUpperCase());
        const cell = (row, id) => row.querySelector(`[id="${id}"]`);
        const official = element => clean(Array.from(element.childNodes)
            .filter(node => !node.classList?.contains(MARKER)).map(node => node.textContent).join(' '));

        function snapshot() {
            return Array.from(document.querySelectorAll('tr[name="treno"]')).flatMap(row => {
                const platform = cell(row, 'RBinario');
                const trainNumber = clean(cell(row, 'RTreno')?.textContent);
                const clock = clean(cell(row, 'ROrario')?.textContent);
                if (!platform || !trainNumber || !/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(clock)) return [];
                const carrier = cell(row, 'RVettore')?.querySelector('img')?.alt || '';
                const category = cell(row, 'RCategoria')?.querySelector('img')?.alt || '';
                const destination = clean(cell(row, 'RStazione')?.textContent);
                const delay = clean(cell(row, 'RRitardo')?.textContent);
                const trainId = row.querySelector('[id^="btn_"]')?.id.slice(4) || [trainNumber, carrier, destination].join('|');
                return [{row, platform, data: {
                    trainId, trainNumber, clock, destination, carrier, category,
                    delayMinutes: delay === '' ? 0 : /^\d+$/.test(delay) && Number(delay) < 720 ? Number(delay) : -1,
                    cancelled: /cancellat|soppress|cancelled|canceled/i.test(delay),
                    platform: official(platform),
                }}];
            });
        }

        let catalog = null;
        let catalogAt = 0;
        let generation = 0;
        let timer;
        const observer = new MutationObserver(schedule);

        function editBoard(action) {
            observer.disconnect();
            try { action(); } finally {
                observer.observe(document.body, {childList: true, subtree: true, characterData: true});
            }
        }

        function clearEstimates() {
            document.querySelectorAll('.' + MARKER).forEach(element => element.remove());
        }

        async function refresh(version) {
            editBoard(clearEstimates);
            if (!catalog || Date.now() - catalogAt > 300000) {
                const result = await request('/rfi/stations');
                if (version !== generation) return;
                if (!result?.stations) return;
                catalog = result.stations;
                catalogAt = Date.now();
            }
            if (!catalog.includes(station)) return;
            const entries = snapshot().filter(entry => !known(entry.data.platform) && !entry.data.cancelled);
            if (!entries.length) return;
            const data = entries.map(entry => entry.data);
            const fingerprint = JSON.stringify(data);
            const result = await request('/predict?operator=rfi', {ts: new Date().toISOString(), station, data});
            if (version !== generation || !result?.predictions) return;
            const current = snapshot().filter(entry => !known(entry.data.platform) && !entry.data.cancelled);
            if (JSON.stringify(current.map(entry => entry.data)) !== fingerprint) return;
            const predictions = new Map(result.predictions.map(pred => [pred.trainId + '|' + pred.clock, pred]));
            editBoard(() => {
                let displayed = false;
                for (const entry of current) {
                    if (known(official(entry.platform))) continue;
                    const prediction = predictions.get(entry.data.trainId + '|' + entry.data.clock);
                    if (!prediction?.probabilities) continue;
                    const labels = formatPlatforms(prediction.probabilities);
                    if (!labels) continue;
                    const estimate = document.createElement('span');
                    estimate.className = MARKER;
                    estimate.textContent = 'Stima: ' + labels;
                    estimate.title = 'Stima sperimentale. Probabilità del modello non calibrate. Controlla i monitor e gli annunci RFI.';
                    estimate.style.cssText = 'display:block;font-size:0.85em;color:#785500;font-style:italic';
                    entry.platform.appendChild(estimate);
                    displayed = true;
                }
                if (displayed && !document.getElementById('on-voie-rfi-banner')) {
                    const banner = document.createElement('div');
                    banner.id = 'on-voie-rfi-banner';
                    banner.textContent = 'Stime sperimentali On Voie Tous, non informazioni RFI. Controlla i monitor e gli annunci della stazione.';
                    banner.style.cssText = 'padding:8px;background:#fff0cc;color:#333;text-align:center';
                    document.body.prepend(banner);
                }
            });
        }

        function schedule() {
            generation += 1;
            const version = generation;
            clearTimeout(timer);
            timer = setTimeout(() => refresh(version), 300);
        }

        function start() {
            observer.observe(document.body, {childList: true, subtree: true, characterData: true});
            schedule();
            setInterval(schedule, 30000);
        }
        if (document.body) start();
        else document.addEventListener('DOMContentLoaded', start, {once: true});
    }
})();
