// ==UserScript==
// @name         On Voie Tous — RFI
// @namespace    http://tampermonkey.net/
// @version      0.2
// @description  Show experimental platform estimates on RFI departure boards
// @match        https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
    'use strict';

    const SERVER = window.ON_VOIE_RFI_SERVER || 'https://compute.olie.science/on_voie_tous';
    const MARKER = 'on-voie-rfi-estimate';
    const url = new URL(window.location.href);
    const station = url.searchParams.get('placeId');
    if (!station || !/^\d+$/.test(station) || url.searchParams.get('arrivals')?.toLowerCase() === 'true') return;

    const clean = text => (text || '').replace(/\s+/g, ' ').trim();
    const known = text => !['', '-', '--', '—', '?', 'N.D.', 'ND', 'NON DISPONIBILE'].includes(clean(text).toUpperCase());
    const cell = (row, id) => row.querySelector(`[id="${id}"]`);
    const official = element => clean(Array.from(element.childNodes)
        .filter(node => !node.classList?.contains(MARKER)).map(node => node.textContent).join(' '));

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
            .filter(p => p.prob >= 0.1).slice(0, 2)
            .map(p => `${p.platform} (${Math.round(p.prob * 100)}%)`).join(', ');
    }

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

    async function request(path, payload) {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 2000);
        try {
            const response = await fetch(SERVER + path, {
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
})();
