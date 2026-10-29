// ==UserScript==
// @name         On Voie Tous
// @namespace    http://tampermonkey.net/
// @version      0.12
// @description  Predicts platforms on SNCF, RFI and ADIF departure boards
// @author       bovine3dom
// @match        https://www.garesetconnexions.sncf/*
// @match        https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor*
// @match        https://pantallas-estaciones.vercel.app/*
// @match        https://www.adif.es/w/*
// @run-at       document-start
// @updateURL    https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @downloadURL  https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @supportURL   https://github.com/bovine3dom/on_voie_tous/issues/
// @grant        GM_xmlhttpRequest
// @connect      compute.olie.science
// @sandbox      raw
// @inject-into  page
// ==/UserScript==

(function() {
    'use strict';

    const PREDICT_SERVER_URL = window.ON_VOIE_TOUS_SERVER || window.ON_VOIE_RFI_SERVER || 'https://compute.olie.science/on_voie_tous';
    const PREDICT_TIMEOUT_MS = 2000;
    const MIN_PROBABILITY = 0.1;
    const MAX_PLATFORMS = 2;

    if (window.location.hostname === 'www.adif.es') {
        startAdifWeb();
        return;
    }
    if (window.location.hostname === 'pantallas-estaciones.vercel.app') {
        startAdif();
        return;
    }
    if (window.location.hostname === 'iechub.rfi.it') {
        startRfi();
        return;
    }
    if (window.location.hostname !== 'www.garesetconnexions.sncf') return;
    if (window.__onVoieSncfActive) return;
    window.__onVoieSncfActive = true;

    function showBanner() {
        if (!document.body) {
            document.addEventListener('DOMContentLoaded', showBanner, {once: true});
            return;
        }
        injectStyles();
        if (document.getElementById('on-voie-tous-banner')) return;

        const banner = document.createElement('div');
        banner.id = 'on-voie-tous-banner';
        const link = document.createElement('a');
        link.href = 'https://github.com/bovine3dom/on_voie_tous';
        link.textContent = 'On Voie Tous';
        banner.append('Platform predictions provided by ', link,
            ', an experimental extension unaffiliated with SNCF, RFI or ADIF.');
        document.body.appendChild(banner);
    }

    function injectStyles() {
        if (document.getElementById('on-voie-tous-styles')) return;

        const style = document.createElement('style');
        style.id = 'on-voie-tous-styles';
        style.textContent = `
            #on-voie-tous-banner {
                all: initial !important;
                display: block !important;
                position: fixed !important;
                inset: 0 0 auto !important;
                background: #f0ad4e !important;
                color: #333 !important;
                padding: 8px 16px !important;
                text-align: center !important;
                font: 14px/1.5 -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
                z-index: 999999 !important;
                box-shadow: 0 2px 4px rgba(0,0,0,0.2) !important;
            }
            #on-voie-tous-banner a {
                all: revert !important;
                font: inherit !important;
                color: inherit !important;
                text-decoration: underline !important;
                cursor: pointer !important;
            }
            tr[name="treno"] [id="RBinario"],
            tr[name="treno"] [id="RBinario"] div,
            .on-voie-rfi-estimate {
                white-space: normal !important;
                overflow-wrap: anywhere !important;
            }
            tr[name="treno"] [id="RBinario"] div,
            .on-voie-rfi-estimate {
                display: inline !important;
            }
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

    function privilegedRequest(path, payload) {
        return new Promise(resolve => {
            if (typeof GM_xmlhttpRequest === 'function') {
                GM_xmlhttpRequest({
                    url: 'https://compute.olie.science/on_voie_tous' + path,
                    method: payload ? 'POST' : 'GET',
                    headers: {'Content-Type': 'application/json'},
                    data: payload ? JSON.stringify(payload) : undefined,
                    anonymous: true,
                    timeout: PREDICT_TIMEOUT_MS,
                    onload: response => {
                        try { resolve(response.status === 200 ? JSON.parse(response.responseText) : null); }
                        catch { resolve(null); }
                    },
                    onerror: () => resolve(null),
                    ontimeout: () => resolve(null),
                });
                return;
            }
            const id = crypto.randomUUID();
            const finish = data => {
                clearTimeout(timeout);
                window.removeEventListener('message', receive);
                resolve(data);
            };
            const receive = event => {
                if (event.source === window && event.origin === location.origin &&
                    event.data?.type === 'on-voie-response' && event.data.id === id) finish(event.data.result);
            };
            const timeout = setTimeout(() => finish(null), PREDICT_TIMEOUT_MS + 500);
            window.addEventListener('message', receive);
            window.postMessage({type: 'on-voie-request', id, path, payload}, location.origin);
        });
    }

    async function request(path, payload) {
        if (location.hostname === 'www.adif.es') return privilegedRequest(path, payload);
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

    function appendAdifEstimate(platform, labels) {
        const estimate = document.createElement('span');
        estimate.className = 'on-voie-adif-estimate';
        estimate.textContent = (platform.textContent.trim() ? ' | ' : '') + labels;
        estimate.title = 'Estimación experimental. Las puntuaciones no son probabilidades calibradas. Consulte los paneles y anuncios de ADIF.';
        platform.appendChild(estimate);
    }

    function startAdifWeb() {
        const station = location.pathname.match(/^\/w\/(\d{5})-/)?.[1];
        if (!station || window.__onVoieAdifWebActive) return;
        window.__onVoieAdifWebActive = true;
        const PREFIX = '_servicios_estacion_ServiciosEstacionPortlet_';
        const MARKER = 'on-voie-adif-estimate';
        const clean = value => String(value ?? '').replace(/\s+/g, ' ').trim();
        const decoded = value => {
            // Keep literal tags as text while decoding character references.
            const text = new DOMParser().parseFromString(String(value ?? '').replace(/</g, '&lt;'), 'text/html');
            return clean(text.body.textContent);
        };
        const pages = new Map();
        const requests = new WeakMap();
        let generation = 0;
        let traffic = null;
        let catalogRequest = null;
        const observer = new MutationObserver(render);

        function render() {
            if (!document.body) return;
            observer.disconnect();
            try {
                document.querySelectorAll('.' + MARKER).forEach(node => node.remove());
                const selected = document.querySelector('input[name="trafficType"]:checked')?.value;
                if (selected && selected !== traffic) return;
                const rows = document.querySelectorAll('#horas-trenes-estacion-salidas tr.horario-row');
                let offset = 0;
                let displayed = false;
                for (let number = 0; pages.has(number); number++) {
                    const page = pages.get(number);
                    if (Date.now() - Date.parse(page.ts) <= 300000) {
                        for (const prediction of page.predictions || []) {
                            const index = prediction.rowIndex;
                            if (!Number.isInteger(index) || index < 0 || index >= page.data.horarios.length ||
                                !(Date.parse(prediction.departureTime) >= Date.now())) continue;
                            const entry = page.data.horarios[index];
                            const row = rows[offset + index];
                            if (!row || prediction.trainNumber !== entry.tren) continue;
                            const fields = [
                                ['hora', '.col-hora > div > span:first-child'],
                                ['horaEstado', '.col-hora > div > span:nth-child(2)'],
                                ['tren', '.col-tren > div > span:nth-child(2) > span'],
                                ['trenDatosOp', '.col-tren > div > span:first-child'],
                                ['estacion', '.col-destino > div > :first-child'],
                                ['via', '.col-via > div > span:first-child'],
                            ];
                            if (!fields.every(([field, selector]) => clean(row.querySelector(selector)?.textContent) === decoded(entry[field]))) continue;
                            const labels = formatPlatforms(prediction.probabilities || []);
                            const platform = row.querySelector('.col-via > div > span:first-child');
                            if (!labels || !platform) continue;
                            appendAdifEstimate(platform, labels);
                            displayed = true;
                        }
                    }
                    offset += page.data.horarios.length;
                }
                if (displayed) showBanner();
            } finally {
                observer.observe(document.body, {childList: true, subtree: true, characterData: true});
            }
        }

        function reset() {
            generation++;
            pages.clear();
            render();
        }

        async function received(xhr, context) {
            if (context.generation !== generation || xhr.status !== 200) return;
            let data;
            try { data = xhr.responseType === 'json' ? xhr.response : JSON.parse(xhr.responseText); }
            catch { return; }
            if (!Array.isArray(data?.horarios) || data.error) return;
            const page = {data, ts: new Date().toISOString(), predictions: []};
            pages.set(context.page, page);
            render();
            if (!data.horarios.length) return;
            catalogRequest ||= request('/adif-web/stations');
            const catalog = await catalogRequest;
            if (!Array.isArray(catalog?.stations)) catalogRequest = null;
            if (context.generation !== generation || pages.get(context.page) !== page || !catalog?.stations?.includes(station)) return;
            const result = await request('/adif-web/predict', {station, traffic: context.traffic, ts: page.ts, data});
            if (context.generation !== generation || pages.get(context.page) !== page) return;
            page.predictions = Array.isArray(result?.predictions) ? result.predictions : [];
            render();
        }

        const open = XMLHttpRequest.prototype.open;
        const send = XMLHttpRequest.prototype.send;
        XMLHttpRequest.prototype.open = function(method, url, ...rest) {
            const result = open.call(this, method, url, ...rest);
            requests.delete(this);
            try {
                const target = new URL(url, location.href);
                const params = target.searchParams;
                const filter = params.get(PREFIX + 'trafficType');
                const page = params.get(PREFIX + 'numPage');
                if (String(method).toUpperCase() === 'GET' && target.origin === location.origin &&
                    target.pathname === location.pathname && params.get('p_p_resource_id') === '/consultarHorario' &&
                    params.get(PREFIX + 'searchType') === 'proximasSalidas' &&
                    params.get(PREFIX + 'stationCode') === station && ['cercanias', 'avldmd'].includes(filter) &&
                    /^\d+$/.test(page) && Number(page) < 100) {
                    requests.set(this, {page: Number(page), traffic: filter});
                }
            } catch { /* Leave unrelated requests unchanged. */ }
            return result;
        };
        XMLHttpRequest.prototype.send = function(...args) {
            const context = requests.get(this);
            if (context) {
                if (context.page === 0 || context.traffic !== traffic) reset();
                traffic = context.traffic;
                pages.delete(context.page);
                render();
                const current = {...context, generation};
                this.addEventListener('loadend', () => { received(this, current).catch(() => {}); }, {once: true});
            }
            return send.apply(this, args);
        };
        document.addEventListener('change', event => {
            if (event.target.name === 'trafficType') reset();
        });
        if (document.body) render();
        else document.addEventListener('DOMContentLoaded', render, {once: true});
        setInterval(render, 30000);
    }

    function startAdif() {
        if (window.__onVoieAdifActive) return;
        window.__onVoieAdifActive = true;
        const MARKER = 'on-voie-adif-estimate';
        const clean = value => String(value ?? '').trim();
        const joined = (items, key) => [...new Set((items || []).map(item => clean(item[key])))].sort().join('|');
        let fingerprint = null;
        let generation = 0;
        let predictions = new Map();
        let catalog = null;
        let catalogRequest = null;
        const observer = new MutationObserver(render);

        function render() {
            if (!document.body) return;
            observer.disconnect();
            try {
                document.querySelectorAll('.' + MARKER).forEach(element => element.remove());
                let displayed = false;
                const rows = document.querySelectorAll('.adif-infotren-vista-departures .train-row[train-id]');
                for (const row of rows) {
                    const labels = predictions.get(row.getAttribute('train-id'));
                    const platform = row.querySelector('.train-platform');
                    if (!labels || !platform) continue;
                    appendAdifEstimate(platform, labels);
                    displayed = true;
                }
                if (displayed) showBanner();
            } finally {
                observer.observe(document.body, {childList: true, subtree: true, characterData: true});
            }
        }

        window.addEventListener('message', async event => {
            if (event.origin !== window.location.origin || event.source !== window.parent ||
                event.data?.target !== 'grvta.setData') return;
            let board;
            try { board = JSON.parse(event.data.objData); } catch { return; }
            const station = clean(board?.station_settings?.code);
            if (!/^\d{1,12}$/.test(station) || !Array.isArray(board.trains)) return;
            const entries = board.trains.flatMap(train => {
                const trainNumber = clean(train.technical_number_planif_out) || clean(train.technical_number_planif);
                const scheduledTime = clean(train.departure_time);
                const id = clean(train.id);
                if (!id || !trainNumber || !['origin', 'intermediate'].includes(train.class_stop) ||
                    !/(?:Z|[+-]\d{2}:\d{2})$/.test(scheduledTime) || !Number.isFinite(Date.parse(scheduledTime))) return [];
                const status = clean(train.status) + ' ' + clean(train.observation);
                const carrier = clean(train.company);
                const category = joined(train.commercial_id, 'product');
                const platform = clean(train.platform);
                if (/cancel|suprimid|anulad/i.test(status) || train.traffic_type === 'B' ||
                    /^BUS/i.test(trainNumber) || platform.toUpperCase() === 'BUS' ||
                    /\bbus|autobus|pullman|autoserv|autocors/i.test(carrier + ' ' + category)) return [];
                const delay = clean(train.delay_out ?? 0);
                return [{id, data: {
                    trainId: JSON.stringify([station, id, trainNumber, scheduledTime]),
                    trainNumber, scheduledTime, stopType: train.class_stop,
                    destination: joined(train.destinations, 'code'), carrier, category,
                    trafficType: clean(train.traffic_type), status, platform,
                    delayMinutes: /^[+-]?\d+$/.test(delay) ? Number(delay) : -1,
                }}];
            });
            const currentFingerprint = JSON.stringify([station, entries]);
            if (currentFingerprint === fingerprint) return;
            fingerprint = currentFingerprint;
            const version = ++generation;
            predictions = new Map();
            render();
            if (!entries.length) return;
            if (!catalog) {
                catalogRequest ||= request('/stations?operator=adif');
                const result = await catalogRequest;
                catalogRequest = null;
                if (Array.isArray(result?.stations)) catalog = result.stations;
            }
            if (version !== generation) return;
            if (!catalog) { fingerprint = null; return; }
            if (!catalog.includes(station)) return;
            const result = await request('/predict?operator=adif', {
                ts: new Date().toISOString(), station, data: entries.map(entry => entry.data),
            });
            if (version !== generation) return;
            if (!Array.isArray(result?.predictions)) { fingerprint = null; return; }
            const byIdentity = new Map(result.predictions.map(pred => [pred.trainId, pred]));
            predictions = new Map(entries.map(entry => [entry.id,
                formatPlatforms(byIdentity.get(entry.data.trainId)?.probabilities || [])]));
            render();
        });
        if (document.body) render();
        else document.addEventListener('DOMContentLoaded', render, {once: true});
    }

    function startRfi() {
        const MARKER = 'on-voie-rfi-estimate';
        if (window.__onVoieRfiActive) return;
        // RFI's own station picker submits PlaceId=…&Arrivals=…, so match names case-insensitively.
        const params = new URLSearchParams(Array.from(new URL(window.location.href).searchParams,
            ([key, value]) => [key.toLowerCase(), value]));
        const station = params.get('placeid');
        if (!station || !/^\d+$/.test(station) || params.get('arrivals')?.toLowerCase() === 'true') return;

        window.__onVoieRfiActive = true;
        const clean = text => (text || '').replace(/\s+/g, ' ').trim();
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
                    delayMinutes: delay === '' ? 0 : /^[+-]?\d+$/.test(delay) ? Number(delay) : -1,
                    cancelled: /cancellat|soppress|cancelled|canceled/i.test(delay),
                    platform: official(platform),
                }}];
            });
        }

        let catalog = null;
        let catalogRequest = null;
        let fingerprint = null;
        let predictions = new Map();
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

        const fingerprintOf = entries => JSON.stringify(entries.map(entry => entry.data)
            .sort((a, b) => (a.trainId + '|' + a.clock).localeCompare(b.trainId + '|' + b.clock)));

        function render(entries) {
            editBoard(() => {
                let displayed = false;
                for (const entry of entries) {
                    const prediction = predictions.get(entry.data.trainId + '|' + entry.data.clock);
                    if (!prediction?.probabilities) continue;
                    const labels = formatPlatforms(prediction.probabilities);
                    if (!labels) continue;
                    let estimate = entry.platform.querySelector('.' + MARKER);
                    if (!estimate) {
                        estimate = document.createElement('span');
                        estimate.className = MARKER;
                        estimate.title = 'Stima sperimentale. Probabilità del modello non calibrate. Controlla i monitor e gli annunci RFI.';
                        entry.platform.appendChild(estimate);
                    }
                    const text = (official(entry.platform) ? ' | ' : '') + labels;
                    if (estimate.textContent !== text) estimate.textContent = text;
                    displayed = true;
                }
                if (displayed) showBanner();
            });
        }

        async function refresh() {
            const entries = snapshot().filter(entry => !entry.data.cancelled);
            const currentFingerprint = fingerprintOf(entries);
            if (currentFingerprint === fingerprint) {
                render(entries);
                return;
            }
            fingerprint = currentFingerprint;
            predictions = new Map();
            const version = ++generation;
            editBoard(clearEstimates);
            if (!entries.length) return;
            if (!catalog) {
                catalogRequest ||= request('/stations?operator=rfi');
                const result = await catalogRequest;
                catalogRequest = null;
                if (Array.isArray(result?.stations)) catalog = result.stations;
            }
            if (version !== generation || !catalog?.includes(station)) return;
            if (fingerprintOf(snapshot().filter(entry => !entry.data.cancelled)) !== currentFingerprint) return;
            const result = await request('/predict?operator=rfi', {
                ts: new Date().toISOString(), station, data: entries.map(entry => entry.data),
            });
            if (version !== generation || !result?.predictions) return;
            const current = snapshot().filter(entry => !entry.data.cancelled);
            if (fingerprintOf(current) !== currentFingerprint) return;
            predictions = new Map(result.predictions.map(pred => [pred.trainId + '|' + pred.clock, pred]));
            render(current);
        }

        function schedule() {
            if (timer) return;
            timer = setTimeout(() => {
                timer = null;
                refresh();
            }, 300);
        }

        function start() {
            observer.observe(document.body, {childList: true, subtree: true, characterData: true});
            schedule();
        }
        if (document.body) start();
        else document.addEventListener('DOMContentLoaded', start, {once: true});
    }
})();
