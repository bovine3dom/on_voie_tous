// ==UserScript==
// @name         On Voie Tous
// @namespace    http://tampermonkey.net/
// @version      0.14
// @description  Predicts platforms on SNCF, RFI and ADIF departure boards. Adds deeplinks to SNCB
// @author       bovine3dom
// @match        https://www.garesetconnexions.sncf/*
// @match        https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor*
// @match        https://pantallas-estaciones.vercel.app/*
// @match        https://www.adif.es/w/*
// @match        https://www.b-europe.com/EN/Real-time*
// @match        https://dpartu.re/*
// @run-at       document-start
// @updateURL    https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js.meta.js
// @downloadURL  https://raw.githubusercontent.com/bovine3dom/on_voie_tous/master/src/content.user.js
// @supportURL   https://github.com/bovine3dom/on_voie_tous/issues/
// @grant        GM_xmlhttpRequest
// @grant        GM.xmlHttpRequest
// @connect      compute.olie.science
// ==/UserScript==
