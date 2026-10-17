import os
import shutil

import pytest

from rfi.test_browser import page, pw, SCRIPT
from .test_pipeline import row

URL = "https://www.adif.es/w/79400-barcelona-e.fran%C3%A7a"
HTML = '''<html><body>
<input name="trafficType" value="cercanias" type="radio" checked>
<table id="horas-trenes-estacion-salidas"><tbody></tbody></table>
<script>
window.poll = (number = 0, search = 'proximasSalidas') => new Promise(resolve => {
    const url = new URL(location.href);
    url.searchParams.set('p_p_resource_id', '/consultarHorario');
    for (const [key, value] of Object.entries({stationCode: '79400', trafficType: 'cercanias', numPage: number, searchType: search}))
        url.searchParams.set('_servicios_estacion_ServiciosEstacionPortlet_' + key, value);
    const xhr = new XMLHttpRequest();
    xhr.open('GET', url);
    xhr.onload = () => {
        const data = JSON.parse(xhr.responseText);
        const tbody = document.querySelector('tbody');
        if (!number) tbody.innerHTML = '';
        for (const r of data.horarios || []) {
            const tr = document.createElement('tr');
            tr.className = 'horario-row';
            tr.innerHTML = `<td class="col-hora"><div><span>${r.hora}</span><span>${r.horaEstado || ''}</span></div></td>
                <td class="col-destino"><div><span>${r.estacion}</span></div></td>
                <td class="col-tren"><div><span>${r.trenDatosOp || ''}</span><span><span>${r.tren}</span></span></div></td>
                <td class="col-via"><div><span>${r.via}</span><i class="fas fa-times"></i></div></td>`;
            tbody.appendChild(tr);
        }
        resolve(data);
    };
    xhr.send();
});
</script></body></html>'''


@pytest.fixture
def board_page(page):
    raw = {"date": "16/10/2026", "time": "14:20", "horarios": [row("3 min", "8"), row("4 min", "", "R3")]}
    calls = []
    def site(route):
        if 'p_p_resource_id=' in route.request.url:
            route.fulfill(json=raw)
        else:
            route.fulfill(content_type="text/html", body=HTML)
    def predict(route):
        calls.append(route.request.post_data_json)
        route.fulfill(json={"predictions": [{"rowIndex": i, "trainNumber": r["tren"],
            "departureTime": "2099-01-01T12:00:00Z", "probabilities": [{"platform": str(i + 1), "prob": .9}]}
            for i, r in reversed(list(enumerate(raw["horarios"])))]})
    page.route("https://www.adif.es/**", site)
    page.route("**/adif-web/stations", lambda route: route.fulfill(json={"stations": ["79400"]}))
    page.route("**/adif-web/predict", predict)
    page.add_init_script("""window.GM_xmlhttpRequest = options => {
        fetch(options.url, {method: options.method, headers: options.headers, body: options.data})
            .then(async response => options.onload({status: response.status, responseText: await response.text()}))
            .catch(options.onerror);
    };""")
    page.add_init_script(path=str(SCRIPT))
    page.goto(URL)
    return page, raw, calls


def test_identity_pagination_and_native_response(board_page):
    page, raw, calls = board_page
    page.add_script_tag(path=str(SCRIPT))
    assert page.evaluate("poll()") == raw
    pw.expect(page.locator('.on-voie-adif-estimate')).to_have_count(2)
    assert page.locator('.col-via > div > span:first-child').all_text_contents() == ['8 | 1 (90%)', '2 (90%)']
    assert calls[0]['data'] == raw
    assert set(calls[0]) == {'station', 'traffic', 'ts', 'data'}
    page.evaluate("poll(1)")
    pw.expect(page.locator('.on-voie-adif-estimate')).to_have_count(4)
    page.evaluate("document.body.appendChild(document.createElement('div'))")
    page.wait_for_timeout(100)
    assert len(calls) == 2
    assert calls[1]['data'] == raw
    page.evaluate("document.querySelector('.col-via span').textContent = '9'")
    pw.expect(page.locator('.on-voie-adif-estimate')).to_have_count(3)


@pytest.mark.parametrize('failure', ['server', 'catalog', 'arrival', 'board'])
def test_failures_leave_official_platforms(board_page, failure):
    page, raw, calls = board_page
    if failure == 'server':
        page.route('**/adif-web/predict', lambda route: route.fulfill(status=503))
    elif failure == 'catalog':
        page.route('**/adif-web/stations', lambda route: route.fulfill(json={'stations': []}))
    elif failure == 'board':
        raw['error'] = True
    page.evaluate("poll(0, 'proximasLlegadas')" if failure == 'arrival' else "poll()")
    page.wait_for_timeout(300)
    assert page.locator('.on-voie-adif-estimate').count() == 0
    assert page.locator('.col-via').all_text_contents() == ['8', '']
    assert calls == []


def test_filter_change_discards_inflight_predictions(board_page):
    page, raw, calls = board_page
    def changed(route):
        page.evaluate("document.querySelector('input').dispatchEvent(new Event('change', {bubbles: true}))")
        route.fulfill(json={'predictions': [{'rowIndex': 0, 'trainNumber': 'R2S',
            'departureTime': '2099-01-01T12:00:00Z', 'probabilities': [{'platform': 'STALE', 'prob': 1}]}]})
    page.route('**/adif-web/predict', changed)
    page.evaluate('poll()')
    page.wait_for_timeout(300)
    assert page.locator('.on-voie-adif-estimate').count() == 0


def test_extension_bridge_with_strict_csp(tmp_path):
    executable = os.getenv('CHROMIUM_PATH', shutil.which('chromium'))
    if not executable:
        pytest.skip('Set CHROMIUM_PATH for the headless browser tests')
    extension = SCRIPT.parent
    with pw.sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(str(tmp_path),
            executable_path=executable, headless=True,
            args=[f'--disable-extensions-except={extension}', f'--load-extension={extension}'])
        try:
            raw = {'date': '16/10/2026', 'time': '14:20', 'horarios': [row('3 min', '8')]}
            def site(route):
                if 'p_p_resource_id=' in route.request.url:
                    route.fulfill(json=raw)
                else:
                    route.fulfill(content_type='text/html', body=HTML,
                        headers={'Content-Security-Policy': "connect-src 'self'"})
            context.route('https://www.adif.es/**', site)
            context.route('**/adif-web/stations', lambda route: route.fulfill(json={'stations': ['79400']}))
            calls = []
            def predict(route):
                calls.append(route.request.post_data_json)
                route.fulfill(json={'predictions': [{'rowIndex': 0, 'trainNumber': 'R2S',
                    'departureTime': '2099-01-01T12:00:00Z',
                    'probabilities': [{'platform': '4', 'prob': .9}]}]})
            context.route('**/adif-web/predict', predict)
            page = context.new_page()
            page.goto(URL)
            assert page.evaluate('poll()') == raw
            pw.expect(page.locator('.on-voie-adif-estimate')).to_have_text(' | 4 (90%)')
            assert calls[0]['data'] == raw
            assert page.locator('.col-via > div > span:first-child').inner_text() == '8 | 4 (90%)'
            # The page cannot use the bridge to request arbitrary URLs.
            page.evaluate("window.postMessage({type: 'on-voie-request', id: 'bad', path: '/private'}, location.origin)")
            page.wait_for_timeout(100)
            assert len(calls) == 1
        finally:
            context.close()
