import os
from pathlib import Path
import shutil

import pytest

pw = pytest.importorskip("playwright.sync_api")
SCRIPT = Path(__file__).resolve().parents[1] / "src/content.user.js"
URL = "https://iechub.rfi.it/ArriviPartenze/ArrivalsDepartures/Monitor?placeId=1728&arrivals=False"


def row(id, platform="", delay=""):
    return f'''<tr name="treno" data-test-id="{id}">
    <td id="RVettore"><img alt="TRENITALIA"></td><td id="RCategoria"><img alt="Categoria REG"></td>
    <td id="RTreno">{id}</td><td id="RStazione">ROMA</td><td id="ROrario">12:30</td>
    <td id="RRitardo">{delay}</td><td id="RBinario"><div>{platform}</div></td>
    <td><button id="btn_{id}"></button></td></tr>'''


@pytest.fixture
def page():
    executable = os.getenv("CHROMIUM_PATH", shutil.which("chromium"))
    if not executable:
        pytest.skip("Set CHROMIUM_PATH for the headless browser tests")
    with pw.sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path=executable, headless=True)
        page = browser.new_page()
        page.route("**/Monitor?**", lambda route: route.fulfill(
            content_type="text/html", body="<html><body><table>"+row("11")+row("22", "20B")+row("33")+"</table></body></html>"))
        page.route("**/stations?operator=rfi", lambda route: route.fulfill(
            json={"stations": ["1728"]}))
        page.goto(URL)
        yield page
        browser.close()


def predictions():
    return {"predictions": [
        {"trainId": "33", "clock": "12:30", "probabilities": [{"platform": "2EST", "prob": 0.9}]},
        {"trainId": "11", "clock": "12:30", "probabilities": [{"platform": "20B", "prob": 0.9}]},
        {"trainId": "22", "clock": "12:30", "probabilities": [{"platform": "2EST", "prob": 0.9}]},
    ]}


def test_official_and_predicted_platforms_use_identity_without_feedback(page):
    calls = []
    def respond(route):
        calls.append(route.request.post_data_json)
        route.fulfill(json=predictions())
    page.route("**/predict?operator=rfi", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(3)
    assert page.locator('[data-test-id="11"] [id="RBinario"]').inner_text() == "20B (90%)"
    assert page.locator('[data-test-id="33"] [id="RBinario"]').inner_text() == "2EST (90%)"
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B | 2EST (90%)"
    assert {train["trainId"]: train["platform"] for train in calls[0]["data"]} == {"11": "", "22": "20B", "33": ""}
    page.locator('[data-test-id="11"] [id="RRitardo"]').evaluate("el => el.textContent = '1'")
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(3)
    page.wait_for_timeout(800)
    assert len(calls) == 2
    assert [train["platform"] for train in calls[1]["data"]] == ["", "20B", ""]


@pytest.mark.parametrize("probabilities,expected", [
    ([("3", 0.20), ("1", 0.25), ("2", 0.25), ("4", 0.05)], "1-3 (70%)"),
    ([("8", 0.40), ("2", 0.20), ("1", 0.20)], "8 (40%), 1-2 (40%)"),
    ([("C", 0.20), ("A", 0.25), ("B", 0.25)], "A-C (70%)"),
    ([("1", 0.06), ("2", 0.06)], "1-2 (12%)"),
    ([("2EST", 0.25), ("3", 0.25)], "2EST (25%), 3 (25%)"),
    ([("20B", 0.25), ("21", 0.25)], "20B (25%), 21 (25%)"),
    ([("1", 0.4), ("2", 0.3), ("3", 0.2)], "1 (40%), 2 (30%)"),
])
def test_client_merges_low_score_neighbours_before_display_filter(page, probabilities, expected):
    page.route("**/predict?operator=rfi", lambda route: route.fulfill(json={"predictions": [{
        "trainId": "11", "clock": "12:30",
        "probabilities": [{"platform": platform, "prob": prob} for platform, prob in probabilities],
    }]}))
    page.add_script_tag(path=str(SCRIPT))
    estimate = page.locator('[data-test-id="11"] .on-voie-rfi-estimate')
    pw.expect(estimate).to_have_text(expected)
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B"


def test_large_and_negative_delays_reach_the_server(page):
    calls = []
    page.locator("table").evaluate("(el, html) => el.innerHTML = html", row("11", delay="1440") + row("22", delay="-15"))
    def respond(route):
        calls.append(route.request.post_data_json)
        route.fulfill(json=predictions())
    page.route("**/predict?operator=rfi", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(2)
    assert [t["delayMinutes"] for t in calls[0]["data"]] == [1440, -15]


def test_an_official_update_discards_stale_scores_and_requests_new_ones(page):
    calls = []
    def respond(route):
        calls.append(route.request.post_data_json)
        if len(calls) == 1:
            page.locator('[data-test-id="11"] [id="RBinario"]').evaluate("el => el.textContent = '9'")
            route.fulfill(json={"predictions": [{
                "trainId": "11", "clock": "12:30", "probabilities": [{"platform": "STALE", "prob": 1}]}]})
        else:
            route.fulfill(json=predictions())
    page.route("**/predict?operator=rfi", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator('[data-test-id="11"] [id="RBinario"]')).to_have_text("9 | 20B (90%)")
    assert len(calls) == 2
    assert calls[1]["data"][0]["platform"] == "9"
    assert "STALE" not in page.locator("body").inner_text()


def test_clock_updates_and_identical_redraws_reuse_predictions_without_polling(page):
    calls, catalogs = [], []
    def respond(route):
        calls.append(route.request.post_data_json)
        route.fulfill(json=predictions())
    def catalog(route):
        catalogs.append(route.request.url)
        route.fulfill(json={"stations": ["1728"]})
    page.route("**/predict?operator=rfi", respond)
    page.route("**/stations?operator=rfi", catalog)
    page.evaluate("""() => {
        window.pollIntervals = [];
        const original = window.setInterval;
        window.setInterval = (...args) => {
            if (args[1] != null) window.pollIntervals.push(args[1]);
            return original(...args);
        };
    }""")
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(3)
    page.evaluate("document.body.appendChild(document.createElement('div')).textContent = 'Clock update'")
    page.wait_for_timeout(700)
    assert len(calls) == len(catalogs) == 1
    page.locator("table").evaluate("(table, html) => table.innerHTML = html", row("33") + row("22", "20B") + row("11"))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(3)
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B | 2EST (90%)"
    page.wait_for_timeout(700)
    assert len(calls) == len(catalogs) == 1
    assert page.evaluate("window.pollIntervals") == []


def test_a_failed_server_leaves_the_board_unchanged(page):
    page.route("**/predict?operator=rfi", lambda route: route.fulfill(status=503))
    page.add_script_tag(path=str(SCRIPT))
    page.wait_for_timeout(1000)
    assert page.locator(".on-voie-rfi-estimate").count() == 0
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B"


def test_document_start_and_duplicate_injection_keep_native_fetch(page):
    calls = []
    def respond(route):
        calls.append(route.request.post_data_json)
        route.fulfill(json=predictions())
    page.route("**/predict?operator=rfi", respond)
    page.add_init_script(
        "window.startedWithoutBody = !document.body; window.nativeFetch = window.fetch;\n"
        + SCRIPT.read_text())
    page.goto(URL)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(3)
    assert page.evaluate("window.startedWithoutBody && window.nativeFetch === window.fetch")
    page.wait_for_timeout(800)
    assert len(calls) == 1


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("status", [200, 503])
def test_sncf_uses_the_default_api_and_preserves_existing_display(page, wrapped, status):
    calls, rfi_calls = [], []
    trains = [
        {"direction": "Departure", "uic": "0087756056", "trainNumber": "11",
         "platform": {"track": "", "isTrackactive": False}},
        {"direction": "Departure", "uic": "0087756056", "trainNumber": "22",
         "platform": {"track": "C", "isTrackactive": True}},
        {"direction": "Arrival", "uic": "0087756056", "trainNumber": "33",
         "platform": {"track": "D", "isTrackactive": True}},
    ]
    payload = {"data": trains} if wrapped else trains
    page.route("https://www.garesetconnexions.sncf/**", lambda route: route.fulfill(
        content_type="text/html", body="<html><body></body></html>"))
    page.route("**/schedule-table/test", lambda route: route.fulfill(json=payload))
    page.route("**/stations?operator=rfi", lambda route: rfi_calls.append(route.request.url) or route.abort())
    def respond(route):
        calls.append(route.request.post_data_json)
        route.fulfill(status=status, json={"predictions": [
            {"probabilities": [{"platform": str(i), "prob": 0.25} for i in range(1, 5)]},
            {"probabilities": [{"platform": "C", "prob": 0.8}, {"platform": "D", "prob": 0.2}]},
        ]})
    page.route("**/predict", respond)  # No operator parameter: SNCF remains the default.
    page.add_init_script(path=str(SCRIPT))
    page.goto("https://www.garesetconnexions.sncf/fr/gares-services/nice/horaires")
    result = page.evaluate("async () => (await fetch('/schedule-table/test')).json()")
    displayed = result["data"] if wrapped else result
    expected = ["1-4 (100%)", "C | C (80%), D (20%)", "D"] if status == 200 else ["", "C", "D"]
    assert [train["platform"]["track"] for train in displayed] == expected
    assert len(calls) == 1
    assert calls[0]["station"] == "0087756056"
    assert [train["trainNumber"] for train in calls[0]["data"]] == ["11", "22"]
    assert rfi_calls == []
    assert page.locator("#on-voie-tous-banner").count() == (status == 200)


def test_official_platform_and_predictions_share_a_wrapping_line(page):
    page.route("**/predict?operator=rfi", lambda route: route.fulfill(json=predictions()))
    page.add_style_tag(content="""
        table { table-layout: fixed; width: 1000px; }
        [id="RBinario"] { width: 300px; white-space: nowrap; }
    """)
    page.add_script_tag(path=str(SCRIPT))
    platform = page.locator('[data-test-id="22"] [id="RBinario"]')
    pw.expect(platform).to_have_text("20B | 2EST (90%)")
    positions = platform.evaluate("""el => [el.querySelector('div'), el.querySelector('span')].map(node => {
        const range = document.createRange();
        range.selectNodeContents(node);
        return range.getClientRects()[0].y;
    })""")
    assert positions[0] == pytest.approx(positions[1])
    height = platform.evaluate("el => el.getBoundingClientRect().height")
    page.add_style_tag(content='[id="RBinario"] { width: 60px; }')
    assert platform.evaluate("el => el.getBoundingClientRect().height") > height
    assert platform.evaluate("el => el.scrollWidth <= el.clientWidth")


@pytest.mark.parametrize("operator", ["sncf", "rfi"])
def test_banner_has_shared_styles_despite_page_css(page, operator):
    if operator == "sncf":
        page.route("https://www.garesetconnexions.sncf/**", lambda route: route.fulfill(
            content_type="text/html", body="<html><body></body></html>"))
        page.route("**/schedule-table/test", lambda route: route.fulfill(json=[{
            "direction": "Departure", "uic": "0087756056",
            "platform": {"track": "", "isTrackactive": False},
        }]))
        page.route("**/predict", lambda route: route.fulfill(json={"predictions": [{
            "probabilities": [{"platform": "1", "prob": 0.9}],
        }]}))
        page.goto("https://www.garesetconnexions.sncf/fr/gares-services/nice/horaires")
    else:
        page.route("**/predict?operator=rfi", lambda route: route.fulfill(json=predictions()))
    page.add_style_tag(content="""
        div { display: none !important; background: blue !important; font-size: 40px !important; }
        a { color: white !important; font-size: 40px !important; text-decoration: none !important; }
        body { font: 24px serif; }
    """)
    page.add_script_tag(path=str(SCRIPT))
    if operator == "sncf":
        # A response before the body exists must still retain predictions and show the banner later.
        result = page.evaluate("""async () => {
            const body = document.body;
            body.remove();
            const data = await (await fetch('/schedule-table/test')).json();
            document.documentElement.appendChild(body);
            document.dispatchEvent(new Event('DOMContentLoaded'));
            return data;
        }""")
        assert result[0]["platform"]["track"] == "1 (90%)"
    banner = page.locator("#on-voie-tous-banner")
    pw.expect(banner).to_be_visible()
    assert banner.count() == 1
    assert banner.evaluate("el => { const s = getComputedStyle(el); return [s.position, s.top, s.backgroundColor, s.color, s.fontSize, s.lineHeight]; }") == [
        "fixed", "0px", "rgb(240, 173, 78)", "rgb(51, 51, 51)", "14px", "21px",
    ]
    link = banner.locator("a")
    assert link.evaluate("el => { const s = getComputedStyle(el); return [s.color, s.fontSize, s.textDecorationLine]; }") == [
        "rgb(51, 51, 51)", "14px", "underline",
    ]


def test_arrivals_are_not_modified(page):
    calls = []
    page.route("**/stations?operator=rfi", lambda route: calls.append(route.request.url) or route.abort())
    page.goto(URL.replace("False", "True"))
    page.add_script_tag(path=str(SCRIPT))
    page.wait_for_timeout(700)
    assert calls == []
    assert page.locator(".on-voie-rfi-estimate").count() == 0
