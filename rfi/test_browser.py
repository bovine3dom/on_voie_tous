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
    <td id="RRitardo">{delay}</td><td id="RBinario">{platform}</td>
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
        page.route("**/rfi/stations", lambda route: route.fulfill(
            json={"stations": ["1728"], "leadMinutes": [15, 130]}))
        page.goto(URL)
        yield page
        browser.close()


def predictions():
    return {"predictions": [
        {"trainId": "33", "clock": "12:30", "probabilities": [{"platform": "2EST", "prob": 0.9}]},
        {"trainId": "11", "clock": "12:30", "probabilities": [{"platform": "20B", "prob": 0.9}]},
    ]}


def test_estimates_use_identity_and_never_replace_official_platforms(page):
    calls = []
    def respond(route):
        calls.append(route.request.post_data_json)
        route.fulfill(json=predictions())
    page.route("**/predict?operator=rfi", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(2)
    assert page.locator('[data-test-id="11"] .on-voie-rfi-estimate').inner_text().startswith("Stima: 20B")
    assert page.locator('[data-test-id="33"] .on-voie-rfi-estimate').inner_text().startswith("Stima: 2EST")
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B"
    assert {train["trainId"] for train in calls[0]["data"]} == {"11", "33"}
    page.wait_for_timeout(800)
    assert len(calls) == 1  # Own DOM changes must not cause request loops.


@pytest.mark.parametrize("probabilities,expected", [
    ([("3", 0.20), ("1", 0.25), ("2", 0.25), ("4", 0.05)], "Stima: 1-3 (70%)"),
    ([("8", 0.40), ("2", 0.20), ("1", 0.20)], "Stima: 8 (40%), 1-2 (40%)"),
    ([("C", 0.20), ("A", 0.25), ("B", 0.25)], "Stima: A-C (70%)"),
    ([("1", 0.06), ("2", 0.06)], "Stima: 1-2 (12%)"),
    ([("2EST", 0.25), ("3", 0.25)], "Stima: 2EST (25%), 3 (25%)"),
    ([("20B", 0.25), ("21", 0.25)], "Stima: 20B (25%), 21 (25%)"),
    ([("1", 0.4), ("2", 0.3), ("3", 0.2)], "Stima: 1 (40%), 2 (30%)"),
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


def test_an_official_update_discards_an_inflight_prediction(page):
    def respond(route):
        page.locator('[data-test-id="11"] [id="RBinario"]').evaluate("el => el.textContent = '9'")
        page.locator('[data-test-id="33"] [id="RBinario"]').evaluate("el => el.textContent = '10'")
        route.fulfill(json=predictions())
    page.route("**/predict?operator=rfi", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator('[data-test-id="11"] [id="RBinario"]')).to_have_text("9")
    page.wait_for_timeout(800)
    assert page.locator(".on-voie-rfi-estimate").count() == 0


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
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(2)
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
    page.route("**/rfi/stations", lambda route: rfi_calls.append(route.request.url) or route.abort())
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


def test_arrivals_are_not_modified(page):
    calls = []
    page.route("**/rfi/stations", lambda route: calls.append(route.request.url) or route.abort())
    page.goto(URL.replace("False", "True"))
    page.add_script_tag(path=str(SCRIPT))
    page.wait_for_timeout(700)
    assert calls == []
    assert page.locator(".on-voie-rfi-estimate").count() == 0
