import os
from pathlib import Path
import shutil

import pytest

pw = pytest.importorskip("playwright.sync_api")
SCRIPT = Path(__file__).resolve().parents[1] / "src/rfi.user.js"
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
            json={"stations": ["1728"], "minConfidence": 0.8, "leadMinutes": [15, 130]}))
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
    page.route("**/rfi/predict", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator(".on-voie-rfi-estimate")).to_have_count(2)
    assert page.locator('[data-test-id="11"] .on-voie-rfi-estimate').inner_text().startswith("Stima: 20B")
    assert page.locator('[data-test-id="33"] .on-voie-rfi-estimate').inner_text().startswith("Stima: 2EST")
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B"
    assert {train["trainId"] for train in calls[0]["data"]} == {"11", "33"}
    page.wait_for_timeout(800)
    assert len(calls) == 1  # Own DOM changes must not cause request loops.


def test_an_official_update_discards_an_inflight_prediction(page):
    def respond(route):
        page.locator('[data-test-id="11"] [id="RBinario"]').evaluate("el => el.textContent = '9'")
        page.locator('[data-test-id="33"] [id="RBinario"]').evaluate("el => el.textContent = '10'")
        route.fulfill(json=predictions())
    page.route("**/rfi/predict", respond)
    page.add_script_tag(path=str(SCRIPT))
    pw.expect(page.locator('[data-test-id="11"] [id="RBinario"]')).to_have_text("9")
    page.wait_for_timeout(800)
    assert page.locator(".on-voie-rfi-estimate").count() == 0


def test_a_failed_server_leaves_the_board_unchanged(page):
    page.route("**/rfi/predict", lambda route: route.fulfill(status=503))
    page.add_script_tag(path=str(SCRIPT))
    page.wait_for_timeout(1000)
    assert page.locator(".on-voie-rfi-estimate").count() == 0
    assert page.locator('[data-test-id="22"] [id="RBinario"]').inner_text() == "20B"


def test_arrivals_are_not_modified(page):
    calls = []
    page.route("**/rfi/stations", lambda route: calls.append(route.request.url) or route.abort())
    page.goto(URL.replace("False", "True"))
    page.add_script_tag(path=str(SCRIPT))
    page.wait_for_timeout(700)
    assert calls == []
    assert page.locator(".on-voie-rfi-estimate").count() == 0
