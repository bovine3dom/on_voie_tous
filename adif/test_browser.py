import json

import pytest

from rfi.test_browser import page, pw, SCRIPT

HOST = "https://pantallas-estaciones.vercel.app"


def train(id="11", **changes):
    result = dict(id=id, technical_number_planif_out="00123", technical_number_planif="999",
                departure_time="2026-10-06T12:30:00+02:00", class_stop="origin",
                destinations=[{"code": "900"}, {"code": "100"}, {"code": "900"}],
                commercial_id=[{"product": "AVE"}], company="RENFE", platform="",
                delay_out="-15")
    return {**result, **changes}


def send(page, trains=None, station="17000"):
    board = {"station_settings": {"code": station}, "trains": trains or [train()]}
    page.evaluate("board => document.querySelector('iframe').contentWindow.postMessage({target: 'grvta.setData', objData: JSON.stringify(board)}, location.origin)", board)


@pytest.fixture
def board(page):
    page.route(HOST + "/**", lambda route: route.fulfill(content_type="text/html", body=(
        '<iframe src="/x-1.9/index.html"></iframe>' if route.request.url.endswith("/~/?station=17000") else
        '<div class="adif-infotren-vista-departures"><div class="train-row" train-id="11"><div class="train-platform">9</div></div>'
        '<div class="train-row" train-id="22"><div class="train-platform"></div></div></div>'
        '<div class="adif-infotren-vista-arrivals"><div class="train-row" train-id="11"><div class="train-platform">4</div></div></div>')))
    page.route("**/stations?operator=adif", lambda route: route.fulfill(json={"stations": ["17000"]}))
    page.add_init_script("window.nativeFetch = window.fetch; window.nativeSocket = window.WebSocket;\n" + SCRIPT.read_text())
    page.goto(HOST + "/~/?station=17000")
    frame = page.frames[1]
    frame.add_script_tag(path=str(SCRIPT))
    return page, frame


def respond(route, calls):
    payload = route.request.post_data_json
    calls.append(payload)
    route.fulfill(json={"predictions": [
        {"trainId": t["trainId"], "probabilities": [{"platform": "20B", "prob": .9}]}
        for t in reversed(payload["data"])]})


def test_message_features_identity_redraw_and_no_feedback(board):
    page, frame = board
    calls = []
    page.route("**/predict?operator=adif", lambda route: respond(route, calls))
    send(page, [train(platform="9"), train("22")])
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(2)
    data = calls[0]["data"][0]
    assert data["trainNumber"] == "00123"
    assert data["destination"] == "100|900"
    assert data["category"] == "AVE"
    assert data["delayMinutes"] == -15
    assert data["scheduledTime"] == "2026-10-06T12:30:00+02:00"
    assert data["platform"] == "9"
    assert json.loads(data["trainId"]) == ["17000", "11", "00123", data["scheduledTime"]]
    assert frame.locator(".adif-infotren-vista-arrivals .train-platform").inner_text() == "4"
    assert frame.evaluate("window.fetch === nativeFetch && window.WebSocket === nativeSocket")
    frame.locator('.train-row[train-id="22"] .train-platform').evaluate("el => el.innerHTML = ''")
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(2)
    send(page, [train(platform="9"), train("22")])
    page.wait_for_timeout(300)
    assert len(calls) == 1
    send(page, [train(platform="10"), train("22")])
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(2)
    page.wait_for_timeout(300)
    assert calls[-1]["data"][0]["platform"] == "10"


@pytest.mark.parametrize("change", [
    {"class_stop": "destination"}, {"status": "cancelled"}, {"observation": "Suprimido"},
    {"traffic_type": "B"}, {"platform": "BUS"}, {"departure_time": "invalid"},
])
def test_ineligible_update_clears_predictions(board, change):
    page, frame = board
    calls = []
    page.route("**/predict?operator=adif", lambda route: respond(route, calls))
    send(page)
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(1)
    send(page, [{**train(), **change}])
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(0)
    assert len(calls) == 1


def test_failed_request_retries_and_unknown_station_clears(board):
    page, frame = board
    page.route("**/predict?operator=adif", lambda route: route.fulfill(status=503))
    with page.expect_response("**/predict?operator=adif"):
        send(page)
    page.wait_for_timeout(100)
    assert frame.locator(".on-voie-adif-estimate").count() == 0
    calls = []
    page.route("**/predict?operator=adif", lambda route: respond(route, calls))
    send(page)
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(1)
    send(page, station="99999")
    pw.expect(frame.locator(".on-voie-adif-estimate")).to_have_count(0)
    assert len(calls) == 1


def test_stale_response_after_station_change_is_discarded(board):
    page, frame = board
    def stale(route):
        send(page, station="99999")
        page.wait_for_timeout(100)
        respond(route, [])
    page.route("**/predict?operator=adif", stale)
    with page.expect_response("**/predict?operator=adif"):
        send(page)
    page.wait_for_timeout(300)
    assert frame.locator(".on-voie-adif-estimate").count() == 0


def test_untrusted_message_is_ignored(board):
    page, frame = board
    calls = []
    page.route("**/predict?operator=adif", lambda route: respond(route, calls))
    frame.evaluate("board => window.dispatchEvent(new MessageEvent('message', {origin: 'https://example.org', data: {target: 'grvta.setData', objData: JSON.stringify(board)}}))", {
        "station_settings": {"code": "17000"}, "trains": [train()]})
    page.wait_for_timeout(300)
    assert calls == []
