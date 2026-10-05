import os
from pathlib import Path
import socket
import subprocess
import sys
import time

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("legacy", [True, False])
def test_both_entrypoints_register_shared_routes(tmp_path, legacy):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    environment = os.environ | {"PREDICT_HOST": "127.0.0.1", "PREDICT_PORT": str(port)}
    for operator, station in (("sncf", "0087756056"), ("rfi", "1728"), ("adif", "51003")):
        folder = tmp_path / operator
        folder.mkdir()
        (folder / f"{station}.cbm").touch()
        environment[f"{operator.upper()}_MODELS_DIR"] = str(folder)
    command = [sys.executable, "predict.py"] if legacy else [sys.executable, "-m", "predict.server"]
    with (tmp_path / "server.log").open("w+") as log:
        process = subprocess.Popen(command, cwd=ROOT / "predict" if legacy else ROOT,
                                   env=environment, stdout=log, stderr=subprocess.STDOUT)
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=1) as client:
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.seek(0)
                        pytest.fail(log.read())
                    try:
                        if client.get("/health").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    time.sleep(0.1)
                else:
                    pytest.fail("Server did not start")
                assert client.get("/stations").json() == {"stations": ["0087756056"]}
                assert client.get("/stations?operator=rfi").json()["stations"] == ["1728"]
                assert client.get("/rfi/stations").json()["stations"] == ["1728"]
                assert client.get("/stations?operator=adif").json()["stations"] == ["51003"]
                for operator, station in (("rfi", "1728"), ("adif", "51003")):
                    response = client.post(f"/predict?operator={operator}", json={
                        "ts": "2026-04-01T10:00:00Z", "station": station, "data": [],
                    })
                    assert response.status_code == 200
                    assert response.json() == {"predictions": []}
        finally:
            process.terminate()
            process.wait(timeout=20)
