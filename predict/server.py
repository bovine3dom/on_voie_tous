import uvicorn

from .predict import app, HOST, PORT
from rfi import server as _rfi
from adif import server as _adif


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, access_log=False)
