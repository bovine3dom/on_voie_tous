import uvicorn

from .predict import HOST, PORT
from rfi.server import app


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, access_log=False)
