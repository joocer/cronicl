from fastapi import FastAPI
from .._queue import queue_sizes

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/queues")
async def queues():
    return queue_sizes()