from pydantic import BaseModel
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

@app.health("/health")
async def health():
    return {"status": "ok"}