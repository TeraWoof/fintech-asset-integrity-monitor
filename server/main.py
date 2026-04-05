import os
import asyncio
import json
import websockets
import logging
import numpy as np
from multiprocessing import Process, Queue, Manager
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from pydantic import BaseModel
from collections import deque

# --- CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PriceUpdate(BaseModel):
    symbol: str
    price: float
    drift: float = 0.0

# --- QUANTITATIVE ENGINE ---
def risk_engine_worker(input_queue: Queue, shared_dict: dict):
    local_buffers = {}
    while True:
        item = input_queue.get()
        if item is None: break
        
        symbol, price = item
        if symbol not in local_buffers:
            local_buffers[symbol] = deque(maxlen=100)
        
        local_buffers[symbol].append(price)
        data_array = np.array(local_buffers[symbol])
        
        if len(data_array) > 1:
            initial_price = data_array[0]
            current_price = data_array[-1]
            drift = ((current_price - initial_price) / initial_price) * 100
            
            shared_dict[symbol] = {
                "price": current_price,
                "drift": round(float(drift), 2)
            }

# --- I/O LAYER ---
async def fetch_binance_data(symbols: list, risk_queue: Queue):
    # Convert list to Binance stream format
    streams = "/".join([f"{s.lower()}@ticker" for s in symbols])
    uri = f"wss://stream.binance.com:9443/ws/{streams}"

    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as binance_ws:
                logger.info(f"📡 Feed Active for Dynamic Subscription: {symbols}")
                while True:
                    raw_message = await binance_ws.recv()
                    data = json.loads(raw_message)
                    if isinstance(data, dict) and 's' in data and 'c' in data:
                        risk_queue.put((data['s'], float(data['c'])))
        except Exception as e:
            logger.error(f"🔄 Reconnecting due to: {e}")
            await asyncio.sleep(5)

# --- FASTAPI LIFESPAN ---
shared_assets_data = None
risk_input_queue = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global shared_assets_data, risk_input_queue
    manager = Manager()
    shared_assets_data = manager.dict()
    risk_input_queue = Queue()

    quant_process = Process(target=risk_engine_worker, args=(risk_input_queue, shared_assets_data))
    quant_process.start()
    
    # We don't start the fetcher here anymore, as it's now dynamic per connection
    yield 
    
    risk_input_queue.put(None)
    quant_process.join()

app = FastAPI(lifespan=lifespan)

# --- DYNAMIC WEBSOCKET ENDPOINT ---
@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket, 
    symbols: str = Query("BTCUSDT,ETHUSDT") # Default symbols if none provided
):
    """
    Dynamic WebSocket: Subscription is based on the 'symbols' query parameter.
    Example: ws://localhost:8000/ws?symbols=SOLUSDT,BNBUSDT
    """
    await websocket.accept()
    
    # Parse symbols from query string
    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    
    # Start a dedicated fetcher task for THIS specific client connection
    # In a massive scale app, you'd use a Pub/Sub (Redis) to avoid duplicated Binance connections
    io_task = asyncio.create_task(fetch_binance_data(symbol_list, risk_input_queue))
    
    try:
        while True:
            # Filter the shared memory to only send data requested by this client
            current_snapshot = list(shared_assets_data.items())
            
            payloads = [
                PriceUpdate(symbol=symbol, price=data["price"], drift=data["drift"]).model_dump()
                for symbol, data in current_snapshot if symbol in symbol_list
            ]
            
            if payloads:
                await websocket.send_json(payloads)
            
            await asyncio.sleep(0.5) 
            
    except WebSocketDisconnect:
        logger.info(f"❌ Client unsubscribed from: {symbol_list}")
    finally:
        io_task.cancel() # Critical: stop fetching Binance data for this client when they leave

if __name__ == "__main__":
    import uvicorn
    # O Railway injeta automaticamente a variável PORT
    port = int(os.environ.get("PORT", 8000)) 
    logger.info(f"🚀 Servidor a iniciar na porta {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)