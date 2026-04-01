import asyncio
import json
import websockets
import logging
import numpy as np
from multiprocessing import Process, Queue
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ValidationError
from collections import deque

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Shared local memory for latest asset data (in-memory cache)
#In production for scalability, consider Redis or similar for shared state across instances, we could use TimeSeriesDB for historical data.
assets_latest_data = {}

class PriceUpdate(BaseModel):
    symbol: str
    price: float
    drift: float = 0.0

def risk_engine_worker(input_queue: Queue, output_dict: dict):
    """
    Worker isolado num processo próprio para bypassar o GIL.
    Usa NumPy para cálculos vetorizados de alta performance.
    """
    local_buffers = {}
    
    while True:
        # Recieves data from the I/O process, performs heavy calculations, and updates the shared state for FastAPI to serve.
        item = input_queue.get()
        if item is None: break
        
        symbol, price = item
        if symbol not in local_buffers:
            local_buffers[symbol] = deque(maxlen=100)
        
        local_buffers[symbol].append(price)
        
        # Numpy Vectorized Drift Calculation: Calcula o drift percentual entre o preço inicial e o atual, usando NumPy para performance O(1) mesmo com buffers grandes.
        data_array = np.array(local_buffers[symbol])
        if len(data_array) > 1:
            initial = data_array[0]
            current = data_array[-1]
            drift = ((current - initial) / initial) * 100
            
            # Update shared state for FastAPI to serve to clients
            output_dict[symbol] = {
                "price": current,
                "drift": round(float(drift), 2)
            }

async def fetch_binance_data(symbols: list, risk_queue: Queue):
    """
    Consumidor assíncrono otimizado para I/O sem bloqueio.
    """
    streams = "/".join([f"{s.lower()}@ticker" for s in symbols])
    uri = f"wss://stream.binance.com:9443/ws/{streams}"

    while True:
        try:
            async with websockets.connect(uri, ping_interval=20) as binance_ws:
                logger.info(f"📡 High-Performance Feed Established: {symbols}")
                while True:
                    raw_data = await binance_ws.recv()
                    data = json.loads(raw_data)
                    
                    if isinstance(data, dict) and 's' in data and 'c' in data:
                        # Offloading: we send only the necessary data to the Risk Engine, minimizing IPC overhead.
                        risk_queue.put((data['s'], float(data['c'])))
                                
        except Exception as e:
            logger.error(f"🔄 Socket Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# --- FASTAPI LIFESPAN ---

# Dictionary shared between processes to hold the latest price and drift data for each symbol, allowing FastAPI to serve this data efficiently without blocking on calculations.
from multiprocessing import Manager
manager = Manager()
shared_assets_data = manager.dict()
risk_input_queue = Queue()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Start the Risk Engine worker process that will handle all heavy computations in isolation, allowing FastAPI to remain responsive and serve data with minimal latency.
    quant_process = Process(
        target=risk_engine_worker, 
        args=(risk_input_queue, shared_assets_data)
    )
    quant_process.start()
    
    # 2. Initialise the Binance WebSocket feed in an asynchronous task, which will continuously fetch data and send it to the Risk Engine via the queue. 
    # This design ensures that the I/O operations do not block the main thread, allowing FastAPI to serve client requests concurrently.
    target_assets = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
    io_task = asyncio.create_task(fetch_binance_data(target_assets, risk_input_queue))
    
    yield
    
    io_task.cancel()
    risk_input_queue.put(None)
    quant_process.join()

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            payloads = []
            # Read the latest data from the shared dictionary updated by the Risk Engine process and send it to the client. 
            # This allows us to serve real-time updates with minimal latency, as FastAPI can read from the shared state without blocking on any heavy computations.
            for symbol, data in shared_assets_data.items():
                payloads.append(PriceUpdate(
                    symbol=symbol,
                    price=data["price"],
                    drift=data["drift"]
                ).model_dump())
            
            if payloads:
                await websocket.send_json(payloads)
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)