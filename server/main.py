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

# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Memória compartilhada local (para acesso rápido do WebSocket)
# Em produção, para milhões de ticks, usaríamos TimescaleDB.
assets_latest_data = {}

class PriceUpdate(BaseModel):
    symbol: str
    price: float
    drift: float = 0.0

# --- LÓGICA DE QUANT (MULTIPROCESSING & NUMPY) ---

def risk_engine_worker(input_queue: Queue, output_dict: dict):
    """
    Worker isolado num processo próprio para bypassar o GIL.
    Usa NumPy para cálculos vetorizados de alta performance.
    """
    local_buffers = {}
    
    while True:
        # Recebe ticks do processo principal
        item = input_queue.get()
        if item is None: break
        
        symbol, price = item
        if symbol not in local_buffers:
            local_buffers[symbol] = deque(maxlen=100)
        
        local_buffers[symbol].append(price)
        
        # Vetorização com NumPy: Cálculo de Drift
        # Convertemos o deque para array NumPy para performance O(1) em cálculos complexos
        data_array = np.array(local_buffers[symbol])
        if len(data_array) > 1:
            initial = data_array[0]
            current = data_array[-1]
            drift = ((current - initial) / initial) * 100
            
            # Atualiza o estado que será lido pelo FastAPI
            output_dict[symbol] = {
                "price": current,
                "drift": round(float(drift), 2)
            }

# --- LÓGICA DE I/O (ASYNCIO) ---

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
                        # Offloading: Enviamos o cálculo pesado para o Risk Engine
                        # libertando o Event Loop para continuar a ler o socket
                        risk_queue.put((data['s'], float(data['c'])))
                                
        except Exception as e:
            logger.error(f"🔄 Socket Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# --- FASTAPI LIFESPAN ---

# Dicionário Gerenciado para comunicação entre processos
from multiprocessing import Manager
manager = Manager()
shared_assets_data = manager.dict()
risk_input_queue = Queue()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Inicia o Risk Engine num Processo separado (Paralelismo Real)
    quant_process = Process(
        target=risk_engine_worker, 
        args=(risk_input_queue, shared_assets_data)
    )
    quant_process.start()
    
    # 2. Inicia o consumidor de I/O no Event Loop principal
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
            # Lemos do estado processado pelo Risk Engine
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