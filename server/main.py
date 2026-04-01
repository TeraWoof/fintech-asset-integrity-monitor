import asyncio
import json
import websockets
import pandas as pd
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from collections import deque

# 1. Buffer and State Management
price_history = deque(maxlen=100)

class PriceUpdate(BaseModel):
    symbol: str
    price: float
    drift: float = 0.0

# 2. Integration with Binance WebSocket API
async def fetch_binance_data(symbol: str):
    uri = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@ticker"
    while True:
        try:
            async with websockets.connect(uri) as binance_ws:
                print(f"📡 Connected to Binance Stream: {symbol}")
                while True:
                    raw_data = await binance_ws.recv()
                    data = json.loads(raw_data)
                    
                    if 'c' in data:
                        current_price = float(data['c'])
                        price_history.append(current_price)
                        
                        # Cálculo de Drift com Pandas (opcional, ou direto do deque)
                        if len(price_history) > 1:
                            initial_price = price_history[0]
                            drift = (current_price - initial_price) / initial_price
                            
                            # Logging de monitorização em tempo real
                            if abs(drift) > 0.05:
                                print(f"🚨 ALERT | MANDATE BREACH | {symbol}: {current_price} (Drift: {drift:.2%})")
                            else:
                                # Apenas um log discreto para sabermos que está vivo
                                print(f"📈 {symbol}: {current_price} | Drift: {drift:.2%}", end="\r")

                    # Pequeno delay para não saturar o buffer com dados idênticos 
                    # e garantir que os 100 itens cobrem um período de tempo útil.
                    await asyncio.sleep(0.5) 
                                
        except Exception as e:
            print(f"❌ Connection error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# 3. Novo Método: Lifespan Event Handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Lógica de Startup: Inicia a tarefa em background
    task = asyncio.create_task(fetch_binance_data("BTCUSDT"))
    print("🚀 Background task started")
    
    yield  # Aqui é onde a aplicação corre
    
    # Lógica de Shutdown: Limpeza (se necessário)
    task.cancel()
    print("🛑 Background task stopped")

# 4. Inicialização do App com Lifespan
app = FastAPI(lifespan=lifespan)

# 5. WebSocket para o Frontend
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("💻 Frontend Client Connected")
    try:
        while True:
            if price_history:
                current_price = price_history[-1]
                # Cálculo do drift para o payload
                initial_price = price_history[0]
                drift = (current_price - initial_price) / initial_price
                
                payload = PriceUpdate(
                    symbol="BTCUSDT",
                    price=current_price,
                    drift=round(drift * 100, 2)
                )
                await websocket.send_json(payload.model_dump())
            
            await asyncio.sleep(1) 
    except WebSocketDisconnect:
        print("🔌 Frontend Client Disconnected")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)