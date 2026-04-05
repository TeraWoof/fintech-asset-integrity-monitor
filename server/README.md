SCRYPT // High-Performance Risk Engine
This is the core backend responsible for real-time data ingestion, quantitative risk processing, and high-frequency distribution. The architecture is engineered for minimal latency and institutional-grade data integrity.

##Technical Architecture
To overcome the limitations of the Python Global Interpreter Lock (GIL) in high-frequency trading environments, we implemented a Hybrid Concurrency Model:

I/O Layer (AsyncIO): An asynchronous event loop manages the WebSocket connection with Binance, allowing the consumption of multi-asset streams without blocking the main execution thread.

Compute Layer (Multiprocessing): Heavy quantitative metrics (Drift and Volatility) are offloaded to an isolated OS process. This enables true parallelism, ensuring that intensive calculations never delay the reception of incoming price ticks.

Vectorized Engine (NumPy): All risk metrics are calculated using NumPy arrays, providing C-level performance for time-series operations.

##Prerequisites
Python 3.11+

Dependencies: fastapi, uvicorn, websockets, numpy, pydantic.

##Getting Started
Install Dependencies:

Bash
pip install fastapi uvicorn websockets numpy pydantic
Configuration:
The engine is pre-configured to monitor high-liquidity pairs: BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, and ADAUSDT.

Run the Engine:

Bash
python main.py
The server will start at http://0.0.0.0:8000. The WebSocket feed is available at the /ws endpoint.

##Data Endpoints
WebSocket: /ws
Broadcasts a JSON array of PriceUpdate objects every second to the frontend:

JSON
[
  {
    "symbol": "BTCUSDT",
    "price": 68541.20,
    "drift": -0.31
  },
  ...
]

##Reliability & Safety
Pydantic Validation: All data flowing through the engine is strictly validated against schemas to prevent type errors or null values in production.

Connection Resilience: Built-in heartbeat (ping/pong) and auto-reconnect logic ensure the feed remains active during network instability.