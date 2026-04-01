SCRYPT // Asset Integrity Monitor
A high-frequency financial surveillance dashboard built to monitor digital asset volatility and market integrity in real-time. This project demonstrates a robust full-stack architecture capable of handling intensive data throughput with low-latency visualization.

##Key Features
High-Frequency Data Ingestion: Consumes live multi-stream WebSockets from Binance using Python's asyncio.

GIL-Bypassing Risk Engine: Utilizes a dedicated multiprocessing worker to perform heavy quantitative calculations (Drift/Volatility) without blocking the I/O event loop.

Atomic State Management: Powered by Zustand, ensuring that only the specific asset card receiving data re-renders, maintaining 60 FPS performance.

Data Virtualization: Implements react-virtuoso to efficiently render large lists of assets by only processing elements within the viewport.

Real-time Visual Analytics: Integrated Recharts sparklines for immediate trend analysis and automated "Breach Alerts" for volatility management.

##Tech Stack
Backend (Engine)
Language: Python 3.11+

Framework: FastAPI

Concurrency: AsyncIO (Networking) + Multiprocessing (Computation)

Math: NumPy (Vectorized calculations)

Communication: Secure WebSockets

Frontend (Dashboard)
Framework: Next.js (App Router)

Styling: Tailwind CSS (Cyberpunk/Institutional Dark Theme)

State: Zustand (Atomic slices)

Charts: Recharts / D3.js

List Rendering: React Virtuoso

##Architecture Overview
The system follows a Producer-Consumer pattern to ensure maximum stability under load:

The Producer: An asynchronous task connects to exchange feeds and pushes raw ticks into a synchronized multiprocessing.Queue.

The Consumer (Risk Engine): A separate OS process pulls from the queue, uses NumPy to calculate the price drift over a rolling window, and updates a shared memory state.

The API/WS Layer: FastAPI broadcasts the processed risk metrics to the React frontend every second.

##Integrity & Safety
Pydantic Validation: All incoming and outgoing data is strictly typed to ensure portfolio integrity.

Error Resilience: Implementation of optional chaining and defensive coding to handle WebSocket handshakes and null states gracefully.