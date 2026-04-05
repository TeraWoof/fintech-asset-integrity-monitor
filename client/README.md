SCRYPT // Asset Integrity Dashboard
This is the high-performance frontend for the SCRYPT Risk Monitor. It is designed to visualize thousands of real-time data points from the backend without compromising UI responsiveness or browser stability.

##Architectural Decisions
To handle the constant stream of data from the Risk Engine, we implemented several advanced React patterns:

Atomic State Management (Zustand): Instead of a heavy global state that triggers full-page re-renders, we use Zustand. This allows each Card component to subscribe only to its specific asset updates, drastically reducing the Virtual DOM overhead.

Data Virtualization (React Virtuoso): To maintain a silky-smooth 60 FPS, we use VirtuosoGrid. The browser only renders the cards currently visible in the viewport, allowing the application to scale to hundreds of assets without memory leaks.

Memoized Analytics: Calculations for filtering and risk breach detection are memoized using useMemo, ensuring that expensive logic only runs when necessary.

High-Frequency Visuals (Recharts): Every asset card features a live sparkline that provides instant trend context. We disabled heavy animations to ensure the charts don't bottleneck the main thread.

##Tech Stack
Framework: Next.js 14+ (App Router)

State: Zustand

Charts: Recharts

Grid Rendering: React Virtuoso

Styling: Tailwind CSS

Icons/UI: Lucide React + Shadcn/UI

##Getting Started
Install Dependencies:

Bash
npm install
# or
yarn install
Environment Setup:
Ensure your backend is running at ws://localhost:8000/ws. You can configure the WebSocket URL in the useCryptoSocket hook.

Run the Dashboard:

Bash
npm run dev
Open http://localhost:5000 to view the live monitor.

##Defensive UI Features
Graceful Degradation: The UI handles WebSocket disconnections with automatic status indicators.

Optional Chaining: All data renders are protected with optional chaining and fallback values to prevent "undefined" crashes during the initial handshake.

Breach Alert System: A sticky, high-visibility notification system triggers immediately when the backend detects a volatility breach.