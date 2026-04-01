
import { LineChart, Line, ResponsiveContainer, YAxis } from 'recharts';

interface CardProps {
    asset: {
        symbol: string;
        price: number;
        drift: number;
    };
    isBreached: boolean;
}


export function Card({ asset, isBreached }: CardProps) {
  // Simulação de histórico para o Sparkline (idealmente viria do backend)
  const chartData = Array.from({ length: 10 }, (_, i) => ({ value: asset.price + (Math.random() - 0.5) * 10 }));

  return (
    <div className={`relative p-4 border transition-all duration-500 ${
      isBreached 
        ? "border-red-500 bg-red-950/10 shadow-[0_0_15px_rgba(239,68,68,0.2)]" 
        : "border-slate-800 bg-slate-900/50 hover:border-slate-600"
    }`}>
      <div className="flex justify-between items-start mb-4">
        <div>
          <p className="text-[10px] text-slate-500 uppercase tracking-widest">Asset Pair</p>
          <h3 className="text-lg font-bold text-white tracking-tighter">{asset.symbol}</h3>
        </div>
        <div className={`text-[10px] px-2 py-0.5 rounded-sm border ${
          isBreached ? "border-red-500 text-red-500" : "border-emerald-500/50 text-emerald-500"
        }`}>
          {isBreached ? "CRITICAL" : "STABLE"}
        </div>
      </div>
      <div className="grid grid-cols-2 gap-4 mb-4">
        <div>
          <p className="text-[9px] text-slate-500 uppercase">Spot Price (USD)</p>
          <p className="text-xl font-mono font-bold text-emerald-400">
            ${asset.price?.toLocaleString(undefined, { minimumFractionDigits: 2 })}
          </p>
        </div>
        <div className="text-right">
          <p className="text-[9px] text-slate-500 uppercase">Volatility Drift</p>
          <p className={`text-xl font-mono font-bold ${asset.drift >= 0 ? "text-emerald-500" : "text-red-500"}`}>
            {asset.drift > 0 ? "+" : ""}{asset.drift}%
          </p>
        </div>
      </div>
      <div className="h-12 w-full opacity-50">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <YAxis hide domain={['auto', 'auto']} />
            <Line 
              type="monotone" 
              dataKey="value" 
              stroke={isBreached ? "#ef4444" : "#10b981"} 
              strokeWidth={1.5} 
              dot={false} 
              isAnimationActive={false} 
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div className="absolute inset-0 pointer-events-none bg-[linear-gradient(rgba(18,16,16,0)_50%,rgba(0,0,0,0.1)_50%),linear-gradient(90deg,rgba(255,0,0,0.02),rgba(0,255,0,0.02),rgba(0,0,255,0.02))] bg-[length:100%_2px,3px_100%]"></div>
    </div>
  );
}