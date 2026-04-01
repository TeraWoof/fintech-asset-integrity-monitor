"use client";
import { useState, useMemo } from "react";
import { VirtuosoGrid } from "react-virtuoso";
import { Card } from "@/src/components/card";
import { useCryptoSocket } from "@/src/hooks/useCryptoSocket";
import { useAssetStore } from "@/src/store/useAssetStore";

export default function Home() {
  useCryptoSocket();
  const assets = useAssetStore((state) => state.assets);
  const [search, setSearch] = useState("");

  const filteredAssets = useMemo(() => {
    return assets.filter((a) =>
      a.symbol.toLowerCase().includes(search.toLowerCase())
    );
  }, [assets, search]);

  const hasBreach = useMemo(() => 
    filteredAssets.some((a) => Math.abs(a.drift) > 5), 
  [filteredAssets]);

  if (assets.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-black text-emerald-500 font-mono italic tracking-tighter">
        <span className="animate-pulse">[SYS] ESTABLISHING BINANCE_STREAM_v2...</span>
        <span className="text-[10px] mt-2 text-slate-700 uppercase tracking-[0.5em]">Waiting for initial handshake</span>
      </div>
    );
  }

  return (
    <main className="min-h-screen bg-black p-4 md:p-8 font-mono text-slate-300">
      <div className="max-w-[1400px] mx-auto border border-slate-800/60 p-6 md:p-10 rounded-sm bg-[#02040a] shadow-2xl relative">
        
        {/* Header */}
        <header className="flex flex-col lg:flex-row justify-between items-start lg:items-center mb-12 border-b border-slate-800 pb-8 gap-6">
          <div>
            <h1 className="text-2xl font-black text-white tracking-tighter italic">
              SCRYPT <span className="text-slate-600">//</span> ASSET_INTEGRITY
            </h1>
            <p className="text-[10px] text-slate-500 mt-1 uppercase tracking-[0.4em] font-bold">
              Real-Time Financial Risk Surveillance
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-6 w-full lg:w-auto">
            <div className="relative flex-grow lg:flex-grow-0">
              <input
                type="text"
                placeholder="CMD: SEARCH_TOKEN"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="bg-[#05070a] border border-slate-800 px-4 py-2 text-xs focus:outline-none focus:border-emerald-500/50 text-emerald-500 w-full lg:w-72 transition-all placeholder:text-slate-800"
              />
            </div>
            <div className="flex items-center gap-3 bg-emerald-950/20 px-4 py-2 border border-emerald-900/30">
              <span className="w-1.5 h-1.5 bg-emerald-500 rounded-full animate-ping"></span>
              <span className="text-[10px] text-emerald-500 font-black uppercase tracking-widest">Live_Feed_Active</span>
            </div>
          </div>
        </header>

        <div className="min-h-[65vh]">
          <VirtuosoGrid
            data={filteredAssets}
            totalCount={filteredAssets.length}
            overscan={300}
            useWindowScroll
            listClassName="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-8"
            itemContent={(_, asset) => (
              <Card
                key={asset.symbol}
                asset={asset}
                isBreached={Math.abs(asset.drift) > 5}
              />
            )}
          />
        </div>

        {filteredAssets.length === 0 && (
          <div className="text-center py-32 border border-slate-900/50 bg-slate-950/50">
            <p className="text-slate-600 text-xs uppercase tracking-[0.5em] italic">
              No data match for query: "{search}"
            </p>
          </div>
        )}

        {hasBreach && (
          <div className="fixed bottom-8 left-1/2 -translate-x-1/2 z-50 w-[90%] max-w-4xl p-4 bg-red-600/10 border border-red-500 backdrop-blur-xl shadow-[0_0_30px_rgba(239,68,68,0.2)]">
            <div className="flex items-center justify-center gap-4 text-red-500 text-[11px] font-black uppercase tracking-[0.4em]">
              <span className="animate-bounce">⚠️</span>
              CRITICAL: ASSET VOLATILITY BREACHED DEFINED MANDATE
              <span className="animate-bounce">⚠️</span>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}