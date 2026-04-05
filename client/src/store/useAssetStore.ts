import { create } from 'zustand';

interface Asset {
  symbol: string;
  price: number;
  drift: number;
  history: number[]; // Buffer 
}

interface AssetStore {
  assets: Asset[];
  updateAssets: (newData: { symbol: string; price: number; drift: number }[]) => void;
}

export const useAssetStore = create<AssetStore>((set) => ({
  assets: [],

  updateAssets: (newData) => set((state) => {
    // Create a map for quick lookup of existing assets by symbol
    const currentAssetsMap = new Map(state.assets.map(a => [a.symbol, a]));

    const updatedAssets = newData.map((item) => {
      const existing = currentAssetsMap.get(item.symbol);
      
      // If the asset exists, update its price and drift, and append the new price to the history
      const newHistory = existing 
        ? [...existing.history, item.price].slice(-30)
        : [item.price];

      return {
        ...item,
        history: newHistory
      };
    });

    return { assets: updatedAssets };
  }),
}));