import { create } from 'zustand';

interface AssetData {
  symbol: string;
  price: number;
  drift: number;
}

interface AssetStore {
  assets: AssetData[];
  setAssets: (data: AssetData[]) => void;
}

export const useAssetStore = create<AssetStore>((set) => ({
  assets: [],
  setAssets: (data) => set({ assets: data }),
}));