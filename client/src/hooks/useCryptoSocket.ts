import { useEffect } from 'react';
import { useAssetStore } from '@/src/store/useAssetStore';

export const useCryptoSocket = () => {
  const setAssets = useAssetStore((state) => state.setAssets);

  useEffect(() => {
    const socket = new WebSocket('ws://localhost:8000/ws');

    socket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setAssets(data);
    };

    socket.onopen = () => console.log('✅ Connected to Multi-Asset Stream');
    socket.onerror = (error) => console.error('❌ WebSocket Error:', error);

    return () => socket.close();
  }, [setAssets]);
};