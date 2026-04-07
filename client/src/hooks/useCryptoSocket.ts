import { useEffect } from 'react';
import { useAssetStore } from '@/src/store/useAssetStore';

export const useCryptoSocket = () => {
  // Change to useAssetStore to get the updateAssets function directly, ensuring we have the latest reference
  const updateAssets = useAssetStore((state) => state.updateAssets);

  useEffect(() => {
    // Defining the WebSocket connection to the backend feed
    const socket = new WebSocket('wss://fintech-poc.onrender.com/ws'); // usualy we add the links in our .env file, but for simplicity, we are hardcoding it here

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        // we send the entire array of assets to the store, and the store will take care of merging it with the existing state and maintaining the history buffer.
        // the updateAssets function in the store will handle the logic of updating the state and maintaining the history buffer for each asset.
        updateAssets(data);
      } catch (error) {
        console.error('⚠️ Failed to parse WebSocket payload:', error);
      }
    };

    socket.onopen = () => {
      console.log('📡 [SCRYPT] Asset Integrity Feed Established');
    };

    socket.onerror = (err) => {
      console.error('❌ [SCRYPT] WebSocket Connection Error:', err);
    };

    socket.onclose = () => {
      console.warn('🔌 [SCRYPT] Feed Disconnected. Retrying in background...');
    };

    // Cleanup: close the WebSocket connection when the component unmounts or when updateAssets changes (though it shouldn't change due to Zustand's stable function references)
    return () => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }
    };
  }, [updateAssets]);
};