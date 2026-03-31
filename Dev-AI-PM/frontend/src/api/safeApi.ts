import { AxiosInstance, AxiosRequestConfig, AxiosResponse } from 'axios';
import { useBackendStore } from '../store/backendStore';
import api from './index';

// Reuse the main API client so we benefit from refresh-token flow on 401.
const axiosInstance: AxiosInstance = api;

// No mock/dummy data: when backend is offline or request fails, return null so UI shows real empty/error state.
export const safeApi = {
  get: async <T = any>(
    url: string,
    config?: AxiosRequestConfig
  ): Promise<{ fallback: boolean; data: T | null; error?: string }> => {
    const backendStatus = useBackendStore.getState().status;

    if (backendStatus === 'offline') {
      return { fallback: true, data: null, error: 'Backend offline' };
    }

    try {
      const response: AxiosResponse<T> = await axiosInstance.get(url, {
        ...config,
        timeout: config?.timeout || 5000,
      });
      return { fallback: false, data: response.data };
    } catch (error: any) {
      return { fallback: true, data: null, error: error?.message || 'Request failed' };
    }
  },

  post: async <T = any>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig
  ): Promise<{ fallback: boolean; data: T | null; error?: string }> => {
    const backendStatus = useBackendStore.getState().status;

    if (backendStatus === 'offline') {
      return { fallback: true, data: null, error: 'Backend offline' };
    }

    try {
      const response: AxiosResponse<T> = await axiosInstance.post(url, data, {
        ...config,
        timeout: config?.timeout || 5000,
      });
      return { fallback: false, data: response.data };
    } catch (error: any) {
      return { fallback: true, data: null, error: error?.message || 'Request failed' };
    }
  },

  put: async <T = any>(
    url: string,
    data?: any,
    config?: AxiosRequestConfig
  ): Promise<{ fallback: boolean; data: T | null; error?: string }> => {
    const backendStatus = useBackendStore.getState().status;

    if (backendStatus === 'offline') {
      return { fallback: true, data: null, error: 'Backend offline' };
    }

    try {
      const response: AxiosResponse<T> = await axiosInstance.put(url, data, config);
      return { fallback: false, data: response.data };
    } catch (error: any) {
      return { fallback: true, data: null, error: error?.message || 'Request failed' };
    }
  },

  delete: async <T = any>(
    url: string,
    config?: AxiosRequestConfig
  ): Promise<{ fallback: boolean; data: T | null; error?: string }> => {
    const backendStatus = useBackendStore.getState().status;

    if (backendStatus === 'offline') {
      return { fallback: true, data: null, error: 'Backend offline' };
    }

    try {
      const response: AxiosResponse<T> = await axiosInstance.delete(url, config);
      return { fallback: false, data: response.data };
    } catch (error: any) {
      return { fallback: true, data: null, error: error?.message || 'Request failed' };
    }
  },
};

export default safeApi;
