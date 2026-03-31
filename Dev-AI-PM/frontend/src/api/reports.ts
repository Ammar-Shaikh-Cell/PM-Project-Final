import { safeApi } from "./safeApi";
import api from "./index";
import { ReportRequest, ReportResponse } from "../types/api";

export const reportsApi = {
    generate: async (payload: ReportRequest): Promise<ReportResponse> => {
        const result = await safeApi.post<ReportResponse>("/reports/generate", payload);
        if (result.fallback || !result.data) {
            throw new Error(result.error || "Report generation failed - backend unavailable");
        }
        return result.data;
    },

    download: async (filename: string): Promise<Blob> => {
        const response = await api.get(`/reports/download/${filename}`, {
            responseType: "blob",
            timeout: 30000,
        });
        return response.data as Blob;
    },

    downloadById: async (reportId: string): Promise<Blob> => {
        const response = await api.get(`/reports/${reportId}/download`, {
            responseType: "blob",
            timeout: 30000,
        });
        return response.data as Blob;
    },
};

