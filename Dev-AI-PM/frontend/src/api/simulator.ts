import { safeApi } from "./safeApi";

export const simulatorApi = {
    triggerFailure: async (payload: {
        machine_id?: string;
        sensor_id?: string;
        anomaly_type?: string;
        duration?: number;
        value_multiplier?: number;
    }) => {
        const result = await safeApi.post("/simulator/trigger-failure", payload);
        if (result.fallback || !result.data) {
            throw new Error(result.error || "Backend unavailable - cannot trigger simulation");
        }
        return result.data;
    },

    generateTestData: async (count: number = 10) => {
        const result = await safeApi.post("/simulator/generate-test-data", null, {
            params: { count },
        });
        if (result.fallback || !result.data) {
            throw new Error(result.error || "Backend unavailable - cannot generate test data");
        }
        return result.data;
    },
};

