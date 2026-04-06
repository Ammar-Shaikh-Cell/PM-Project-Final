import api from "./index";

export interface MaterialProfilePayload {
    name: string;
    tempMinZ1: number | null;
    tempMaxZ1: number | null;
    tempMinZ2: number | null;
    tempMaxZ2: number | null;
    tempMinZ3: number | null;
    tempMaxZ3: number | null;
    tempMinZ4: number | null;
    tempMaxZ4: number | null;
    tempMinZ5: number | null;
    tempMaxZ5: number | null;
    pressureMin: number | null;
    pressureMax: number | null;
    speedMin: number | null;
    speedMax: number | null;
}

export interface MaterialProfile extends MaterialProfilePayload {
    id: string;
    createdAt: string | null;
    updatedAt: string | null;
}

interface ApiMaterialProfile {
    id: string;
    name: string;
    temp_min_z1: number | null;
    temp_max_z1: number | null;
    temp_min_z2: number | null;
    temp_max_z2: number | null;
    temp_min_z3: number | null;
    temp_max_z3: number | null;
    temp_min_z4: number | null;
    temp_max_z4: number | null;
    temp_min_z5: number | null;
    temp_max_z5: number | null;
    pressure_min: number | null;
    pressure_max: number | null;
    speed_min: number | null;
    speed_max: number | null;
    created_at: string | null;
    updated_at: string | null;
}

const toApiPayload = (payload: MaterialProfilePayload) => ({
    name: payload.name,
    temp_min_z1: payload.tempMinZ1,
    temp_max_z1: payload.tempMaxZ1,
    temp_min_z2: payload.tempMinZ2,
    temp_max_z2: payload.tempMaxZ2,
    temp_min_z3: payload.tempMinZ3,
    temp_max_z3: payload.tempMaxZ3,
    temp_min_z4: payload.tempMinZ4,
    temp_max_z4: payload.tempMaxZ4,
    temp_min_z5: payload.tempMinZ5,
    temp_max_z5: payload.tempMaxZ5,
    pressure_min: payload.pressureMin,
    pressure_max: payload.pressureMax,
    speed_min: payload.speedMin,
    speed_max: payload.speedMax,
});

const fromApiMaterial = (material: ApiMaterialProfile): MaterialProfile => ({
    id: material.id,
    name: material.name,
    tempMinZ1: material.temp_min_z1,
    tempMaxZ1: material.temp_max_z1,
    tempMinZ2: material.temp_min_z2,
    tempMaxZ2: material.temp_max_z2,
    tempMinZ3: material.temp_min_z3,
    tempMaxZ3: material.temp_max_z3,
    tempMinZ4: material.temp_min_z4,
    tempMaxZ4: material.temp_max_z4,
    tempMinZ5: material.temp_min_z5,
    tempMaxZ5: material.temp_max_z5,
    pressureMin: material.pressure_min,
    pressureMax: material.pressure_max,
    speedMin: material.speed_min,
    speedMax: material.speed_max,
    createdAt: material.created_at,
    updatedAt: material.updated_at,
});

export const materialsApi = {
    list: async (): Promise<MaterialProfile[]> => {
        const { data } = await api.get<ApiMaterialProfile[] | { items?: ApiMaterialProfile[]; data?: ApiMaterialProfile[] }>("/materials");
        const items = Array.isArray(data)
            ? data
            : Array.isArray(data?.items)
                ? data.items
                : Array.isArray(data?.data)
                    ? data.data
                    : [];

        return items.map(fromApiMaterial);
    },

    create: async (payload: MaterialProfilePayload): Promise<MaterialProfile> => {
        const { data } = await api.post<ApiMaterialProfile>("/materials", toApiPayload(payload));
        return fromApiMaterial(data);
    },

    update: async (materialId: string, payload: MaterialProfilePayload): Promise<MaterialProfile> => {
        const { data } = await api.put<ApiMaterialProfile>(`/materials/${materialId}`, toApiPayload(payload));
        return fromApiMaterial(data);
    },
};
