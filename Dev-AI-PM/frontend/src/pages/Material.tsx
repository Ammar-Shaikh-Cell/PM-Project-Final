import React, { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { materialsApi, MaterialProfile, MaterialProfilePayload } from "../api/materials";
import { useErrorToast } from "../components/ErrorToast";

type MaterialFormState = {
  name: string;
  tempMinZ1: string;
  tempMaxZ1: string;
  tempMinZ2: string;
  tempMaxZ2: string;
  tempMinZ3: string;
  tempMaxZ3: string;
  tempMinZ4: string;
  tempMaxZ4: string;
  tempMinZ5: string;
  tempMaxZ5: string;
  pressureMin: string;
  pressureMax: string;
  speedMin: string;
  speedMax: string;
};

type NumericFieldKey = keyof Omit<MaterialFormState, "name">;

const initialFormState: MaterialFormState = {
  name: "",
  tempMinZ1: "",
  tempMaxZ1: "",
  tempMinZ2: "",
  tempMaxZ2: "",
  tempMinZ3: "",
  tempMaxZ3: "",
  tempMinZ4: "",
  tempMaxZ4: "",
  tempMinZ5: "",
  tempMaxZ5: "",
  pressureMin: "",
  pressureMax: "",
  speedMin: "",
  speedMax: "",
};

const temperatureFields: Array<{ label: string; minKey: NumericFieldKey; maxKey: NumericFieldKey }> = [
  { label: "Temp Zone 1", minKey: "tempMinZ1", maxKey: "tempMaxZ1" },
  { label: "Temp Zone 2", minKey: "tempMinZ2", maxKey: "tempMaxZ2" },
  { label: "Temp Zone 3", minKey: "tempMinZ3", maxKey: "tempMaxZ3" },
  { label: "Temp Zone 4", minKey: "tempMinZ4", maxKey: "tempMaxZ4" },
  { label: "Temp Zone 5", minKey: "tempMinZ5", maxKey: "tempMaxZ5" },
];

const processFields: Array<{ label: string; minKey: NumericFieldKey; maxKey: NumericFieldKey }> = [
  { label: "Pressure (bar)", minKey: "pressureMin", maxKey: "pressureMax" },
  { label: "Screw Speed (rpm)", minKey: "speedMin", maxKey: "speedMax" },
];

const parseNumber = (value: string): number | null => {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
};

const toInputValue = (value: number | null): string => {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value);
};

const formatRange = (min: number | null, max: number | null): string => {
  if (min === null && max === null) {
    return "Not set";
  }

  return `${min ?? "-"} - ${max ?? "-"}`;
};

const formToPayload = (form: MaterialFormState): MaterialProfilePayload => ({
  name: form.name.trim(),
  tempMinZ1: parseNumber(form.tempMinZ1),
  tempMaxZ1: parseNumber(form.tempMaxZ1),
  tempMinZ2: parseNumber(form.tempMinZ2),
  tempMaxZ2: parseNumber(form.tempMaxZ2),
  tempMinZ3: parseNumber(form.tempMinZ3),
  tempMaxZ3: parseNumber(form.tempMaxZ3),
  tempMinZ4: parseNumber(form.tempMinZ4),
  tempMaxZ4: parseNumber(form.tempMaxZ4),
  tempMinZ5: parseNumber(form.tempMinZ5),
  tempMaxZ5: parseNumber(form.tempMaxZ5),
  pressureMin: parseNumber(form.pressureMin),
  pressureMax: parseNumber(form.pressureMax),
  speedMin: parseNumber(form.speedMin),
  speedMax: parseNumber(form.speedMax),
});

const materialToForm = (material: MaterialProfile): MaterialFormState => ({
  name: material.name,
  tempMinZ1: toInputValue(material.tempMinZ1),
  tempMaxZ1: toInputValue(material.tempMaxZ1),
  tempMinZ2: toInputValue(material.tempMinZ2),
  tempMaxZ2: toInputValue(material.tempMaxZ2),
  tempMinZ3: toInputValue(material.tempMinZ3),
  tempMaxZ3: toInputValue(material.tempMaxZ3),
  tempMinZ4: toInputValue(material.tempMinZ4),
  tempMaxZ4: toInputValue(material.tempMaxZ4),
  tempMinZ5: toInputValue(material.tempMinZ5),
  tempMaxZ5: toInputValue(material.tempMaxZ5),
  pressureMin: toInputValue(material.pressureMin),
  pressureMax: toInputValue(material.pressureMax),
  speedMin: toInputValue(material.speedMin),
  speedMax: toInputValue(material.speedMax),
});

const getErrorMessage = (error: unknown, fallback: string): string => {
  const apiError = error as {
    response?: { data?: { detail?: string } };
    message?: string;
  };

  return apiError.response?.data?.detail || apiError.message || fallback;
};

const MaterialProfiles = () => {
  const queryClient = useQueryClient();
  const { showError, ErrorComponent } = useErrorToast();

  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [form, setForm] = useState<MaterialFormState>(initialFormState);

  const { data: materials = [], isLoading, error } = useQuery({
    queryKey: ["materials"],
    queryFn: () => materialsApi.list(),
  });

  const resetForm = () => {
    setForm(initialFormState);
    setEditingId(null);
  };

  const closeForm = () => {
    resetForm();
    setShowForm(false);
  };

  const createMutation = useMutation({
    mutationFn: (payload: MaterialProfilePayload) => materialsApi.create(payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["materials"] });
      closeForm();
    },
    onError: (mutationError: unknown) => {
      showError(getErrorMessage(mutationError, "Failed to create material profile."));
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ materialId, payload }: { materialId: string; payload: MaterialProfilePayload }) =>
      materialsApi.update(materialId, payload),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["materials"] });
      closeForm();
    },
    onError: (mutationError: unknown) => {
      showError(getErrorMessage(mutationError, "Failed to update material profile."));
    },
  });

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const key = e.target.name as keyof MaterialFormState;
    setForm((current) => ({
      ...current,
      [key]: e.target.value,
    }));
  };

  const handleCreateClick = () => {
    resetForm();
    setShowForm(true);
  };

  const handleSave = () => {
    if (!form.name.trim()) {
      showError("Material name is required.");
      return;
    }

    const payload = formToPayload(form);

    if (editingId) {
      updateMutation.mutate({ materialId: editingId, payload });
      return;
    }

    createMutation.mutate(payload);
  };

  const handleEdit = (material: MaterialProfile) => {
    setForm(materialToForm(material));
    setEditingId(material.id);
    setShowForm(true);
  };

  const isSaving = createMutation.isPending || updateMutation.isPending;
  const loadErrorMessage = error ? getErrorMessage(error, "Failed to load material profiles.") : null;

  return (
    <div className="p-4 sm:p-6 text-slate-100">
      <div className="space-y-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-[#a551f4]">Material Profiles</h1>
            <p className="text-slate-400 mt-1">Manage your material profiles</p>
          </div>

          <button
            onClick={handleCreateClick}
            className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 rounded-lg"
          >
            + Create Material Profile
          </button>
        </div>

        {loadErrorMessage && (
          <div className="rounded-xl border border-rose-500/40 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
            {loadErrorMessage}
          </div>
        )}

        {showForm && (
          <div className="bg-slate-800 p-6 rounded-xl space-y-4">
            <h2 className="text-xl font-semibold">
              {editingId ? "Edit Material" : "Create Material"}
            </h2>

            <div>
              <label className="text-sm text-slate-200">Material Name</label>
              <input
                name="name"
                value={form.name}
                onChange={handleChange}
                className="w-full mt-1 p-2 rounded bg-slate-700"
              />
            </div>

            {temperatureFields.map((field) => (
              <div key={field.label}>
                <label className="text-sm text-slate-200">{field.label}</label>
                <div className="flex gap-2 mt-1">
                  <input
                    type="number"
                    step="any"
                    name={field.minKey}
                    placeholder="Min"
                    value={form[field.minKey]}
                    onChange={handleChange}
                    className="w-full p-2 rounded bg-slate-700"
                  />
                  <input
                    type="number"
                    step="any"
                    name={field.maxKey}
                    placeholder="Max"
                    value={form[field.maxKey]}
                    onChange={handleChange}
                    className="w-full p-2 rounded bg-slate-700"
                  />
                </div>
              </div>
            ))}

            {processFields.map((field) => (
              <div key={field.label}>
                <label className="text-sm text-slate-200">{field.label}</label>
                <div className="flex gap-2 mt-1">
                  <input
                    type="number"
                    step="any"
                    name={field.minKey}
                    placeholder="Min"
                    value={form[field.minKey]}
                    onChange={handleChange}
                    className="w-full p-2 rounded bg-slate-700"
                  />
                  <input
                    type="number"
                    step="any"
                    name={field.maxKey}
                    placeholder="Max"
                    value={form[field.maxKey]}
                    onChange={handleChange}
                    className="w-full p-2 rounded bg-slate-700"
                  />
                </div>
              </div>
            ))}

            <div className="flex justify-end gap-3 pt-2">
              <button
                onClick={closeForm}
                disabled={isSaving}
                className="px-4 py-2 bg-slate-600 rounded-lg disabled:opacity-60"
              >
                Cancel
              </button>

              <button
                onClick={handleSave}
                disabled={isSaving}
                className="px-4 py-2 bg-emerald-600 rounded-lg disabled:opacity-60"
              >
                {isSaving ? "Saving..." : "Save"}
              </button>
            </div>
          </div>
        )}

        <div className="space-y-4">
          {isLoading ? (
            <div className="text-center py-10 text-slate-400">Loading material profiles...</div>
          ) : materials.length === 0 ? (
            <div className="text-center py-10 text-slate-400">No materials found.</div>
          ) : (
            <div className="overflow-x-auto rounded-xl bg-slate-800">
              <table className="min-w-full text-sm text-left text-slate-200">
                <thead className="bg-slate-700/50 text-slate-300">
                  <tr>
                    <th className="px-4 py-3 font-medium">Material</th>
                    <th className="px-4 py-3 font-medium">Temp Zone 1</th>
                    <th className="px-4 py-3 font-medium">Temp Zone 2</th>
                    <th className="px-4 py-3 font-medium">Temp Zone 3</th>
                    <th className="px-4 py-3 font-medium">Temp Zone 4</th>
                    <th className="px-4 py-3 font-medium">Temp Zone 5</th>
                    <th className="px-4 py-3 font-medium">Pressure</th>
                    <th className="px-4 py-3 font-medium">Screw Speed</th>
                    <th className="px-4 py-3 font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {materials.map((material) => (
                    <tr key={material.id} className="border-t border-slate-700">
                      <td className="px-4 py-3 font-semibold whitespace-nowrap">{material.name}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.tempMinZ1, material.tempMaxZ1)}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.tempMinZ2, material.tempMaxZ2)}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.tempMinZ3, material.tempMaxZ3)}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.tempMinZ4, material.tempMaxZ4)}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.tempMinZ5, material.tempMaxZ5)}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.pressureMin, material.pressureMax)}</td>
                      <td className="px-4 py-3 whitespace-nowrap">{formatRange(material.speedMin, material.speedMax)}</td>
                      <td className="px-4 py-3">
                        <button
                          onClick={() => handleEdit(material)}
                          className="px-3 py-1 bg-blue-600 rounded"
                        >
                          Edit
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {ErrorComponent}
    </div>
  );
};

export default MaterialProfiles;