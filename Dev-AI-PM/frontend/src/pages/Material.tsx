import React, { useState } from "react";

interface Material {
  id: number;
  name: string;
  tempMin: string;
  tempMax: string;
  pressureMin: string;
  pressureMax: string;
  speedMin: string;
  speedMax: string;
}

const MaterialProfiles = () => {
  const [materials, setMaterials] = useState<Material[]>([]);
  const [showForm, setShowForm] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);

  const [form, setForm] = useState({
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
  });
  

  // Handle input change
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setForm({ ...form, [e.target.name]: e.target.value });
  };

  // Reset form
  const resetForm = () => {
    setForm({
      name: "",
      tempMin: "",
      tempMax: "",
      pressureMin: "",
      pressureMax: "",
      speedMin: "",
      speedMax: "",
    });
    setEditingId(null);
  };

  // Save material
  const handleSave = () => {
    if (editingId !== null) {
      // Edit
      setMaterials((prev) =>
        prev.map((m) =>
          m.id === editingId ? { ...m, ...form } : m
        )
      );
    } else {
      // Create
      const newMaterial: Material = {
        id: Date.now(),
        ...form,
      };
      setMaterials((prev) => [...prev, newMaterial]);
    }

    resetForm();
    setShowForm(false);
  };

  // Edit click
  const handleEdit = (material: Material) => {
    setForm(material);
    setEditingId(material.id);
    setShowForm(true);
  };

  return (
    <div className="p-4 sm:p-6 text-slate-100">
      <div className="space-y-6">
        
        {/* Header */}
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-[#a551f4]">Material Profiles</h1>
            <p className="text-slate-400 mt-1">
              Manage your material profiles
            </p>
          </div>

          <button
            onClick={() => setShowForm(true)}
            className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 rounded-lg"
          >
            + Create Material Profile
          </button>
        </div>

        {/* Form */}
        {showForm && (
          <div className="bg-slate-800 p-6 rounded-xl space-y-4">
            
            <h2 className="text-xl font-semibold">
              {editingId ? "Edit Material" : "Create Material"}
            </h2>

            {/* Material Name */}
            <div>
              <label>Material Name</label>
              <input
                name="name"
                value={form.name}
                onChange={handleChange}
                className="w-full mt-1 p-2 rounded bg-slate-700"
              />
            </div>

            {/* Temp Zone */}
            <div>
              <label>Temp Zone 1</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="tempMin"
                  placeholder="Min"
                  value={form.tempMinZ1}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="tempMax"
                  placeholder="Max"
                  value={form.tempMaxZ1}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            <div>
              <label>Temp Zone 2</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="tempMin"
                  placeholder="Min"
                  value={form.tempMinZ2}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="tempMax"
                  placeholder="Max"
                  value={form.tempMaxZ2}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            <div>
              <label>Temp Zone 3</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="tempMin"
                  placeholder="Min"
                  value={form.tempMinZ3}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="tempMax"
                  placeholder="Max"
                  value={form.tempMaxZ3}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            <div>
              <label>Temp Zone 4</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="tempMin"
                  placeholder="Min"
                  value={form.tempMinZ4}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="tempMax"
                  placeholder="Max"
                  value={form.tempMaxZ4}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            <div>
              <label>Temp Zone 5</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="tempMin"
                  placeholder="Min"
                  value={form.tempMinZ5}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="tempMax"
                  placeholder="Max"
                  value={form.tempMaxZ5}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            {/* Pressure */}
            <div>
              <label>Pressure (bar)</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="pressureMin"
                  placeholder="Min"
                  value={form.pressureMin}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="pressureMax"
                  placeholder="Max"
                  value={form.pressureMax}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            {/* Screw Speed */}
            <div>
              <label>Screw Speed (rpm)</label>
              <div className="flex gap-2 mt-1">
                <input
                  name="speedMin"
                  placeholder="Min"
                  value={form.speedMin}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
                <input
                  name="speedMax"
                  placeholder="Max"
                  value={form.speedMax}
                  onChange={handleChange}
                  className="w-full p-2 rounded bg-slate-700"
                />
              </div>
            </div>

            {/* Buttons */}
            <div className="flex justify-end gap-3 pt-2">
              <button
                onClick={() => {
                  resetForm();
                  setShowForm(false);
                }}
                className="px-4 py-2 bg-slate-600 rounded-lg"
              >
                Cancel
              </button>

              <button
                onClick={handleSave}
                className="px-4 py-2 bg-emerald-600 rounded-lg"
              >
                Save
              </button>
            </div>
          </div>
        )}

        {/* List */}
        <div className="space-y-4">
          {materials.length === 0 ? (
            <div className="text-center py-10 text-slate-400">
              No materials found.
            </div>
          ) : (
            materials.map((m) => (
              <div
                key={m.id}
                className="bg-slate-800 p-4 rounded-xl flex justify-between items-center"
              >
                <div>
                  <p className="font-semibold">{m.name}</p>
                  <p className="text-sm text-slate-400">
                    Temp: {m.tempMin} - {m.tempMax} | Pressure: {m.pressureMin} - {m.pressureMax} | Speed: {m.speedMin} - {m.speedMax}
                  </p>
                </div>

                <button
                  onClick={() => handleEdit(m)}
                  className="px-3 py-1 bg-blue-600 rounded"
                >
                  Edit
                </button>
              </div>
            ))
          )}
        </div>

      </div>
    </div>
  );
};

export default MaterialProfiles;