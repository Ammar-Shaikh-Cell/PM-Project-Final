import React, { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { reportsApi } from "../api/reports";
import { machinesApi } from "../api/machines";
import { useErrorToast } from "../components/ErrorToast";
import { ReportRequest } from "../types/api";

export default function ReportsPage() {
    const { showError, ErrorComponent } = useErrorToast();
    const format: "pdf" = "pdf";
    const [dateRange, setDateRange] = useState("24h");
    const [selectedMachine, setSelectedMachine] = useState<string>("all");

    const { data: machines = [] } = useQuery({
        queryKey: ["machines"],
        queryFn: () => machinesApi.list(),
    });

    const generateMutation = useMutation({
        mutationFn: async (payload: ReportRequest) => {
            const data = await reportsApi.generate(payload);
            return data;
        },
        onSuccess: async (data) => {
            try {
                let blob: Blob;
                let filename: string;
                
                // If data has url, download it
                if (data.url) {
                    filename = data.report_name || data.url.split("/").pop() || `report_${Date.now()}.${format}`;
                    blob = await reportsApi.download(filename);
                } else if (data.report_name) {
                    filename = data.report_name;
                    blob = await reportsApi.download(filename);
                } else {
                    throw new Error("No report URL or filename returned from backend");
                }
                
                // Validate blob before creating URL
                if (!blob || blob.size === 0) {
                    throw new Error("Invalid or empty file received");
                }
                
                // Create download link
                const url = window.URL.createObjectURL(blob);
                const link = document.createElement("a");
                link.href = url;
                link.download = filename;
                link.style.display = "none";
                document.body.appendChild(link);
                link.click();
                
                // Cleanup after a short delay to ensure download starts
                setTimeout(() => {
                    document.body.removeChild(link);
                    window.URL.revokeObjectURL(url);
                }, 100);
                
                showError("✅ Report generated and downloaded successfully!");
            } catch (error: any) {
                console.error("Download error:", error);
                showError(`❌ Failed to download PDF report: ${error.message || "Unknown error"}`);
            }
        },
        onError: (error: any) => {
            showError(`❌ Failed to generate PDF report: ${error.response?.data?.detail || error.message}`);
        },
    });

    const handleGenerate = () => {
        const now = new Date();
        const dateTo = new Date(now);
        let dateFrom = new Date(now);

        if (dateRange === "24h") {
            dateFrom.setHours(dateFrom.getHours() - 24);
        } else if (dateRange === "7d") {
            dateFrom.setDate(dateFrom.getDate() - 7);
        } else if (dateRange === "30d") {
            dateFrom.setDate(dateFrom.getDate() - 30);
        }

        const payload: ReportRequest = {
            format,
            date_from: dateFrom.toISOString(),
            date_to: dateTo.toISOString(),
            ...(selectedMachine !== "all" && { machine_id: selectedMachine }),
        };

        generateMutation.mutate(payload);
    };

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-3xl font-bold text-slate-100">Reports</h1>
                <p className="text-slate-400 mt-1">Generate and download reports</p>
            </div>

            <div className="bg-slate-900/70 border border-slate-700/40 rounded-2xl p-6">
                <h2 className="text-lg font-semibold text-slate-100 mb-4">Generate Report</h2>
                <div className="grid md:grid-cols-3 gap-4 mb-6">
                    <div>
                        <label className="block text-sm text-slate-400 mb-2">Format</label>
                        <div className="w-full px-4 py-2 bg-slate-800 border border-slate-700 rounded-lg text-slate-200">
                            PDF
                        </div>
                    </div>
                    <div>
                        <label className="block text-sm text-slate-400 mb-2">Date Range</label>
                        <select
                            value={dateRange}
                            onChange={(e) => setDateRange(e.target.value)}
                            className="w-full px-4 py-2 bg-slate-800 border border-slate-700 rounded-lg text-slate-200"
                        >
                            <option value="24h">Last 24 Hours</option>
                            <option value="7d">Last 7 Days</option>
                            <option value="30d">Last 30 Days</option>
                        </select>
                    </div>
                    <div>
                        <label className="block text-sm text-slate-400 mb-2">Machine</label>
                        <select
                            value={selectedMachine}
                            onChange={(e) => setSelectedMachine(e.target.value)}
                            className="w-full px-4 py-2 bg-slate-800 border border-slate-700 rounded-lg text-slate-200"
                        >
                            <option value="all">All Machines</option>
                            {machines.map((m: any) => (
                                <option key={m.id} value={m.id}>
                                    {m.name}
                                </option>
                            ))}
                        </select>
                    </div>
                </div>
                <button
                    onClick={handleGenerate}
                    disabled={generateMutation.isPending}
                    className="px-6 py-3 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg font-medium transition-colors disabled:opacity-50"
                >
                    {generateMutation.isPending ? "Generating..." : "Generate Report"}
                </button>
            </div>

            {ErrorComponent}
        </div>
    );
}

