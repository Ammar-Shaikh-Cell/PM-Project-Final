import React, { useState, useEffect } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import api from "../api";
import { useErrorToast } from "../components/ErrorToast";
import { useSSE } from "../hooks/useSSE";
// Using standard HTML elements instead of UI components

interface EmailRecipient {
    id: string;
    email: string;
    name: string | null;
    is_active: boolean;
    description: string | null;
    created_at: string;
    updated_at: string;
}

export default function NotificationsPage() {
    const { showError, ErrorComponent } = useErrorToast();
    const queryClient = useQueryClient();
    const [testEmail, setTestEmail] = useState<string>("");
    const [notifications, setNotifications] = useState<any[]>([]);
    const [showAddEmailModal, setShowAddEmailModal] = useState(false);
    const [newEmail, setNewEmail] = useState("");
    const [newName, setNewName] = useState("");
    const [newDescription, setNewDescription] = useState("");
    
    // Subscribe to real-time events via SSE
    useSSE("/ws/events/stream", (message) => {
        console.log("Notification event:", message.type, message.data);
        
        // Add notification to list
        setNotifications((prev) => {
            const newNotification = {
                id: Date.now(),
                type: message.type,
                data: message.data,
                timestamp: new Date(),
            };
            return [newNotification, ...prev].slice(0, 50); // Keep last 50
        });
        
        // Show toast for important events
        if (message.type === "alarm.created" || message.type === "prediction.created") {
            const severity = message.data?.severity || message.data?.status || "info";
            showError(`${message.type}: ${message.data?.message || JSON.stringify(message.data)}`);
        }
    });

    const testEmailMutation = useMutation({
        mutationFn: async (email?: string) => {
            const payload = email ? { to: email } : null;
            const { data } = await api.post("/notifications/test-email", payload);
            return data;
        },
        onSuccess: (data) => {
            if (data.ok) {
                showError("Test email sent successfully!");
            } else {
                showError(`Failed to send test email: ${data.error || "Unknown error"}`);
            }
        },
        onError: (error: any) => {
            showError(`Failed to send test email: ${error.response?.data?.error || error.message || "Unknown error"}`);
        },
    });

    const testWebhookMutation = useMutation({
        mutationFn: async (url: string) => {
            const { data } = await api.post("/notifications/test-webhook", {
                url,
                event_type: "test.event",
            });
            return data;
        },
        onSuccess: (data) => {
            if (data.ok) {
                showError("Test webhook triggered successfully!");
            } else {
                showError(`Failed to trigger webhook: ${data.error || "Unknown error"}`);
            }
        },
        onError: (error: any) => {
            showError(`Failed to trigger webhook: ${error.response?.data?.error || error.message || "Unknown error"}`);
        },
    });

    const handleTestEmail = () => {
        testEmailMutation.mutate(testEmail || undefined);
    };

    const handleTestWebhook = (url: string) => {
        if (!url) {
            showError("Please enter a webhook URL");
            return;
        }
        testWebhookMutation.mutate(url);
    };

    // Email Recipients Management
    const { data: emailRecipients = [], isLoading: loadingRecipients } = useQuery<EmailRecipient[]>({
        queryKey: ["email-recipients"],
        queryFn: async () => {
            const { data } = await api.get("/email-recipients");
            return data;
        },
    });

    const createRecipientMutation = useMutation({
        mutationFn: async (recipient: { email: string; name?: string; description?: string }) => {
            const { data } = await api.post("/email-recipients", recipient);
            return data;
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["email-recipients"] });
            setShowAddEmailModal(false);
            setNewEmail("");
            setNewName("");
            setNewDescription("");
            showError("Email recipient added successfully!");
        },
        onError: (error: any) => {
            showError(`Failed to add email recipient: ${error.response?.data?.detail || error.message}`);
        },
    });

    const deleteRecipientMutation = useMutation({
        mutationFn: async (id: string) => {
            await api.delete(`/email-recipients/${id}`);
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["email-recipients"] });
            showError("Email recipient removed successfully!");
        },
        onError: (error: any) => {
            showError(`Failed to remove email recipient: ${error.response?.data?.detail || error.message}`);
        },
    });

    const toggleRecipientMutation = useMutation({
        mutationFn: async ({ id, is_active }: { id: string; is_active: boolean }) => {
            const { data } = await api.patch(`/email-recipients/${id}`, { is_active: !is_active });
            return data;
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ["email-recipients"] });
            showError("Email recipient status updated!");
        },
        onError: (error: any) => {
            showError(`Failed to update email recipient: ${error.response?.data?.detail || error.message}`);
        },
    });

    const handleAddEmail = () => {
        if (!newEmail) {
            showError("Please enter an email address");
            return;
        }
        createRecipientMutation.mutate({
            email: newEmail,
            name: newName || null,
            description: newDescription || null,
        });
    };

    const handleDeleteEmail = (id: string) => {
        if (window.confirm("Are you sure you want to remove this email recipient?")) {
            deleteRecipientMutation.mutate(id);
        }
    };

    const handleToggleActive = (id: string, is_active: boolean) => {
        toggleRecipientMutation.mutate({ id, is_active });
    };

    return (
        <div className="space-y-6">
            <div>
                <h1 className="text-3xl font-bold text-slate-900">Notifications</h1>
                <p className="text-slate-600 mt-1">Test and manage email notifications and webhooks</p>
            </div>

            {/* Email Recipients Management */}
            <div className="bg-white/90 border border-slate-200 rounded-2xl p-6 shadow-sm">
                <div className="flex items-center justify-between mb-4">
                    <div>
                        <h2 className="text-lg font-semibold text-slate-900">Email Recipients</h2>
                        <p className="text-slate-600 text-sm mt-1">
                            Manage email addresses that receive notifications
                        </p>
                    </div>
                    <button
                        onClick={() => setShowAddEmailModal(true)}
                        className="px-4 py-2 bg-purple-700 hover:bg-purple-600 text-white rounded-lg font-medium transition-colors text-sm"
                    >
                        + Add Email
                    </button>
                </div>

                {loadingRecipients ? (
                    <div className="text-center py-8 text-slate-600">Loading recipients...</div>
                ) : emailRecipients.length === 0 ? (
                    <div className="text-center py-8 text-slate-600">
                        <p className="mb-2">No email recipients configured.</p>
                        <p className="text-sm">Add an email address to start receiving notifications.</p>
                    </div>
                ) : (
                    <div className="space-y-3">
                        {emailRecipients.map((recipient) => (
                            <div
                                key={recipient.id}
                                className={`p-4 rounded-lg border ${
                                    recipient.is_active
                                        ? "bg-slate-50 border-slate-200"
                                        : "bg-slate-100 border-slate-300 opacity-60"
                                }`}
                            >
                                <div className="flex items-start justify-between">
                                    <div className="flex-1">
                                        <div className="flex items-center gap-2 mb-1">
                                            <span className="font-medium text-slate-900">
                                                {recipient.name || recipient.email}
                                            </span>
                                            {recipient.name && (
                                                <span className="text-sm text-slate-500">
                                                    ({recipient.email})
                                                </span>
                                            )}
                                            <span
                                                className={`px-2 py-0.5 rounded text-xs font-medium ${
                                                    recipient.is_active
                                                        ? "bg-green-100 text-green-700"
                                                        : "bg-slate-200 text-slate-600"
                                                }`}
                                            >
                                                {recipient.is_active ? "Active" : "Inactive"}
                                            </span>
                                        </div>
                                        {recipient.description && (
                                            <p className="text-sm text-slate-600 mt-1">
                                                {recipient.description}
                                            </p>
                                        )}
                                        <p className="text-xs text-slate-500 mt-2">
                                            Added: {new Date(recipient.created_at).toLocaleDateString()}
                                        </p>
                                    </div>
                                    <div className="flex items-center gap-2 ml-4">
                                        <button
                                            onClick={() => handleToggleActive(recipient.id, recipient.is_active)}
                                            className={`px-3 py-1.5 rounded text-sm font-medium transition-colors ${
                                                recipient.is_active
                                                    ? "bg-yellow-100 text-yellow-700 hover:bg-yellow-200"
                                                    : "bg-green-100 text-green-700 hover:bg-green-200"
                                            }`}
                                            disabled={toggleRecipientMutation.isPending}
                                        >
                                            {recipient.is_active ? "Disable" : "Enable"}
                                        </button>
                                        <button
                                            onClick={() => handleDeleteEmail(recipient.id)}
                                            className="px-3 py-1.5 bg-red-100 text-red-700 rounded text-sm font-medium hover:bg-red-200 transition-colors"
                                            disabled={deleteRecipientMutation.isPending}
                                        >
                                            Remove
                                        </button>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* Add Email Modal */}
            {showAddEmailModal && (
                <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
                    <div className="bg-white rounded-2xl p-6 max-w-md w-full mx-4 shadow-xl">
                        <h3 className="text-xl font-semibold text-slate-900 mb-4">Add Email Recipient</h3>
                        <div className="space-y-4">
                            <div>
                                <label className="block text-sm text-slate-700 mb-2">
                                    Email Address <span className="text-red-500">*</span>
                                </label>
                                <input
                                    type="email"
                                    value={newEmail}
                                    onChange={(e) => setNewEmail(e.target.value)}
                                    placeholder="recipient@example.com"
                                    className="w-full px-4 py-2 bg-white border border-slate-200 rounded-lg text-slate-900 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-700 mb-2">Name (optional)</label>
                                <input
                                    type="text"
                                    value={newName}
                                    onChange={(e) => setNewName(e.target.value)}
                                    placeholder="John Doe"
                                    className="w-full px-4 py-2 bg-white border border-slate-200 rounded-lg text-slate-900 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-slate-700 mb-2">Description (optional)</label>
                                <textarea
                                    value={newDescription}
                                    onChange={(e) => setNewDescription(e.target.value)}
                                    placeholder="Optional notes about this recipient"
                                    rows={3}
                                    className="w-full px-4 py-2 bg-white border border-slate-200 rounded-lg text-slate-900 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300"
                                />
                            </div>
                            <div className="flex gap-3 pt-2">
                                <button
                                    onClick={handleAddEmail}
                                    disabled={createRecipientMutation.isPending || !newEmail}
                                    className="flex-1 px-4 py-2 bg-purple-700 hover:bg-purple-600 text-white rounded-lg font-medium transition-colors disabled:opacity-50"
                                >
                                    {createRecipientMutation.isPending ? "Adding..." : "Add Email"}
                                </button>
                                <button
                                    onClick={() => {
                                        setShowAddEmailModal(false);
                                        setNewEmail("");
                                        setNewName("");
                                        setNewDescription("");
                                    }}
                                    className="px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 rounded-lg font-medium transition-colors"
                                >
                                    Cancel
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Email Notifications Test */}
            <div className="bg-white/90 border border-slate-200 rounded-2xl p-6 shadow-sm">
                <h2 className="text-lg font-semibold text-slate-900 mb-4">Test Email Notifications</h2>
                <p className="text-slate-600 text-sm mb-4">
                    Test email notifications. Emails are automatically sent to all active recipients for:
                </p>
                <ul className="text-slate-700 text-sm mb-4 space-y-1 list-disc list-inside">
                    <li>User registration (welcome email)</li>
                    <li>Critical/Warning AI predictions</li>
                    <li>Alarm triggers</li>
                </ul>
                <div className="space-y-4">
                    <div>
                        <label htmlFor="test-email" className="block text-sm text-slate-700 mb-2">Test Email Address (optional)</label>
                        <input
                            id="test-email"
                            type="email"
                            placeholder="test@example.com"
                            value={testEmail}
                            onChange={(e) => setTestEmail(e.target.value)}
                            className="w-full px-4 py-2 bg-white border border-slate-200 rounded-lg text-slate-900 mt-2 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300"
                        />
                        <p className="text-xs text-slate-500 mt-1">
                            Leave empty to send to all active recipients
                        </p>
                    </div>
                    <button
                        onClick={handleTestEmail}
                        disabled={testEmailMutation.isPending}
                        className="px-6 py-3 bg-purple-700 hover:bg-purple-600 text-white rounded-lg font-medium transition-colors disabled:opacity-50"
                    >
                        {testEmailMutation.isPending ? "Sending..." : "Send Test Email"}
                    </button>
                </div>
            </div>

            {/* Webhook Notifications */}
            <div className="bg-white/90 border border-slate-200 rounded-2xl p-6 shadow-sm">
                <h2 className="text-lg font-semibold text-slate-900 mb-4">Webhook Notifications</h2>
                <p className="text-slate-600 text-sm mb-4">
                    Test webhook endpoints. Webhooks are triggered for:
                </p>
                <ul className="text-slate-700 text-sm mb-4 space-y-1 list-disc list-inside">
                    <li>Alarm events (critical/warning)</li>
                    <li>Prediction events (anomalies detected)</li>
                    <li>Machine status changes</li>
                </ul>
                <div className="space-y-4">
                    <div>
                        <label htmlFor="test-webhook-url" className="block text-sm text-slate-700 mb-2">Webhook URL</label>
                        <input
                            id="test-webhook-url"
                            type="url"
                            placeholder="https://your-webhook-endpoint.com/webhook"
                            onKeyDown={(e) => {
                                if (e.key === "Enter") {
                                    handleTestWebhook(e.currentTarget.value);
                                }
                            }}
                            className="w-full px-4 py-2 bg-white border border-slate-200 rounded-lg text-slate-900 mt-2 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300"
                        />
                    </div>
                    <button
                        onClick={(e) => {
                            const input = document.getElementById("test-webhook-url") as HTMLInputElement;
                            handleTestWebhook(input?.value || "");
                        }}
                        disabled={testWebhookMutation.isPending}
                        className="px-6 py-3 bg-purple-700 hover:bg-purple-600 text-white rounded-lg font-medium transition-colors disabled:opacity-50"
                    >
                        {testWebhookMutation.isPending ? "Testing..." : "Test Webhook"}
                    </button>
                </div>
            </div>

            {/* Real-time Notifications Feed */}
            <div className="bg-white/90 border border-slate-200 rounded-2xl p-6 shadow-sm">
                <h2 className="text-lg font-semibold text-slate-900 mb-4">Real-time Notifications Feed</h2>
                <div className="space-y-2 max-h-96 overflow-y-auto">
                    {notifications.length === 0 ? (
                        <p className="text-slate-600 text-sm">No notifications yet. Events will appear here in real-time.</p>
                    ) : (
                        notifications.map((notif) => (
                            <div
                                key={notif.id}
                                className="bg-slate-50 p-3 rounded-lg border border-slate-200"
                            >
                                <div className="flex items-start justify-between">
                                    <div className="flex-1">
                                        <div className="text-sm font-medium text-slate-900">
                                            {notif.type}
                                        </div>
                                        <div className="text-xs text-slate-500 mt-1">
                                            {notif.timestamp.toLocaleTimeString()}
                                        </div>
                                        <pre className="text-xs text-slate-700 mt-2 bg-white p-2 rounded overflow-x-auto border border-slate-200">
                                            {JSON.stringify(notif.data, null, 2)}
                                        </pre>
                                    </div>
                                </div>
                            </div>
                        ))
                    )}
                </div>
            </div>

            {/* API Endpoints Info */}
            <div className="bg-white/90 border border-slate-200 rounded-2xl p-6 shadow-sm">
                <h2 className="text-lg font-semibold text-slate-900 mb-4">API Endpoints</h2>
                <div className="space-y-3 text-sm">
                    <div className="bg-slate-50 p-3 rounded-lg border border-slate-200">
                        <code className="text-purple-700">POST /notifications/test-email</code>
                        <p className="text-slate-600 mt-1">Send a test email notification</p>
                        <pre className="text-xs text-slate-600 mt-2 bg-white p-2 rounded border border-slate-200">
{`Body (optional):
{
  "to": "email@example.com"
}`}
                        </pre>
                    </div>
                    <div className="bg-slate-50 p-3 rounded-lg border border-slate-200">
                        <code className="text-purple-700">POST /notifications/test-webhook</code>
                        <p className="text-slate-600 mt-1">Test a webhook endpoint</p>
                        <pre className="text-xs text-slate-600 mt-2 bg-white p-2 rounded border border-slate-200">
{`Body:
{
  "url": "https://your-webhook.com",
  "event_type": "test.event"
}`}
                        </pre>
                    </div>
                    <div className="bg-slate-50 p-3 rounded-lg border border-slate-200">
                        <p className="text-slate-600">
                            Full API documentation available at:{" "}
                            <a
                                href="http://localhost:8000/docs"
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-purple-700 hover:underline"
                            >
                                http://localhost:8000/docs
                            </a>
                        </p>
                    </div>
                </div>
            </div>

            {ErrorComponent}
        </div>
    );
}

