document.addEventListener("DOMContentLoaded", () => {
    const banner = document.getElementById("result-banner");
    const navItems = document.querySelectorAll(".nav-item");
    const sections = document.querySelectorAll(".content-area");
    const pageTitle = document.getElementById("page-title");

    const refreshBtn = document.getElementById("refresh-btn");
    const simulateBtn = document.getElementById("simulate-btn");
    const agentBtn = document.getElementById("agent-btn");
    const occResetBtn = document.getElementById("occ-reset-btn");
    const occBtn = document.getElementById("occ-btn");

    const pageTitles = {
        dashboard: "Production Optimistic Concurrency Workflow",
        applications: "Applications",
        events: "Event Stream",
    };

    const occState = {
        appId: null,
        streamId: null,
        summary: null,
        messages: [],
        streamEvents: [],
    };

    function escapeHtml(value) {
        return String(value ?? "")
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
    }

    function formatMoney(value) {
        const amount = Number(value || 0);
        return amount ? `$${amount.toLocaleString()}` : "$0";
    }

    function formatDate(value) {
        if (!value) return "-";
        return new Date(value).toLocaleString();
    }

    function formatJsonSnippet(value) {
        const text = JSON.stringify(value ?? {});
        if (text.length <= 92) {
            return text;
        }
        return `${text.slice(0, 89)}...`;
    }

    async function readApiPayload(response) {
        const contentType = response.headers.get("content-type") || "";
        if (contentType.includes("application/json")) {
            try {
                return await response.json();
            } catch {
                return {};
            }
        }

        const text = await response.text();
        return { message: text || `HTTP ${response.status}` };
    }

    async function postJson(url) {
        const res = await fetch(url, { method: "POST" });
        const data = await readApiPayload(res);
        return { res, data };
    }

    function showBanner(kind, title, lines) {
        banner.classList.remove("hidden");
        const palette = {
            success: {
                background: "rgba(21, 94, 58, 0.45)",
                border: "rgba(28, 183, 108, 0.30)",
                title: "#9de8c1",
            },
            error: {
                background: "rgba(125, 53, 43, 0.52)",
                border: "rgba(217, 95, 86, 0.30)",
                title: "#ffc1bc",
            },
            info: {
                background: "rgba(36, 103, 171, 0.34)",
                border: "rgba(78, 163, 255, 0.25)",
                title: "#d7ebff",
            },
        }[kind] || {
            background: "rgba(255,255,255,0.05)",
            border: "rgba(255,255,255,0.1)",
            title: "#f8fbff",
        };

        banner.style.background = palette.background;
        banner.style.borderColor = palette.border;
        banner.innerHTML = `
            <strong style="color:${palette.title}">${escapeHtml(title)}</strong>
            <div style="margin-top:10px; display:grid; gap:6px;">
                ${lines.map((line) => `<div>${escapeHtml(line)}</div>`).join("")}
            </div>
        `;
    }

    function hideBanner() {
        banner.classList.add("hidden");
    }

    function messageCardTone(tone) {
        if (tone === "success" || tone === "warning" || tone === "info") {
            return tone;
        }
        return "info";
    }

    function renderOccSummary() {
        const summary = occState.summary;
        const versionEl = document.getElementById("occ-card-version");
        const streamEl = document.getElementById("occ-card-stream");
        const streamMetaEl = document.getElementById("occ-card-stream-meta");
        const amountEl = document.getElementById("occ-card-amount");
        const statusEl = document.getElementById("occ-card-status");
        const expectedEl = document.getElementById("occ-card-expected");
        const latestBadgeEl = document.getElementById("occ-card-latest-badge");
        const latestEventEl = document.getElementById("occ-card-latest-event");
        const scenarioStatusEl = document.getElementById("occ-scenario-status");

        if (!summary) {
            streamEl.textContent = "Not initialized";
            versionEl.textContent = "v--";
            versionEl.className = "summary-pill neutral";
            streamMetaEl.textContent = "Use the reset control to seed a production-style loan workflow stream.";
            amountEl.textContent = "$0";
            statusEl.textContent = "No aggregate has been initialized yet.";
            expectedEl.textContent = "v--";
            latestBadgeEl.textContent = "Idle";
            latestBadgeEl.className = "summary-pill neutral";
            latestEventEl.textContent = "Latest event: Awaiting initialization";
            scenarioStatusEl.textContent = "Awaiting initialization";
            return;
        }

        streamEl.textContent = summary.stream_id;
        versionEl.textContent = `v${summary.current_version}`;
        versionEl.className = "summary-pill";
        streamMetaEl.textContent = `Scenario ${summary.app_id} is seeded and ready for two writers to target the same version.`;
        amountEl.textContent = formatMoney(summary.requested_amount_usd);
        statusEl.textContent = `Current stream head is ${summary.latest_event_type}; the next append will expect v${summary.expected_version}.`;
        expectedEl.textContent = `v${summary.expected_version}`;
        latestBadgeEl.textContent = summary.latest_event_type || "Ready";
        latestBadgeEl.className = "summary-pill";
        latestEventEl.textContent = `Latest event: ${summary.latest_event_type}`;
        scenarioStatusEl.textContent = `Scenario ready: ${summary.app_id}`;
    }

    function renderOccMessages() {
        const container = document.getElementById("occ-messages");
        const note = document.getElementById("occ-context-note");

        if (!occState.messages.length) {
            note.textContent = "Notice how the second writer reloads the stream and retries after the version conflict.";
            container.innerHTML = `
                <div class="message-card info empty-state">
                    <strong>Ready when you are</strong>
                    <p>Initialize a production-style stream to seed version 2, then run the workflow to see OCC collision handling unfold.</p>
                </div>
            `;
            return;
        }

        note.textContent = "The message stack below is built from the real retry logs and assertions returned by the concurrency endpoint.";
        container.innerHTML = occState.messages
            .map((message) => {
                const tone = messageCardTone(message.tone);
                return `
                    <div class="message-card ${tone}">
                        <strong>${escapeHtml(message.title)}</strong>
                        <p>${escapeHtml(message.body)}</p>
                    </div>
                `;
            })
            .join("");
    }

    function renderOccEvents() {
        const tbody = document.getElementById("occ-events-table-body");
        if (!occState.streamEvents.length) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="6" class="empty-row">Initialize the scenario to populate the append-only log.</td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = occState.streamEvents
            .map((event) => `
                <tr>
                    <td class="mono">${escapeHtml(event.version)}</td>
                    <td class="mono">${escapeHtml(event.aggregate_id)}</td>
                    <td>${escapeHtml(event.aggregate_type)}</td>
                    <td>${escapeHtml(event.event_type)}</td>
                    <td><span class="data-chip">${escapeHtml(formatJsonSnippet(event.data))}</span></td>
                    <td class="mono">${escapeHtml(event.event_id)}</td>
                </tr>
            `)
            .join("");
    }

    function applyOccPayload(data) {
        occState.appId = data.app_id || null;
        occState.streamId = data.stream_id || null;
        occState.summary = data.summary || null;
        occState.messages = data.messages || [];
        occState.streamEvents = data.stream_events || [];
        renderOccSummary();
        renderOccMessages();
        renderOccEvents();
    }

    async function initializeOccScenario(showInfoBanner = true) {
        let { res, data } = await postJson("/api/concurrency_reset");

        // Backward compatibility: some running UI server versions expose only
        // /api/concurrency_test and not /api/concurrency_reset.
        if (res.status === 404 || res.status === 405) {
            ({ res, data } = await postJson("/api/concurrency_test"));
        }

        if (!res.ok || data.status === "error") {
            const detail = data.detail || data.reason || data.message;
            throw new Error(detail ? String(detail) : `Initialization failed (HTTP ${res.status})`);
        }

        applyOccPayload(data);
        if (showInfoBanner) {
            showBanner("info", "Production OCC Scenario Initialized", [
                `Application ID: ${data.app_id}`,
                `Stream ID: ${data.stream_id}`,
                `Current Version: v${data.summary?.current_version ?? 0}`,
            ]);
        }
    }

    async function runOccScenario() {
        if (!occState.appId) {
            await initializeOccScenario(false);
        }
        const params = new URLSearchParams({ app_id: occState.appId });
        const { res, data } = await postJson(`/api/concurrency_test?${params.toString()}`);
        if (!res.ok || data.status === "error") {
            throw new Error(data.reason || data.message || `Failed to execute concurrency scenario (HTTP ${res.status})`);
        }
        applyOccPayload(data);
    }

    async function fetchEvents() {
        try {
            const res = await fetch("/api/events");
            const events = await res.json();
            const tbody = document.getElementById("events-table-body");
            tbody.innerHTML = "";

            events.forEach((event) => {
                const row = document.createElement("tr");
                row.innerHTML = `
                    <td>${escapeHtml(event.global_position)}</td>
                    <td>${escapeHtml(event.stream_position ?? "-")}</td>
                    <td class="mono">${escapeHtml(event.stream_id)}</td>
                    <td>${escapeHtml(event.event_type)}</td>
                    <td>${escapeHtml(formatDate(event.recorded_at))}</td>
                `;
                tbody.appendChild(row);
            });
        } catch (error) {
            console.error("Failed to fetch events", error);
        }
    }

    async function fetchApplications() {
        try {
            const res = await fetch("/api/applications");
            const applications = await res.json();
            const tbody = document.getElementById("applications-table-body");
            tbody.innerHTML = "";

            applications.forEach((app) => {
                const stateTone =
                    app.state === "APPROVED"
                        ? "success"
                        : app.state === "DECLINED"
                          ? "danger"
                          : app.state && app.state.includes("PENDING")
                            ? "warning"
                            : "neutral";

                const row = document.createElement("tr");
                row.innerHTML = `
                    <td class="mono">${escapeHtml(app.application_id)}</td>
                    <td>${escapeHtml(app.applicant_id || "-")}</td>
                    <td><span class="state-pill ${stateTone}">${escapeHtml(app.state || "NEW")}</span></td>
                    <td>${escapeHtml(formatMoney(app.requested_amount_usd))}</td>
                    <td>${app.confidence ? `${Math.round(Number(app.confidence) * 100)}%` : "-"}</td>
                    <td>${escapeHtml(formatDate(app.last_updated))}</td>
                `;
                tbody.appendChild(row);
            });
        } catch (error) {
            console.error("Failed to fetch applications", error);
        }
    }

    function updateSupportingViews() {
        fetchApplications();
        fetchEvents();
    }

    navItems.forEach((item) => {
        item.addEventListener("click", (event) => {
            event.preventDefault();
            const view = item.dataset.view;

            navItems.forEach((navItem) => navItem.classList.remove("active"));
            item.classList.add("active");
            pageTitle.textContent = pageTitles[view] || "Command Center";

            sections.forEach((section) => {
                section.classList.toggle("hidden", section.id !== `view-${view}`);
            });

            if (view === "applications") {
                fetchApplications();
            }
            if (view === "events") {
                fetchEvents();
            }
        });
    });

    refreshBtn.addEventListener("click", () => {
        hideBanner();
        updateSupportingViews();
        renderOccSummary();
        renderOccMessages();
        renderOccEvents();
    });

    occResetBtn.addEventListener("click", async () => {
        occResetBtn.disabled = true;
        occResetBtn.textContent = "Initializing...";
        try {
            await initializeOccScenario(true);
            updateSupportingViews();
        } catch (error) {
            showBanner("error", "Initialization Failed", [error.message || "Unknown reset error"]);
        } finally {
            occResetBtn.disabled = false;
            occResetBtn.textContent = "Initialize / Reset Aggregates";
        }
    });

    occBtn.addEventListener("click", async () => {
        occBtn.disabled = true;
        occBtn.textContent = "Running OCC...";
        try {
            await runOccScenario();
            showBanner("success", "Optimistic Concurrency Collision Resolved", [
                `Scenario: ${occState.appId}`,
                `Winning stream: ${occState.streamId}`,
                `Final version: v${occState.summary?.current_version ?? "-"}`,
            ]);
            updateSupportingViews();
        } catch (error) {
            showBanner("error", "Concurrency Run Failed", [error.message || "Unknown OCC error"]);
        } finally {
            occBtn.disabled = false;
            occBtn.textContent = "🚀 Invoke Production Workflow (Triggers OCC Collision)";
        }
    });

    simulateBtn.addEventListener("click", async () => {
        simulateBtn.disabled = true;
        simulateBtn.textContent = "Simulating...";
        try {
            const { res, data } = await postJson("/api/simulate");
            if (!res.ok || data.status === "error") {
                throw new Error(data.reason || data.message || "Simulation failed");
            }
            showBanner("success", "Synthetic Loan Application Created", [
                `Application ID: ${data.app_id}`,
                `Company ID: ${data.company_id}`,
                `Requested Amount: ${formatMoney(data.requested_amount_usd)}`,
            ]);
            updateSupportingViews();
        } catch (error) {
            showBanner("error", "Simulation Failed", [error.message || "Unknown simulation error"]);
        } finally {
            simulateBtn.disabled = false;
            simulateBtn.textContent = "Simulate Loan Traffic";
        }
    });

    agentBtn.addEventListener("click", async () => {
        agentBtn.disabled = true;
        agentBtn.textContent = "Running Agent...";
        try {
            let { res, data } = await postJson("/api/run_agent");

            // UX guardrail: if no credit-ready app exists, create one first and run again.
            if (data.status === "error" && data.reason === "no credit-ready applications available") {
                const seeded = await postJson("/api/simulate");
                if (!seeded.res.ok || seeded.data.status === "error") {
                    throw new Error(seeded.data.reason || seeded.data.message || "Unable to seed an application for agent run");
                }
                ({ res, data } = await postJson(`/api/run_agent?app_id=${encodeURIComponent(seeded.data.app_id)}`));
            }

            if (!res.ok || data.status === "error") {
                throw new Error(data.reason || data.message || "Agent run failed");
            }
            showBanner("success", "Credit Agent Completed", [
                `Application ID: ${data.app_id}`,
                `Session ID: ${data.session_id}`,
                "Credit analysis results were appended and replayed into projections.",
            ]);
            updateSupportingViews();
        } catch (error) {
            showBanner("error", "Agent Run Failed", [error.message || "Unknown agent error"]);
        } finally {
            agentBtn.disabled = false;
            agentBtn.textContent = "Run Credit Agent";
        }
    });

    renderOccSummary();
    renderOccMessages();
    renderOccEvents();
    updateSupportingViews();
    setInterval(updateSupportingViews, 5000);
});
