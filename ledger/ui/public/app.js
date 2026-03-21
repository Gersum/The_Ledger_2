document.addEventListener('DOMContentLoaded', () => {
    // Nav logic
    const navItems = document.querySelectorAll('.nav-item');
    const sections = document.querySelectorAll('.content-area');
    const pageTitle = document.getElementById('page-title');

    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            navItems.forEach(n => n.classList.remove('active'));
            item.classList.add('active');
            
            const view = item.getAttribute('data-view');
            pageTitle.textContent = view.charAt(0).toUpperCase() + view.slice(1) + (view === 'dashboard' ? ' Dashboard' : '');
            
            sections.forEach(sec => {
                if(sec.id === `view-${view}`) sec.classList.remove('hidden');
                else sec.classList.add('hidden');
            });

            if (view === 'applications') fetchApplications();
            if (view === 'events') fetchEvents();
        });
    });

    const refreshBtn = document.getElementById('refresh-btn');
    refreshBtn.addEventListener('click', updateAllData);

    const simulateBtn = document.getElementById('simulate-btn');
    simulateBtn.addEventListener('click', async () => {
        simulateBtn.textContent = 'Simulating...';
        try {
            await fetch('/api/simulate', { method: 'POST' });
        } catch (e) {
            console.log("Error simulating");
        }
        setTimeout(() => { simulateBtn.textContent = 'Simulate Traffic'; updateAllData(); }, 1500);
    });

    const occBtn = document.getElementById('occ-btn');
    if (occBtn) {
        occBtn.addEventListener('click', async () => {
            occBtn.textContent = 'Running OCC...';
            try { 
                const res = await fetch('/api/concurrency_test', { method: 'POST' }); 
                const data = await res.json();
                const banner = document.getElementById('result-banner');
                banner.classList.remove('hidden');
                banner.style.backgroundColor = 'rgba(239, 68, 68, 0.1)';
                banner.style.color = 'var(--text-primary)';
                banner.style.border = '1px solid var(--danger)';
                
                let html = `<strong style="color:var(--danger)">⚡ Optimistic Concurrency Event (With Retry Loop!)</strong><br><br>
                <div style="display:flex; gap: 2rem; margin-bottom: 1.5rem;">
                    <div style="flex:1;">
                        <strong style="color:var(--accent-primary)">${data.results[0].agent}</strong><br>
                        ${data.results[0].logs.map(l => `<div style="margin-top:4px; opacity:0.9; white-space: pre-wrap; font-size: 0.8rem;">${l}</div>`).join('')}
                    </div>
                    <div style="flex:1;">
                        <strong style="color:var(--warning)">${data.results[1].agent}</strong><br>
                        ${data.results[1].logs.map(l => `<div style="margin-top:4px; opacity:0.9; white-space: pre-wrap; font-size: 0.8rem;">${l}</div>`).join('')}
                    </div>
                </div>
                <div style="border-top: 1px solid rgba(255,255,255,0.1); padding-top: 1rem;">
                    ${(data.assertions || []).map(a => `<div style="color:var(--success); margin-top:4px; font-weight:600;">${a}</div>`).join('')}
                </div>`;
                
                banner.innerHTML = html;
            } catch (e) {}
            setTimeout(() => { occBtn.textContent = 'Test Concurrency'; updateAllData(); }, 1000);
        });
    }

    const agentBtn = document.getElementById('agent-btn');
    if (agentBtn) {
        agentBtn.addEventListener('click', async () => {
            agentBtn.textContent = 'Agent Running...';
            try { 
                const res = await fetch('/api/run_agent', { method: 'POST' }); 
                const data = await res.json();
                const banner = document.getElementById('result-banner');
                banner.classList.remove('hidden');
                banner.style.backgroundColor = 'rgba(16, 185, 129, 0.1)';
                banner.style.color = 'var(--text-primary)';
                banner.style.border = '1px solid var(--success)';
                banner.innerHTML = `<strong style="color:var(--success)">🤖 LangGraph Agent Simulation Complete!</strong><br><br>Session ID: <code>${data.session_id}</code><br><span style="color:var(--text-secondary)">5 nodes were sequentially executed. The final decisions and token usage have been emitted to the Event Store metrics over on the right.</span>`;
            } catch (e) {}
            setTimeout(() => { agentBtn.textContent = 'Run AI Agent'; updateAllData(); }, 2500);
        });
    }

    // Fetches

    async function fetchMetrics() {
        try {
            const res = await fetch('/api/metrics');
            const data = await res.json();
            
            document.getElementById('metric-total').textContent = data.stats?.total || 0;
            document.getElementById('metric-approved').textContent = data.stats?.approved || 0;
            document.getElementById('metric-declined').textContent = data.stats?.declined || 0;

            const agentBody = document.getElementById('agent-metrics-body');
            agentBody.innerHTML = '';
            (data.agent_performance || []).forEach(agent => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td><span class="badge" style="background:rgba(255,255,255,0.1);color:#fff">${agent.agent_type}</span></td>
                    <td>${agent.total_sessions}</td>
                    <td>${(agent.total_tokens / 1000).toFixed(1)}k</td>
                    <td>$${Number(agent.total_cost_usd).toFixed(4)}</td>
                `;
                agentBody.appendChild(tr);
            });
        } catch (err) {
            console.error('Failed to fetch metrics', err);
        }
    }

    async function fetchEvents() {
        try {
            const res = await fetch('/api/events');
            const events = await res.json();
            
            // Populate full table
            const tbody = document.getElementById('events-table-body');
            tbody.innerHTML = '';
            
            // Populate live feed (only top 10)
            const feed = document.getElementById('live-feed');
            feed.innerHTML = '';

            let maxEv = events.length;
            document.getElementById('metric-events').textContent = maxEv > 0 ? events[0].global_position : 0;

            events.forEach((ev, idx) => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${ev.global_position}</td>
                    <td>${ev.stream_id.substring(0,25)}...</td>
                    <td class="event-type" style="color:var(--accent-hover)">${ev.event_type}</td>
                    <td>${new Date(ev.recorded_at).toLocaleTimeString()}</td>
                `;
                tbody.appendChild(tr);

                if (idx < 15) {
                    const div = document.createElement('div');
                    div.className = 'event-item';
                    div.innerHTML = `
                        <div class="event-meta">
                            <span>Pos: ${ev.global_position}</span>
                            <span>${new Date(ev.recorded_at).toLocaleTimeString()}</span>
                        </div>
                        <div class="event-type">${ev.event_type}</div>
                        <div style="font-size:0.75rem; color:var(--text-secondary); margin-top:0.25rem">Stream: ${ev.stream_id}</div>
                    `;
                    feed.appendChild(div);
                }
            });
        } catch (err) {
            console.error('Failed to fetch events', err);
        }
    }

    async function fetchApplications() {
        try {
            const res = await fetch('/api/applications');
            const apps = await res.json();
            const tbody = document.getElementById('applications-table-body');
            tbody.innerHTML = '';
            
            apps.forEach(app => {
                const tr = document.createElement('tr');
                let color = "var(--text-secondary)";
                if (app.state === 'APPROVED') color = "var(--success)";
                if (app.state === 'DECLINED') color = "var(--danger)";
                if (app.state.includes('PENDING')) color = "var(--warning)";

                tr.innerHTML = `
                    <td style="font-family:monospace">${app.application_id}</td>
                    <td>${app.applicant_id}</td>
                    <td><span style="color:${color}; font-weight:600">${app.state}</span></td>
                    <td>$${Number(app.requested_amount_usd).toLocaleString()}</td>
                    <td>${app.confidence ? (app.confidence*100).toFixed(0) + '%' : '-'}</td>
                    <td style="font-size:0.75rem">${new Date(app.last_updated).toLocaleString()}</td>
                `;
                tbody.appendChild(tr);
            });
        } catch (err) {
            console.error('Failed to fetch applications', err);
        }
    }

    function updateAllData() {
        fetchMetrics();
        fetchEvents();
        fetchApplications();
    }

    // Initial load
    updateAllData();

    // Auto-refresh every 5 seconds for that real-time feel
    setInterval(updateAllData, 5000);
});
