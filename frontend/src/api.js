const API_BASE = '/api';

export const fetchRecentLogs = async () => {
    const res = await fetch(`${API_BASE}/logs/recent`);
    if (!res.ok) throw new Error('Failed to fetch logs');
    return res.json();
};

export const analyzeLogs = async (runId, logs) => {
    const res = await fetch(`${API_BASE}/analyze`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ run_id: runId, logs }),
    });
    if (!res.ok) throw new Error('Analysis failed');
    return res.json();
};

export const writeToNotebook = async (path, content) => {
    const res = await fetch(`${API_BASE}/recommendation/to-notebook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ notebook_path: path, content }),
    });
    if (!res.ok) throw new Error('Failed to write to notebook');
    return res.json();
};
