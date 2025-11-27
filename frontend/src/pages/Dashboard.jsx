import React, { useEffect, useState } from 'react';
import { fetchRecentLogs } from '../api';
import LogViewer from '../components/LogViewer';
import RecommendationPanel from '../components/RecommendationPanel';
import PlotlyChart from '../components/PlotlyChart';
import { Play, RefreshCw, Activity } from 'lucide-react';

const Dashboard = () => {
    const [runs, setRuns] = useState([]);
    const [selectedRunId, setSelectedRunId] = useState(null);
    const [loading, setLoading] = useState(true);
    const [analyzing, setAnalyzing] = useState(false);
    const [report, setReport] = useState(null);
    const [streamMessages, setStreamMessages] = useState([]);

    useEffect(() => {
        loadRuns();
    }, []);

    const loadRuns = async () => {
        setLoading(true);
        try {
            const data = await fetchRecentLogs();
            setRuns(data);
            if (data.length > 0 && !selectedRunId) {
                setSelectedRunId(data[0].run_id);
            }
        } catch (e) {
            console.error(e);
        } finally {
            setLoading(false);
        }
    };

    const selectedRun = runs.find(r => r.run_id === selectedRunId);

    const handleAnalyze = () => {
        if (!selectedRunId) return;
        setAnalyzing(true);
        setReport(null);
        setStreamMessages([]);

        const eventSource = new EventSource(`/api/stream_analyze?run_id=${selectedRunId}`);

        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);

            if (data.status === 'complete') {
                if (data.report) setReport(data.report);
                setAnalyzing(false);
                eventSource.close();
            } else if (data.status === 'processing' || data.status === 'analyzing') {
                setStreamMessages(prev => [...prev, data.message]);
            } else if (data.error) {
                console.error(data.error);
                setAnalyzing(false);
                eventSource.close();
            }
        };

        eventSource.onerror = () => {
            setAnalyzing(false);
            eventSource.close();
        };
    };

    return (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Sidebar: Run List */}
            <div className="lg:col-span-1 space-y-4">
                <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
                    <div className="flex items-center justify-between mb-4">
                        <h2 className="font-semibold text-slate-800">Recent Job Runs</h2>
                        <button onClick={loadRuns} className="p-1 hover:bg-slate-100 rounded">
                            <RefreshCw className={`w-4 h-4 text-slate-500 ${loading ? 'animate-spin' : ''}`} />
                        </button>
                    </div>
                    <div className="space-y-2">
                        {runs.map(run => (
                            <div
                                key={run.run_id}
                                onClick={() => setSelectedRunId(run.run_id)}
                                className={`p-3 rounded-md cursor-pointer border transition-colors ${selectedRunId === run.run_id
                                        ? 'bg-blue-50 border-blue-200 ring-1 ring-blue-200'
                                        : 'bg-white border-slate-200 hover:border-blue-200'
                                    }`}
                            >
                                <div className="flex justify-between items-start">
                                    <span className="font-medium text-slate-700">{run.job_name}</span>
                                    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${run.status === 'SUCCESS' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                                        }`}>
                                        {run.status}
                                    </span>
                                </div>
                                <div className="text-xs text-slate-500 mt-1">
                                    ID: {run.run_id} • {new Date(run.start_time).toLocaleString()}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Streaming Console */}
                {analyzing && (
                    <div className="bg-slate-900 rounded-lg p-4 text-xs font-mono text-green-400 h-48 overflow-y-auto shadow-inner">
                        <div className="flex items-center gap-2 mb-2 text-slate-400 border-b border-slate-700 pb-2">
                            <Activity className="w-3 h-3 animate-pulse" />
                            <span>Analysis Stream</span>
                        </div>
                        {streamMessages.map((msg, i) => (
                            <div key={i} className="mb-1 opacity-90">> {msg}</div>
                        ))}
                        <div className="animate-pulse">_</div>
                    </div>
                )}
            </div>

            {/* Main Content */}
            <div className="lg:col-span-2 space-y-6">
                {selectedRun && (
                    <>
                        <div className="flex items-center justify-between">
                            <div>
                                <h2 className="text-2xl font-bold text-slate-800">{selectedRun.job_name}</h2>
                                <p className="text-slate-500">Run ID: {selectedRun.run_id}</p>
                            </div>
                            <button
                                onClick={handleAnalyze}
                                disabled={analyzing}
                                className="flex items-center gap-2 px-6 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 shadow-sm transition-all active:scale-95"
                            >
                                {analyzing ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                                {analyzing ? 'Analyzing...' : 'Run AI Analysis'}
                            </button>
                        </div>

                        <LogViewer logs={selectedRun.logs} />

                        {report && (
                            <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
                                <div className="bg-white p-4 rounded-lg border border-slate-200 shadow-sm h-[400px]">
                                    <h3 className="font-semibold text-slate-700 mb-4">Error Distribution</h3>
                                    <PlotlyChart data={report.plotly_data.data} layout={report.plotly_data.layout} />
                                </div>
                                <RecommendationPanel report={report} />
                            </div>
                        )}
                    </>
                )}
            </div>
        </div>
    );
};

export default Dashboard;
