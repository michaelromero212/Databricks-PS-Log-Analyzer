import React, { useState } from 'react';
import { AlertTriangle, CheckCircle, Zap, BookOpen, Terminal } from 'lucide-react';
import { writeToNotebook } from '../api';

const RecommendationPanel = ({ report }) => {
    const [writing, setWriting] = useState(false);

    const handleCommit = async () => {
        setWriting(true);
        try {
            await writeToNotebook('/Users/shared/Analysis_Report', report.notebook_cell_text);
            alert('Successfully wrote to notebook!');
        } catch (e) {
            alert('Failed to write to notebook: ' + e.message);
        } finally {
            setWriting(false);
        }
    };

    if (!report) return null;

    return (
        <div className="space-y-6">
            {/* Summary Card */}
            <div className="bg-white p-6 rounded-lg border border-slate-200 shadow-sm">
                <h2 className="text-lg font-semibold text-slate-800 mb-2">Analysis Summary</h2>
                <p className="text-slate-600">{report.summary}</p>
                <div className="mt-4 flex gap-4">
                    <button
                        onClick={handleCommit}
                        disabled={writing}
                        className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50 transition-colors"
                    >
                        <BookOpen className="w-4 h-4" />
                        {writing ? 'Writing...' : 'Commit to Notebook'}
                    </button>
                </div>
            </div>

            {/* Clusters */}
            <div className="grid gap-6 md:grid-cols-2">
                {report.clusters.map((cluster) => (
                    <div key={cluster.cluster_id} className="bg-white rounded-lg border border-slate-200 shadow-sm overflow-hidden">
                        <div className="bg-red-50 border-b border-red-100 p-4 flex items-start gap-3">
                            <AlertTriangle className="w-5 h-5 text-red-600 mt-0.5" />
                            <div>
                                <h3 className="font-semibold text-red-900">Issue #{cluster.cluster_id}</h3>
                                <p className="text-xs text-red-700 mt-1 font-mono bg-red-100 px-2 py-1 rounded inline-block">
                                    {cluster.error_pattern.substring(0, 60)}...
                                </p>
                            </div>
                        </div>

                        <div className="p-4 space-y-4">
                            <div>
                                <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider mb-2">Root Cause</h4>
                                <p className="text-sm text-slate-600 leading-relaxed">{cluster.root_cause}</p>
                            </div>

                            <div>
                                <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider mb-2">Suggested Fixes</h4>
                                <ul className="space-y-2">
                                    {cluster.fixes.map((fix, i) => (
                                        <li key={i} className="flex items-start gap-2 text-sm text-slate-600 bg-slate-50 p-2 rounded">
                                            <CheckCircle className="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
                                            <span>{fix.description}</span>
                                        </li>
                                    ))}
                                </ul>
                            </div>

                            {cluster.tuning_params.length > 0 && (
                                <div>
                                    <h4 className="text-sm font-semibold text-slate-700 uppercase tracking-wider mb-2">Tuning Parameters</h4>
                                    <div className="space-y-2">
                                        {cluster.tuning_params.map((param, i) => (
                                            <div key={i} className="flex items-center justify-between bg-blue-50 p-2 rounded border border-blue-100">
                                                <div className="flex items-center gap-2">
                                                    <Zap className="w-4 h-4 text-blue-500" />
                                                    <span className="font-mono text-sm text-blue-900">{param.param}</span>
                                                </div>
                                                <span className="font-mono text-sm font-bold text-blue-700">{param.value}</span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default RecommendationPanel;
