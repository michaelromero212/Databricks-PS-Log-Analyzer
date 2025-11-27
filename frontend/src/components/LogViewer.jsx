import React, { useState } from 'react';
import { Search, Filter } from 'lucide-react';

const LogViewer = ({ logs }) => {
    const [filter, setFilter] = useState('');
    const [levelFilter, setLevelFilter] = useState('ALL');

    const filteredLogs = logs.filter(log => {
        const matchesText = log.message.toLowerCase().includes(filter.toLowerCase());
        const matchesLevel = levelFilter === 'ALL' || log.level === levelFilter;
        return matchesText && matchesLevel;
    });

    return (
        <div className="bg-white rounded-lg border border-slate-200 shadow-sm flex flex-col h-[500px]">
            <div className="p-4 border-b border-slate-200 flex items-center justify-between bg-slate-50 rounded-t-lg">
                <h3 className="font-semibold text-slate-700">Log Viewer</h3>
                <div className="flex gap-2">
                    <div className="relative">
                        <Search className="w-4 h-4 absolute left-2.5 top-2.5 text-slate-400" />
                        <input
                            type="text"
                            placeholder="Search logs..."
                            className="pl-9 pr-3 py-1.5 text-sm border border-slate-300 rounded-md focus:ring-2 focus:ring-blue-500 outline-none"
                            value={filter}
                            onChange={(e) => setFilter(e.target.value)}
                        />
                    </div>
                    <select
                        className="text-sm border border-slate-300 rounded-md px-2 py-1.5 outline-none"
                        value={levelFilter}
                        onChange={(e) => setLevelFilter(e.target.value)}
                    >
                        <option value="ALL">All Levels</option>
                        <option value="INFO">INFO</option>
                        <option value="WARN">WARN</option>
                        <option value="ERROR">ERROR</option>
                    </select>
                </div>
            </div>
            <div className="flex-1 overflow-auto p-0 font-mono text-xs">
                <table className="w-full">
                    <thead className="bg-slate-100 sticky top-0">
                        <tr>
                            <th className="text-left px-4 py-2 text-slate-500 font-medium w-32">Timestamp</th>
                            <th className="text-left px-4 py-2 text-slate-500 font-medium w-20">Level</th>
                            <th className="text-left px-4 py-2 text-slate-500 font-medium">Message</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filteredLogs.map((log, idx) => (
                            <tr key={idx} className={`border-b border-slate-100 hover:bg-slate-50 ${log.level === 'ERROR' ? 'bg-red-50 text-red-700' : 'text-slate-600'}`}>
                                <td className="px-4 py-1.5 whitespace-nowrap opacity-70">{new Date(log.timestamp).toLocaleTimeString()}</td>
                                <td className={`px-4 py-1.5 font-bold ${log.level === 'ERROR' ? 'text-red-600' : log.level === 'WARN' ? 'text-amber-600' : 'text-blue-600'}`}>
                                    {log.level}
                                </td>
                                <td className="px-4 py-1.5 break-all">{log.message}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
                {filteredLogs.length === 0 && (
                    <div className="p-8 text-center text-slate-400 italic">No logs found matching filters.</div>
                )}
            </div>
        </div>
    );
};

export default LogViewer;
