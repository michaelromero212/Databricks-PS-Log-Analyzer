import Dashboard from './pages/Dashboard'

function App() {
    return (
        <div className="min-h-screen bg-slate-50 text-slate-900 font-sans">
            <nav className="bg-white border-b border-slate-200 px-6 py-4 flex items-center justify-between">
                <div className="flex items-center gap-3">
                    <div className="w-8 h-8 bg-orange-600 rounded flex items-center justify-center text-white font-bold">D</div>
                    <h1 className="text-xl font-semibold text-slate-800">Databricks Log Analyzer</h1>
                </div>
                <div className="text-sm text-slate-500">PS Professional Services</div>
            </nav>
            <main className="p-6">
                <Dashboard />
            </main>
        </div>
    )
}

export default App
