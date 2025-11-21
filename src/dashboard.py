import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from config import Config
from databricks_client import DatabricksClient
from rule_engine import RuleEngine
from ai_analysis import AIAnalyzer

# --- Page Configuration ---
st.set_page_config(
    page_title="Databricks PS Log Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS ---
st.markdown("""
<style>
    /* Force Light Theme Backgrounds */
    .stApp {
        background-color: #ffffff !important;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background-color: #f5f5f7 !important;
        border-right: 1px solid #e5e5e5 !important;
    }
    [data-testid="stSidebar"] * {
        color: #1d1d1f !important;
    }
    
    /* Typography & Text Colors */
    h1, h2, h3, h4, h5, h6, p, li, div, span {
        color: #1d1d1f !important;
        font-family: -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    
    /* Metric Cards */
    div[data-testid="stMetric"] {
        background-color: #ffffff !important;
        border: 1px solid #e5e5e5 !important;
        border-radius: 12px !important;
        padding: 16px !important;
        box-shadow: 0 2px 4px rgba(0,0,0,0.02) !important;
    }
    div[data-testid="stMetricLabel"] {
        color: #86868b !important;
        font-size: 0.9rem !important;
    }
    div[data-testid="stMetricValue"] {
        color: #1d1d1f !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }
    
    /* Buttons */
    div.stButton > button {
        background-color: #0071e3 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.6rem 1.2rem !important;
        font-weight: 500 !important;
    }
    div.stButton > button:hover {
        background-color: #0077ed !important;
        box-shadow: 0 2px 8px rgba(0,113,227,0.3) !important;
    }
    
    /* Dataframes */
    div[data-testid="stDataFrame"] {
        border: 1px solid #e5e5e5 !important;
    }
    
    /* --- DROPDOWN & INPUT FIXES --- */
    /* Input Boxes */
    div[data-baseweb="select"] > div {
        background-color: #ffffff !important;
        color: #1d1d1f !important;
        border-color: #d1d5db !important;
    }
    
    /* Dropdown Menu Container (The Popover) */
    div[data-baseweb="popover"] {
        background-color: #ffffff !important;
        border: 1px solid #e5e5e5 !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1) !important;
    }
    
    /* The Menu List */
    ul[data-baseweb="menu"] {
        background-color: #ffffff !important;
    }
    
    /* Individual Options */
    li[data-baseweb="option"] {
        background-color: #ffffff !important;
        color: #1d1d1f !important;
        display: flex !important;
        align-items: center !important;
    }
    
    /* Text inside options - Force Black */
    li[data-baseweb="option"] * {
        color: #1d1d1f !important;
    }
    
    /* Hover State for Options */
    li[data-baseweb="option"]:hover {
        background-color: #f5f5f7 !important;
    }
    li[data-baseweb="option"]:hover * {
        color: #0071e3 !important;
    }
    
    /* Selected State */
    li[data-baseweb="option"][aria-selected="true"] {
        background-color: #e5f1ff !important;
    }
    li[data-baseweb="option"][aria-selected="true"] * {
        color: #0071e3 !important;
    }
    
    /* --- CODE BLOCK FIXES --- */
    /* Force Code Blocks to Light Theme */
    .stCode {
        background-color: #f5f5f7 !important;
        border: 1px solid #e5e5e5 !important;
        border-radius: 8px !important;
    }
    /* Code Text Color */
    .stCode code {
        color: #1d1d1f !important;
        background-color: transparent !important;
    }
    /* Syntax Highlighting Overrides (Basic) */
    .stCode span {
        color: #1d1d1f !important; /* Fallback */
    }
    
    /* Custom Badges & Cards */
    .finding-card {
        background-color: #ffffff !important;
        border: 1px solid #e5e5e5 !important;
        border-radius: 10px !important;
        padding: 20px !important;
        margin-bottom: 15px !important;
    }
    .badge-high {
        background-color: #fee2e2 !important;
        color: #991b1b !important;
        padding: 4px 8px !important;
        border-radius: 4px !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        border: 1px solid #fecaca !important;
    }
    .badge-medium {
        background-color: #fef3c7 !important;
        color: #92400e !important;
        padding: 4px 8px !important;
        border-radius: 4px !important;
        font-size: 0.8rem !important;
        font-weight: 600 !important;
        border: 1px solid #fde68a !important;
    }
    
    /* Radio Buttons in Sidebar */
    .stRadio > label {
        color: #1d1d1f !important;
    }
    
    /* Expander */
    .streamlit-expanderHeader {
        color: #1d1d1f !important;
        background-color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

# --- Initialization ---
if 'components' not in st.session_state:
    st.session_state.components = {
        'client': DatabricksClient(),
        'engine': RuleEngine(),
        'ai': AIAnalyzer()
    }

client = st.session_state.components['client']
engine = st.session_state.components['engine']
ai = st.session_state.components['ai']

# --- Sidebar ---
with st.sidebar:
    st.title("🔍 Log Analyzer")
    st.caption("Databricks Professional Services")
    
    st.markdown("### Navigation")
    mode = st.radio("Go to", ["Overview", "Job Runs", "SQL Analysis", "Notebook Analysis"], label_visibility="collapsed")
    
    st.markdown("---")
    st.markdown("### Connection Status")
    if client.connected:
        st.success("Connected to Databricks")
    else:
        st.info("Offline Mode (Mock Data)")
        st.caption("Using local sample files for demonstration.")

# --- Main Content ---

if mode == "Overview":
    st.title("Executive Dashboard")
    st.markdown("High-level summary of job performance and optimization opportunities.")
    
    runs = client.list_runs(limit=20)
    run_df = pd.DataFrame(runs)
    
    if not run_df.empty:
        run_df['status'] = run_df['state'].apply(lambda x: x.get('result_state'))
        run_df['start_time_dt'] = pd.to_datetime(run_df['start_time'], unit='ms')
        
        # Top Metrics Row
        col1, col2, col3, col4 = st.columns(4)
        
        total_runs = len(run_df)
        failed_runs = len(run_df[run_df['status'] != 'SUCCESS'])
        success_rate = ((total_runs - failed_runs) / total_runs) * 100
        
        with col1:
            st.metric("Total Runs", total_runs)
        with col2:
            st.metric("Success Rate", f"{success_rate:.1f}%")
        with col3:
            st.metric("Failures", failed_runs, delta=-failed_runs if failed_runs > 0 else 0, delta_color="inverse")
        with col4:
            st.metric("Optimization Tips", "5 New", delta="High Priority", delta_color="off")
            
        st.markdown("### Performance Trends")
        
        # Charts Row
        c1, c2 = st.columns([2, 1])
        
        with c1:
            # Timeline Chart
            fig_timeline = px.scatter(
                run_df, 
                x='start_time_dt', 
                y='run_name', 
                color='status',
                color_discrete_map={'SUCCESS': '#10b981', 'FAILED': '#ef4444'},
                title="Job Execution Timeline",
                height=350,
                template="plotly_white"
            )
            fig_timeline.update_layout(
                xaxis=dict(showgrid=True, gridcolor='#f3f4f6', title="Time"),
                yaxis=dict(showgrid=True, gridcolor='#f3f4f6', title="Job Name"),
                margin=dict(l=20, r=20, t=40, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
            
        with c2:
            # Pie Chart
            status_counts = run_df['status'].value_counts()
            fig_pie = px.pie(
                values=status_counts.values, 
                names=status_counts.index, 
                color=status_counts.index,
                color_discrete_map={'SUCCESS': '#10b981', 'FAILED': '#ef4444'},
                hole=0.6,
                title="Status Distribution",
                height=350,
                template="plotly_white"
            )
            fig_pie.update_traces(textposition='outside', textinfo='percent+label')
            fig_pie.update_layout(showlegend=False, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("### Recent Runs")
        st.dataframe(
            run_df[['run_id', 'run_name', 'status', 'start_time_dt']].sort_values('start_time_dt', ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "run_id": "Run ID",
                "run_name": "Job Name",
                "start_time_dt": st.column_config.DatetimeColumn("Start Time", format="D MMM YYYY, h:mm a"),
                "status": st.column_config.TextColumn("Status")
            }
        )

elif mode == "Job Runs":
    st.title("Job Run Analysis")
    st.markdown("Deep dive into specific job runs.")
    
    runs = client.list_runs()
    run_options = {f"{r['run_name']} (ID: {r['run_id']})": r['run_id'] for r in runs}
    
    col1, col2 = st.columns([1, 3])
    with col1:
        st.markdown("#### Select Run")
        selected_run_name = st.radio("Run List", list(run_options.keys()), label_visibility="collapsed")
        selected_run_id = run_options[selected_run_name]
    
    with col2:
        st.markdown(f"### Analysis: `{selected_run_name}`")
        st.markdown("---")
        
        log_data = client.get_run_log(selected_run_id)
        if log_data:
            findings = engine.analyze_run(log_data)
            
            if not findings:
                st.success("✅ No critical issues found in this run.")
            
            for f in findings:
                with st.container():
                    # Custom Finding Card
                    severity_class = "badge-high" if f['severity'] == "high" else "badge-medium"
                    
                    st.markdown(f"""
                    <div class="finding-card">
                        <div class="finding-header">
                            <span class="badge {severity_class}">{f['severity'].upper()}</span>
                            <span class="finding-title">{f['type'].replace('_', ' ').title()}</span>
                        </div>
                        <p class="finding-message">{f['message']}</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if 'details' in f:
                        with st.expander("View Technical Details"):
                            st.code(f['details'], language="text")
                            
                        if f['severity'] == 'high':
                            st.info("🤖 **AI Insight:** " + ai.analyze_error(f['details']))

elif mode == "SQL Analysis":
    st.title("SQL Workload Optimization")
    st.markdown("Static analysis of SQL files.")
    
    sql_files = [f for f in os.listdir(Config.SAMPLE_SQL_DIR) if f.endswith(".sql")]
    
    col1, col2 = st.columns([1, 2])
    with col1:
        selected_file = st.selectbox("Select SQL File", sql_files)
    
    if selected_file:
        path = os.path.join(Config.SAMPLE_SQL_DIR, selected_file)
        with open(path, 'r') as f:
            content = f.read()
            
        c1, c2 = st.columns([1, 1])
        
        with c1:
            st.markdown("#### Source Code")
            st.code(content, language="sql")
            
        with c2:
            st.markdown("#### Findings")
            res = engine.analyze_sql_file(path)
            
            if not res['recommendations']:
                st.success("✅ Clean SQL! No common anti-patterns detected.")
            
            for r in res['recommendations']:
                severity_class = "badge-high" if r['severity'] == "high" else "badge-medium"
                st.markdown(f"""
                <div class="finding-card">
                    <div class="finding-header">
                        <span class="badge {severity_class}">{r['severity'].upper()}</span>
                        <span class="finding-title">{r['description']}</span>
                    </div>
                    <p class="finding-message">{r['recommendation']}</p>
                </div>
                """, unsafe_allow_html=True)
                
            st.markdown("---")
            if st.button("✨ Ask AI to Optimize Query"):
                with st.spinner("Analyzing..."):
                    suggestion = ai.suggest_optimization(content)
                    st.success(suggestion)

elif mode == "Notebook Analysis":
    st.title("Notebook Code Scanner")
    
    nb_files = [f for f in os.listdir(Config.SAMPLE_NOTEBOOKS_DIR) if f.endswith(".py")]
    selected_file = st.selectbox("Select Notebook", nb_files)
    
    if selected_file:
        path = os.path.join(Config.SAMPLE_NOTEBOOKS_DIR, selected_file)
        res = engine.analyze_notebook_file(path)
        
        st.markdown(f"#### Analysis Results for `{selected_file}`")
        
        if not res['findings']:
            st.success("✅ No issues found.")
            
        for f in res['findings']:
            st.warning(f"**{f['description']}**\n\n{f['recommendation']}")
