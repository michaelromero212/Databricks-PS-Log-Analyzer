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
    /* ========================================
       WCAG AA COMPLIANT COLOR SYSTEM
       All text colors meet 4.5:1 contrast ratio
       ======================================== */
    
    :root {
        /* Primary Colors - Accessible */
        --primary-600: #5B21B6;      /* WCAG AA on white */
        --primary-700: #4C1D95;      /* WCAG AAA on white */
        --primary-50: #FAF5FF;       /* Light backgrounds */
        
        /* Neutral Grays - High Contrast */
        --gray-900: #0F172A;         /* Headings - 15.7:1 */
        --gray-800: #1E293B;         /* Body text - 12.6:1 */
        --gray-700: #334155;         /* Secondary text - 9.1:1 */
        --gray-600: #475569;         /* Muted text - 6.7:1 */
        --gray-200: #E2E8F0;         /* Borders */
        --gray-100: #F1F5F9;         /* Backgrounds */
        --gray-50: #F8FAFC;          /* Light backgrounds */
        
        /* Status Colors - Accessible */
        --success-700: #15803D;      /* 4.6:1 on white */
        --success-50: #F0FDF4;
        --error-700: #B91C1C;        /* 5.1:1 on white */
        --error-50: #FEF2F2;
        --warning-800: #854D0E;      /* 6.4:1 on white */
        --warning-50: #FFFBEB;
        
        /* Sidebar Dark */
        --sidebar-bg: #1E293B;
        --sidebar-text: #F8FAFC;     /* 13.3:1 on sidebar-bg */
    }
    
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    
    /* ========================================
       GLOBAL LAYOUT - RESPONSIVE FOUNDATION
       ======================================== */
    
    /* Light background - clean and professional */
    .stApp {
        background: #F8FAFC !important;  /* Light gray background */
        min-height: 100vh !important;
    }
    
    /* Main Content - Solid white container */
    .main .block-container {
        background: #ffffff !important;
        border-radius: clamp(12px, 2vw, 24px) !important;
        padding: clamp(1rem, 3vw, 2.5rem) !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08) !important;
        border: 1px solid #E2E8F0 !important;
        margin: clamp(0.5rem, 2vw, 2rem) !important;
        max-width: 100% !important;
    }
    
    /* ========================================
       SIDEBAR - ACCESSIBLE DARK THEME
       ======================================== */
    
    [data-testid="stSidebar"] {
        background: var(--sidebar-bg) !important;
        border-right: 1px solid var(--gray-700) !important;
    }
    
    /* REMOVED gradient text - it fails WCAG */
    /* Solid white text for maximum contrast (13.3:1) */
    [data-testid="stSidebar"] h1 {
        color: var(--sidebar-text) !important;
        font-weight: 800 !important;
        font-size: clamp(1.3rem, 3vw, 1.8rem) !important;
        margin-bottom: 0.5rem !important;
    }
    
    [data-testid="stSidebar"] * {
        color: var(--sidebar-text) !important;
    }
    
    [data-testid="stSidebar"] .stCaption {
        color: #CBD5E1 !important; /* 8.4:1 contrast - WCAG AAA */
        font-size: 0.85rem !important;
    }
    
    /* ========================================
       TYPOGRAPHY - ACCESSIBLE HIERARCHY
       No gradient text, all solid colors
       ======================================== */
    
    h1, h2, h3, h4, h5, h6, p, li, div, span, label {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }
    
    /* REMOVED gradient text effect - replaced with solid color */
    h1 {
        color: var(--gray-900) !important;  /* 15.7:1 contrast */
        font-weight: 800 !important;
        font-size: clamp(1.75rem, 4vw, 2.5rem) !important;
        margin-bottom: 0.5rem !important;
        letter-spacing: -0.02em !important;
    }
    
    h2 {
        color: var(--gray-800) !important;  /* 12.6:1 contrast */
        font-weight: 700 !important;
        font-size: clamp(1.3rem, 3vw, 1.8rem) !important;
        margin-top: 1.5rem !important;
        margin-bottom: 1rem !important;
    }
    
    h3 {
        color: var(--gray-700) !important;  /* 9.1:1 contrast */
        font-weight: 600 !important;
        font-size: clamp(1.1rem, 2.5vw, 1.3rem) !important;
        margin-bottom: 0.75rem !important;
    }
    
    p, li, div, span {
        color: var(--gray-800) !important;
    }
    
    /* ========================================
       METRIC CARDS - RESPONSIVE & ACCESSIBLE
       ======================================== */
    
    div[data-testid="stMetric"] {
        background: #ffffff !important;
        border: 2px solid var(--gray-200) !important;
        border-radius: clamp(12px, 2vw, 20px) !important;
        padding: clamp(16px, 3vw, 24px) !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05) !important;
        transition: all 0.2s ease !important;
    }
    
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 16px rgba(91, 33, 182, 0.15) !important;
        border-color: var(--primary-600) !important;
    }
    
    div[data-testid="stMetricLabel"] {
        color: var(--gray-600) !important;  /* 6.7:1 contrast - WCAG AA */
        font-size: clamp(0.75rem, 1.5vw, 0.875rem) !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
    }
    
    div[data-testid="stMetricValue"] {
        color: var(--gray-900) !important;  /* 15.7:1 contrast - WCAG AAA */
        font-size: clamp(1.75rem, 4vw, 2.5rem) !important;
        font-weight: 800 !important;
    }
    
    /* ========================================
       BUTTONS - ACCESSIBLE INTERACTIVE
       ======================================== */
    
    div.stButton > button {
        background: var(--primary-600) !important;
        color: #ffffff !important;  /* 7.1:1 contrast */
        border: none !important;
        border-radius: 10px !important;
        padding: clamp(0.6rem, 2vw, 0.75rem) clamp(1.2rem, 3vw, 1.5rem) !important;
        font-weight: 600 !important;
        font-size: clamp(0.875rem, 1.5vw, 0.95rem) !important;
        transition: all 0.2s ease !important;
    }
    
    div.stButton > button:hover {
        background: var(--primary-700) !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(91, 33, 182, 0.3) !important;
    }
    
    /* ========================================
       DATAFRAMES - RESPONSIVE TABLES
       ======================================== */
    
    div[data-testid="stDataFrame"] {
        border: 1px solid var(--gray-200) !important;
        border-radius: 12px !important;
        overflow-x: auto !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04) !important;
    }
    
    /* ========================================
       FORM CONTROLS - ACCESSIBLE INPUTS
       ======================================== */
    
    div[data-baseweb="select"] > div {
        background-color: #ffffff !important;
        color: var(--gray-900) !important;
        border: 2px solid var(--gray-200) !important;
        border-radius: 10px !important;
        transition: border-color 0.2s ease !important;
    }
    
    div[data-baseweb="select"] > div:hover {
        border-color: var(--primary-600) !important;
    }
    
    div[data-baseweb="popover"] {
        background-color: #ffffff !important;
        border: 1px solid var(--gray-200) !important;
        border-radius: 12px !important;
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.12) !important;
    }
    
    ul[data-baseweb="menu"] {
        background-color: #ffffff !important;
        padding: 6px !important;
    }
    
    li[data-baseweb="option"] {
        color: var(--gray-800) !important;
        border-radius: 6px !important;
        padding: 10px 14px !important;
        margin: 2px 0 !important;
        transition: background-color 0.15s ease !important;
    }
    
    li[data-baseweb="option"]:hover {
        background-color: var(--gray-100) !important;
    }
    
    li[data-baseweb="option"][aria-selected="true"] {
        background-color: var(--primary-50) !important;
        color: var(--primary-700) !important;  /* 8.9:1 on primary-50 */
        font-weight: 600 !important;
    }
    
    /* ========================================
       CODE BLOCKS - READABLE DARK THEME
       ======================================== */
    
    .stCode {
        background: var(--gray-900) !important;
        border: 1px solid var(--gray-700) !important;
        border-radius: 12px !important;
        padding: clamp(12px, 2vw, 20px) !important;
    }
    
    .stCode code {
        color: #F8FAFC !important;  /* 13.8:1 on gray-900 */
        font-family: 'Monaco', 'Courier New', monospace !important;
        font-size: clamp(0.8rem, 1.5vw, 0.9rem) !important;
    }
    
    /* ========================================
       CARDS & BADGES - ACCESSIBLE ALERTS
       ======================================== */
    
    .finding-card {
        background: #ffffff !important;
        border: 1px solid var(--gray-200) !important;
        border-radius: 12px !important;
        padding: clamp(16px, 3vw, 24px) !important;
        margin-bottom: 16px !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04) !important;
    }
    
    .badge-high {
        background: var(--error-50) !important;
        color: var(--error-700) !important;  /* 5.1:1 - WCAG AA */
        padding: 4px 12px !important;
        border-radius: 6px !important;
        font-size: 0.75rem !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        border: 1px solid #FEE2E2 !important;
    }
    
    .badge-medium {
        background: var(--warning-50) !important;
        color: var(--warning-800) !important;  /* 6.4:1 - WCAG AA */
        padding: 4px 12px !important;
        border-radius: 6px !important;
        font-size: 0.75rem !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        border: 1px solid #FEF3C7 !important;
    }
    
    /* ========================================
       SIDEBAR NAVIGATION - ACCESSIBLE
       ======================================== */
    
    [data-testid="stSidebar"] .stRadio > label {
        color: var(--sidebar-text) !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stSidebar"] .stRadio label {
        background-color: rgba(255, 255, 255, 0.08) !important;
        padding: 10px 14px !important;
        border-radius: 8px !important;
        transition: background-color 0.2s ease !important;
        border: 1px solid rgba(255, 255, 255, 0.12) !important;
    }
    
    [data-testid="stSidebar"] .stRadio label:hover {
        background-color: rgba(255, 255, 255, 0.15) !important;
    }
    
    /* ========================================
       EXPANDERS - ACCESSIBLE ACCORDION
       ======================================== */
    
    .streamlit-expanderHeader {
        color: var(--gray-800) !important;
        background: var(--gray-50) !important;
        border-radius: 8px !important;
        padding: 14px !important;
        font-weight: 600 !important;
        border: 1px solid var(--gray-200) !important;
    }
    
    .streamlit-expanderHeader:hover {
        background: var(--gray-100) !important;
        border-color: var(--primary-600) !important;
    }
    
    /* ========================================
       ALERT MESSAGES - WCAG COMPLIANT
       ======================================== */
    
    .stSuccess {
        background: var(--success-50) !important;
        color: var(--success-700) !important;  /* 4.6:1 - WCAG AA */
        border-radius: 10px !important;
        border: 1px solid #86EFAC !important;
    }
    
    .stInfo {
        background: #EFF6FF !important;
        color: #1E40AF !important;  /* 8.6:1 - WCAG AAA */
        border-radius: 10px !important;
        border: 1px solid #BFDBFE !important;
    }
    
    .stWarning {
        background: var(--warning-50) !important;
        color: var(--warning-800) !important;  /* 6.4:1 - WCAG AA */
        border-radius: 10px !important;
        border: 1px solid #FDE68A !important;
    }
    
    /* ========================================
       RESPONSIVE BREAKPOINTS
       ======================================== */
    
    /* Mobile: 320px - 767px */
    @media (max-width: 767px) {
        .main .block-container {
            padding: 1rem !important;
            margin: 0.5rem !important;
            border-radius: 12px !important;
        }
        
        /* Stack columns on mobile */
        [data-testid="column"] {
            width: 100% !important;
            min-width: 100% !important;
        }
        
        /* Smaller metrics on mobile */
        div[data-testid="stMetric"] {
            padding: 12px !important;
        }
        
        div[data-testid="stMetricValue"] {
            font-size: 1.5rem !important;
        }
        
        /* Responsive charts - handled in Python */
    }
    
    /* Tablet: 768px - 1023px */
    @media (min-width: 768px) and (max-width: 1023px) {
        .main .block-container {
            padding: 1.5rem !important;
            margin: 1rem !important;
        }
        
        h1 {
            font-size: 2rem !important;
        }
    }
    
    /* Desktop: 1024px+ */
    @media (min-width: 1024px) {
        .main .block-container {
            max-width: 1400px !important;
            margin-left: auto !important;
            margin-right: auto !important;
        }
    }
    
    /* Large Desktop: 1440px+ */
    @media (min-width: 1440px) {
        .main .block-container {
            max-width: 1600px !important;
        }
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

# Custom Header
st.markdown("""
<div style='text-align: center; padding: 1rem 0 2rem 0;'>
    <div style='font-size: 0.9rem; color: #5B21B6; font-weight: 600; letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 0.5rem;'>
        Databricks Professional Services
    </div>
</div>
""", unsafe_allow_html=True)

if mode == "Overview":
    st.title("Executive Dashboard")
    st.markdown("<p style='font-size: 1.1rem; color: #475569; margin-bottom: 2rem;'>Comprehensive insights into job performance, optimization opportunities, and system health metrics.</p>", unsafe_allow_html=True)
    
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
        
        st.markdown("### 📊 Performance Trends")
        
        # Responsive chart height based on screen size
        # Streamlit doesn't have direct viewport access, but we use container_width
        # and set reasonable heights for better mobile experience
        chart_height_timeline = 400  # Desktop
        chart_height_pie = 400
        
        # Charts Row - will stack on mobile due to CSS
        c1, c2 = st.columns([2, 1])
        
        with c1:
            # Timeline Chart - Responsive
            fig_timeline = px.scatter(
                run_df, 
                x='start_time_dt', 
                y='run_name', 
                color='status',
                color_discrete_map={'SUCCESS': '#10b981', 'FAILED': '#ef4444', 'RUNNING': '#5B21B6'},
                title="Job Execution Timeline",
                height=chart_height_timeline,
                template="plotly_white"
            )
            fig_timeline.update_layout(
                xaxis=dict(
                    showgrid=True, 
                    gridcolor='#F1F5F9', 
                    title="<b>Execution Time</b>",
                    title_font=dict(size=15, color='#0F172A')  # Larger, darker
                ),
                yaxis=dict(
                    showgrid=True, 
                    gridcolor='#F1F5F9', 
                    title="<b>Job Name</b>",
                    title_font=dict(size=15, color='#0F172A')  # Larger, darker
                ),
                margin=dict(l=20, r=20, t=50, b=20),
                legend=dict(
                    orientation="h", 
                    yanchor="bottom", 
                    y=1.02, 
                    xanchor="right", 
                    x=1,
                    bgcolor="rgba(255,255,255,0.95)",
                    bordercolor="#E2E8F0",
                    borderwidth=1,
                    font=dict(size=14)  # Larger legend text
                ),
                title_font=dict(size=18, color='#0F172A', family='Inter'),  # Larger title
                plot_bgcolor='rgba(255,255,255,1)',
                paper_bgcolor='rgba(255,255,255,0)',
                # Larger font for all text
                font=dict(size=14, color='#1E293B')
            )
            fig_timeline.update_traces(marker=dict(size=10, line=dict(width=1.5, color='white')))
            st.plotly_chart(fig_timeline, use_container_width=True)
            
        with c2:
            # Pie Chart - Responsive
            status_counts = run_df['status'].value_counts()
            fig_pie = px.pie(
                values=status_counts.values, 
                names=status_counts.index, 
                color=status_counts.index,
                color_discrete_map={'SUCCESS': '#10b981', 'FAILED': '#ef4444', 'RUNNING': '#5B21B6'},
                hole=0.65,
                title="Status Distribution",
                height=chart_height_pie,
                template="plotly_white"
            )
            fig_pie.update_traces(
                textposition='outside', 
                textinfo='percent+label',
                textfont=dict(size=14, family='Inter', color='#0F172A'),  # Larger, darker
                marker=dict(line=dict(color='white', width=2))
            )
            fig_pie.update_layout(
                showlegend=False, 
                margin=dict(l=20, r=20, t=50, b=20),
                title_font=dict(size=18, color='#0F172A', family='Inter'),  # Larger title
                plot_bgcolor='rgba(255,255,255,0)',
                paper_bgcolor='rgba(255,255,255,0)',
                font=dict(size=14, color='#1E293B')  # Larger general text
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("### 📋 Recent Runs")
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
    st.title("🚀 Job Run Analysis")
    st.markdown("<p style='font-size: 1.1rem; color: #64748b; margin-bottom: 2rem;'>Deep dive into specific job runs with detailed error analysis and AI-powered insights.</p>", unsafe_allow_html=True)
    
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
    st.title("⚡ SQL Workload Optimization")
    st.markdown("<p style='font-size: 1.1rem; color: #64748b; margin-bottom: 2rem;'>Advanced static analysis to identify performance bottlenecks and optimization opportunities.</p>", unsafe_allow_html=True)
    
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
    st.title("📓 Notebook Code Scanner")
    st.markdown("<p style='font-size: 1.1rem; color: #64748b; margin-bottom: 2rem;'>Comprehensive code quality analysis for Databricks notebooks.</p>", unsafe_allow_html=True)
    
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
