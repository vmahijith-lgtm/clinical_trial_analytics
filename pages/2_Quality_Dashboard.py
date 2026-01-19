"""
Quality Dashboard Page
Monitor and analyze data quality metrics - Memory Optimized
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.helpers import format_number
from utils.memory_manager import memory_manager
from utils.database import AnalyticsDatabase
from utils.dataset_analyzer import DatasetNameAnalyzer

st.set_page_config(page_title="Quality Dashboard", page_icon="✅", layout="wide")

# Custom CSS styling - Enhanced
st.markdown("""
<style>
    .page-container {
        background: #f5f7fa;
    }
    
    .page-header {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 30px 35px;
        border-radius: 15px;
        margin-bottom: 25px;
        box-shadow: 0 8px 24px rgba(17, 153, 142, 0.3);
    }
    
    .page-header h1 {
        margin: 0 0 8px 0;
        font-size: 2.2em;
        font-weight: bold;
    }
    
    .page-header p {
        margin: 0;
        font-size: 1em;
        opacity: 0.95;
    }
    
    .quality-excellent { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
    .quality-good { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }
    .quality-fair { background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); }
    .quality-poor { background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%); }
    
    .quality-card {
        padding: 25px;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
        font-weight: bold;
        font-size: 18px;
        border: none;
        transition: transform 0.3s;
    }
    
    .quality-card:hover {
        transform: translateY(-3px);
    }
    
    .dashboard-section {
        background: white;
        padding: 25px;
        border-radius: 12px;
        margin: 20px 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        border-top: 4px solid #667eea;
    }
    
    .top-performer {
        background: linear-gradient(135deg, #f5f7fa 0%, #eff2f7 100%);
        padding: 15px;
        border-radius: 8px;
        margin: 8px 0;
        border-left: 4px solid #11998e;
    }
    
    .chart-container {
        background: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    h1 { color: white; margin-bottom: 8px; font-weight: bold; }
    h2 { color: #2c3e50; margin-top: 25px; padding-bottom: 12px; border-bottom: 3px solid #667eea; font-weight: bold; }
    h3 { color: #34495e; margin-top: 15px; font-weight: bold; }
    .subtitle { color: #666; font-size: 14px; font-weight: 500; }
</style>
""", unsafe_allow_html=True)

# Initialize session state variables
if 'file_catalog' not in st.session_state:
    st.session_state.file_catalog = {}
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = True

# Initialize database
db = AnalyticsDatabase()

# Initialize dataset analyzer
dataset_analyzer = DatasetNameAnalyzer()

# Check if data exists in database (works across sessions)
db_datasets_check = db.get_all_datasets()
if not db_datasets_check:
    st.warning("⚠️ No data in database. Please load data from the Home page first!")
    st.stop()

# Set processing_complete if not set (for session state compatibility)
if not st.session_state.get('processing_complete', False):
    st.session_state.processing_complete = True

# Reload button and refresh logic
col1, col2 = st.columns([0.9, 0.1])
with col1:
    st.markdown("""
    <div class="page-header">
        <h1>✅ Data Quality Dashboard</h1>
        <p>Monitor data quality metrics and identify issues across all datasets</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    if st.button("🔄 Reload", help="Refresh data from database", key="reload_quality"):
        import time
        st.session_state.last_reload = time.time()
        st.rerun()

all_datasets = db.get_all_datasets()

if not all_datasets:
    st.warning("⚠️ No datasets found in database. Please load data from the Home page first!")
    st.stop()

# Memory check and cleanup
if memory_manager.should_cleanup():
    memory_manager.auto_cleanup()

# Get quality metrics from database
quality_summaries = []
for dataset in all_datasets:
    metrics = db.get_quality_metrics(dataset['id'])
    if metrics:
        quality_summaries.append({
            'dataset_name': dataset['name'],
            'overall_quality_score': metrics['overall_score'],
            'completeness': metrics['completeness'],
            'rows': dataset['row_count'],
            'columns': dataset['column_count'],
            'overall_status': 'Excellent' if metrics['overall_score'] >= 0.9 else 'Good' if metrics['overall_score'] >= 0.75 else 'Fair' if metrics['overall_score'] >= 0.5 else 'Poor'
        })

# Main dashboard metrics
col1, col2, col3, col4 = st.columns(4)

avg_quality = sum(q['overall_quality_score'] for q in quality_summaries) / len(quality_summaries) if quality_summaries else 0
avg_completeness = sum(q['completeness'] for q in quality_summaries) / len(quality_summaries) if quality_summaries else 0

with col1:
    quality_status = "🟢 Excellent" if avg_quality >= 0.9 else "🟡 Good" if avg_quality >= 0.75 else "🔴 Needs Work"
    st.markdown(f"""
    <div class="quality-card quality-excellent">
        <div style="font-size: 2.5em; margin: 10px 0;">✅</div>
        <div style="font-size: 2em;">{avg_quality:.1%}</div>
        <div style="font-size: 0.9em; margin-top: 10px; opacity: 0.9;">Overall Quality</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="quality-card quality-good">
        <div style="font-size: 2.5em; margin: 10px 0;">📊</div>
        <div style="font-size: 2em;">{avg_completeness:.1%}</div>
        <div style="font-size: 0.9em; margin-top: 10px; opacity: 0.9;">Avg Completeness</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="quality-card quality-fair">
        <div style="font-size: 2.5em; margin: 10px 0;">📈</div>
        <div style="font-size: 2em;">{len(quality_summaries)}</div>
        <div style="font-size: 0.9em; margin-top: 10px; opacity: 0.9;">Datasets</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="quality-card quality-good">
        <div style="font-size: 2.5em; margin: 10px 0;">📁</div>
        <div style="font-size: 2em;">{len(st.session_state.file_catalog)}</div>
        <div style="font-size: 0.9em; margin-top: 10px; opacity: 0.9;">Files</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Dataset Insights Section
st.markdown("### 🔍 Dataset Insights")

# Create insights for all datasets
insights_data = []
for dataset in all_datasets:
    insights = dataset_analyzer.analyze_dataset_name(dataset['name'])
    insights_data.append({
        'dataset': dataset['name'],
        'category': insights['category'],
        'subtype': insights['subtype'],
        'icon': insights['category_icon'],
        'color': insights['category_color'],
        'key_insights': insights['key_insights'][:2],  # Show first 2 insights
        'version': insights['version'],
        'date_info': insights['date_info'],
        'is_updated': insights['is_updated']
    })

# Display insights in a grid
insights_cols = st.columns(3)
for i, insight in enumerate(insights_data[:9]):  # Show first 9 datasets
    col_idx = i % 3
    with insights_cols[col_idx]:
        st.markdown(f"""
        <div style="background: {insight['color']}; color: white; padding: 15px; border-radius: 8px; margin: 5px 0; text-align: center; height: 120px;">
            <div style="font-size: 1.5em;">{insight['icon']}</div>
            <div style="font-weight: bold; font-size: 0.9em; margin: 5px 0;">{insight['dataset'][:20]}...</div>
            <div style="font-size: 0.8em; opacity: 0.9;">{insight['category']}</div>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# Display Quality Summaries
st.markdown("### 📊 Dataset Quality Summary")

# Create dataframe from quality summaries
quality_df = pd.DataFrame([
    {
        'Dataset': q['dataset_name'],
        'Quality Score': q['overall_quality_score'],
        'Status': q['overall_status'],
        'Completeness': q['completeness'],
        'Rows': format_number(q['rows'], 0),
        'Columns': q['columns']
    }
    for q in quality_summaries
])

# Display as table
st.markdown('<div class="chart-container">', unsafe_allow_html=True)
st.dataframe(quality_df, use_container_width=True, hide_index=True)
st.markdown('</div>', unsafe_allow_html=True)

# Visualizations
st.markdown("---")
st.markdown("### 📈 Quality Metrics Analysis")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 📊 Quality Score Distribution")
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    # Quality score distribution
    fig = px.histogram(
        quality_df,
        x='Quality Score',
        nbins=10,
        title="How are quality scores distributed?",
        color_discrete_sequence=['#667eea'],
        labels={'Quality Score': 'Quality Score', 'count': 'Number of Datasets'}
    )
    fig.update_layout(
        hovermode='x unified',
        plot_bgcolor='rgba(240, 242, 246, 0.5)',
        paper_bgcolor='white',
        font=dict(size=11),
        title_font_size=13,
        height=400,
        showlegend=False
    )
    fig.add_vline(x=avg_quality, line_dash="dash", line_color="#f5576c", 
                  annotation_text=f"Avg: {avg_quality:.1%}", annotation_position="top right")
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown("#### ✅ Quality Status Distribution")
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    # Status distribution
    status_counts = {}
    for q in quality_summaries:
        status = q['overall_status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    # Color mapping for statuses
    color_map = {
        'Excellent': '#38ef7d',
        'Good': '#00f2fe',
        'Fair': '#fee140',
        'Poor': '#f45c43'
    }
    
    fig = px.pie(
        values=list(status_counts.values()),
        names=list(status_counts.keys()),
        title="What's the overall quality status?",
        color_discrete_map=color_map,
        hole=0.3
    )
    fig.update_layout(
        paper_bgcolor='white',
        font=dict(size=11),
        title_font_size=13,
        height=400,
        hovermode='closest'
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# Top and bottom performers
st.markdown("---")
st.markdown("### 🏆 Performance Highlights")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🏆 Top 5 Best Datasets")
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    top_5 = quality_df.nlargest(5, 'Quality Score')[['Dataset', 'Quality Score', 'Status']]
    
    for idx, row in top_5.iterrows():
        score_pct = row['Quality Score'] * 100
        score_color = "#11998e" if score_pct >= 90 else "#4facfe" if score_pct >= 75 else "#fa709a"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {score_color}15 0%, {score_color}05 100%); 
                    padding: 12px 15px; border-radius: 8px; margin: 8px 0; 
                    border-left: 4px solid {score_color};">
            <div style="font-weight: bold; color: #2c3e50; margin-bottom: 4px;">{row['Dataset']}</div>
            <div style="color: #666; font-size: 0.9em;">
                Score: <span style="color: {score_color}; font-weight: bold;">{score_pct:.1f}%</span> • 
                Status: {row['Status']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown("#### ⚠️ Needs Attention")
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    bottom_5 = quality_df.nsmallest(5, 'Quality Score')[['Dataset', 'Quality Score', 'Status']]
    
    for idx, row in bottom_5.iterrows():
        score_pct = row['Quality Score'] * 100
        score_color = "#fa709a" if score_pct < 75 else "#fee140"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, {score_color}15 0%, {score_color}05 100%); 
                    padding: 12px 15px; border-radius: 8px; margin: 8px 0; 
                    border-left: 4px solid {score_color};">
            <div style="font-weight: bold; color: #2c3e50; margin-bottom: 4px;">⚠️ {row['Dataset']}</div>
            <div style="color: #666; font-size: 0.9em;">
                Score: <span style="color: {score_color}; font-weight: bold;">{score_pct:.1f}%</span> • 
                Status: {row['Status']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)