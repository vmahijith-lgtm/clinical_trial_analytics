"""
Analytics Page
Advanced analytics and operational intelligence - Memory Optimized
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import sys
import gc

sys.path.append(str(Path(__file__).parent.parent))

from utils.helpers import format_number
from utils.memory_manager import sample_for_display, sample_for_visualization, memory_manager
from utils.database import AnalyticsDatabase
from utils.dataset_analyzer import dataset_analyzer

st.set_page_config(page_title="Analytics", page_icon="📈", layout="wide")

# Custom CSS styling - Enhanced
st.markdown("""
<style>
    .page-container {
        background: #f5f7fa;
    }
    
    .page-header {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        color: white;
        padding: 30px 35px;
        border-radius: 15px;
        margin-bottom: 25px;
        box-shadow: 0 8px 24px rgba(245, 87, 108, 0.3);
    }
    
    .page-header h1 {
        margin: 0;
        font-size: 2.2em;
        font-weight: bold;
    }
    
    .analysis-container {
        background: white;
        padding: 25px;
        border-radius: 12px;
        margin: 15px 0;
        border-left: 5px solid #667eea;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }
    
    .analysis-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 25px 30px;
        border-radius: 12px;
        margin-bottom: 20px;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
    }
    
    .analysis-header h2 {
        margin: 0;
        font-size: 1.5em;
        color: white;
    }
    
    .stat-box {
        text-align: center;
        padding: 20px;
        background: linear-gradient(135deg, #f5f7fa 0%, #eff2f7 100%);
        border-radius: 10px;
        border: 1px solid #e8eef8;
        margin: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
        transition: all 0.3s;
    }
    
    .stat-box:hover {
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.15);
    }
    
    .stat-value { font-size: 2.2em; font-weight: bold; color: #667eea; margin: 8px 0; }
    .stat-label { font-size: 0.95em; color: #666; margin-top: 8px; font-weight: 500; }
    
    .chart-container {
        background: white;
        padding: 20px;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        margin: 15px 0;
    }
    
    h1 { color: white; margin: 0; font-weight: bold; }
    h2 { color: #2c3e50; border-bottom: 3px solid #667eea; padding-bottom: 12px; margin-top: 25px; font-weight: bold; }
    h3 { color: #34495e; margin-top: 15px; font-weight: bold; }
    
    .divider {
        border: none;
        border-top: 2px solid #e0e0e0;
        margin: 20px 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize database
db = AnalyticsDatabase()

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
        <h1>📈 Analytics & Insights</h1>
    </div>
    """, unsafe_allow_html=True)

with col2:
    if st.button("🔄 Reload", help="Refresh data from database", key="reload_analytics"):
        import time
        st.session_state.last_reload = time.time()
        st.rerun()

all_datasets_db = db.get_all_datasets()

if not all_datasets_db:
    st.warning("⚠️ No datasets found in database. Please load data from the Home page first!")
    st.stop()

# Memory check and cleanup
if memory_manager.should_cleanup():
    memory_manager.auto_cleanup()

# Get list of available datasets from database
all_datasets = []
for dataset in all_datasets_db:
    all_datasets.append({
        'id': dataset['id'],
        'name': dataset['name'],
        'display_name': f"{dataset['name'].replace('_', ' ')}",
        'rows': dataset['row_count'],
        'quality_score': dataset['quality_score']
    })

if not all_datasets:
    st.warning("No datasets found in database")
    st.stop()

# Dataset selection and search section
st.markdown("---")
st.markdown("### 🔍 Select Dataset")

col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    # Search box for filtering datasets
    search_term = st.text_input(
        "🔎 Search datasets by name",
        placeholder="Type to search...",
        key="dataset_search"
    )
    
    # Filter datasets based on search
    if search_term:
        filtered_datasets = [d for d in all_datasets if search_term.lower() in d['display_name'].lower()]
    else:
        filtered_datasets = all_datasets
    
    if not filtered_datasets:
        st.warning("No datasets matching your search")
        st.stop()
    
    # Dataset selector
    dataset_options = [d['display_name'] for d in filtered_datasets]
    selected_name = st.selectbox(
        "Select dataset:",
        options=dataset_options,
        key="dataset_selector"
    )
    
    # Find selected dataset
    selected_dataset = next(d for d in filtered_datasets if d['display_name'] == selected_name)

with col2:
    st.markdown("**Dataset Info**")
    st.metric("Rows", format_number(selected_dataset['rows'], 0))

with col3:
    st.markdown("**Analysis Type**")
    analysis_type = st.selectbox(
        "Select analysis:",
        ["📋 Overview", "📈 Statistical Summary", "📊 Distribution Analysis"],
        key="analysis_type_selector",
        label_visibility="collapsed"
    )

st.markdown("---")

# Load selected dataset from database
try:
    df = db.load_dataset_data(selected_dataset['id'])
    if df is None or df.empty:
        # Check if data was ever saved to database
        has_data = db.has_dataset_data(selected_dataset['id'])
        
        st.error(f"""
        ❌ **Dataset data not found**
        
        Dataset ID: {selected_dataset['id']} | Data in DB: {"Yes" if has_data else "No"}
        
        The dataset metadata exists but the data hasn't been stored in the database yet.
        
        **What to do:**
        1. Go to **Home** page
        2. Click **"Load and Process All Data"** button
        3. Wait for processing to complete
        4. Return to this page and refresh
        
        **If this persists:**
        - Check the terminal/logs for error messages
        - Click **"Database Management"** → **"Clear Cache & Database"** to reset
        - Then process the Excel files again
        """)
        st.stop()
except Exception as e:
    st.error(f"""
    ❌ Failed to load dataset: {str(e)}
    
    **Solution:** Go to Home page and click "Load and Process All Data" to process the data
    """)
    st.stop()

# Analyze dataset name for insights
dataset_insights = dataset_analyzer.analyze_dataset_name(selected_dataset['name'])

# Display dataset header with insights
st.markdown(f"""
<div class="analysis-container">
    <h3 style="margin: 0; color: #667eea;">📑 {selected_dataset['display_name']}</h3>
</div>
""", unsafe_allow_html=True)

# Display dataset insights
st.markdown("#### 🔍 Dataset Insights")
insights_cols = st.columns([1, 2])

with insights_cols[0]:
    st.markdown(f"""
    <div style="background: {dataset_insights['category_color']}; color: white; padding: 15px; border-radius: 8px; text-align: center;">
        <div style="font-size: 2em;">{dataset_insights['category_icon']}</div>
        <div style="font-weight: bold; margin: 5px 0;">{dataset_insights['category'].title()}</div>
        <div style="font-size: 0.9em; opacity: 0.9;">{dataset_insights['subtype']}</div>
    </div>
    """, unsafe_allow_html=True)

with insights_cols[1]:
    st.markdown("**Key Insights:**")
    for insight in dataset_insights['key_insights']:
        st.markdown(f"• {insight}")

    # Additional metadata
    metadata_items = []
    if dataset_insights['version']:
        metadata_items.append(f"🔢 Version: {dataset_insights['version']}")
    if dataset_insights['date_info']:
        metadata_items.append(f"📅 Date: {dataset_insights['date_info']}")
    if dataset_insights['is_updated']:
        metadata_items.append("✅ Recently Updated")

    if metadata_items:
        st.markdown("**Metadata:**")
        for item in metadata_items:
            st.markdown(f"• {item}")

st.divider()

# Display basic info with enhanced styling
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown("""
    <div class="stat-box">
        <div class="stat-value">""" + format_number(len(df), 0) + """</div>
        <div class="stat-label">📊 Total Rows</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown("""
    <div class="stat-box">
        <div class="stat-value">""" + str(len(df.columns)) + """</div>
        <div class="stat-label">📋 Total Columns</div>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown("""
    <div class="stat-box">
        <div class="stat-value">""" + f"{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB" + """</div>
        <div class="stat-label">💾 Memory Size</div>
    </div>
    """, unsafe_allow_html=True)
with col4:
    st.markdown("""
    <div class="stat-box">
        <div class="stat-value">""" + str(len(df.dtypes.unique())) + """</div>
        <div class="stat-label">🏷️ Data Types</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# Analysis based on selection
if "Overview" in analysis_type:
    st.markdown("""
    <div class="analysis-header">
        <h2>📊 Dataset Overview</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Use sampled data for display
    df_sample = sample_for_display(df, max_rows=1000)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📋 First 100 Rows")
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        try:
            df_to_display = df_sample.head(100).copy()
            for col in df_to_display.columns:
                if not pd.api.types.is_numeric_dtype(df_to_display[col]) and not pd.api.types.is_bool_dtype(df_to_display[col]):
                    df_to_display[col] = df_to_display[col].astype(str)
            st.dataframe(df_to_display, use_container_width=True, height=400)
        except Exception as e:
            st.warning(f"Display format: {e}")
            st.dataframe(df_sample.head(100).astype(str), use_container_width=True, height=400)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("#### 🏷️ Column Information")
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        col_info = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes.astype(str),
            'Non-Null': df.count(),
            'Null': df.isnull().sum(),
            'Unique': df.nunique()
        })
        st.dataframe(col_info, use_container_width=True, height=400)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Cleanup
    del df_sample
    gc.collect()

elif "Statistical" in analysis_type:
    st.markdown("""
    <div class="analysis-header">
        <h2>📈 Statistical Summary</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Get numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    if numeric_cols:
        # Use sampled data for statistics
        df_sample = sample_for_display(df, max_rows=10000)
        
        st.markdown("#### 📊 Numeric Columns Summary")
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        stats_df = df_sample[numeric_cols].describe().T
        st.dataframe(stats_df, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("#### 🔍 Detailed Column Analysis")
        # Select columns for detailed view
        selected_cols = st.multiselect(
            "Select columns for visualization",
            options=numeric_cols,
            default=numeric_cols[:2] if len(numeric_cols) >= 2 else numeric_cols
        )
        
        if selected_cols:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📊 Histogram - Distribution**")
                col_to_plot = st.selectbox("Select column for histogram", selected_cols, key="dist_col")
                
                fig = px.histogram(
                    df_sample,
                    x=col_to_plot,
                    nbins=50,
                    title=f"Distribution of {col_to_plot}",
                    marginal="box",
                    color_discrete_sequence=['#667eea'],
                    labels={col_to_plot: col_to_plot, 'count': 'Frequency'}
                )
                fig.update_layout(
                    hovermode='x unified',
                    plot_bgcolor='rgba(240, 242, 246, 0.5)',
                    paper_bgcolor='white',
                    font=dict(size=11),
                    title_font_size=14,
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.markdown("**📈 Box Plot - Quartiles & Outliers**")
                fig = px.box(
                    df_sample[selected_cols],
                    title="Box Plot of Selected Columns",
                    color_discrete_sequence=['#764ba2'],
                    points="outliers"
                )
                fig.update_layout(
                    hovermode='closest',
                    plot_bgcolor='rgba(240, 242, 246, 0.5)',
                    paper_bgcolor='white',
                    font=dict(size=11),
                    title_font_size=14,
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Summary statistics for selected columns
            st.markdown("#### 📌 Summary Statistics")
            summary_stats = pd.DataFrame({
                'Column': selected_cols,
                'Mean': [df_sample[col].mean() for col in selected_cols],
                'Median': [df_sample[col].median() for col in selected_cols],
                'Std Dev': [df_sample[col].std() for col in selected_cols],
                'Min': [df_sample[col].min() for col in selected_cols],
                'Max': [df_sample[col].max() for col in selected_cols],
            })
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.dataframe(summary_stats, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Cleanup
        del df_sample
        gc.collect()
    else:
        st.info("ℹ️ No numeric columns found in this dataset")

elif "Distribution" in analysis_type:
    st.markdown("""
    <div class="analysis-header">
        <h2>📊 Advanced Distribution Analysis</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Get numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    
    if numeric_cols:
        # Use sampled data for visualization
        viz_sample = sample_for_visualization(df, max_points=2000)
        
        col1, col2 = st.columns([1.2, 1.8])
        
        with col1:
            st.markdown("### ⚙️ Configuration")
            selected_col = st.selectbox("Select column", numeric_cols, key="dist_analysis_col")
            
            plot_type = st.radio(
                "Visualization type",
                ["📊 Histogram", "📈 Box Plot", "🎻 Violin Plot", "📉 KDE Plot"]
            )
        
        with col2:
            st.markdown("### 📊 Visualization")
            
            if "Histogram" in plot_type:
                fig = px.histogram(
                    viz_sample,
                    x=selected_col,
                    nbins=50,
                    title=f"Distribution of {selected_col}",
                    marginal="box",
                    color_discrete_sequence=['#667eea'],
                    labels={selected_col: selected_col, 'count': 'Frequency'}
                )
            elif "Box" in plot_type:
                fig = px.box(
                    viz_sample,
                    y=selected_col,
                    title=f"Box Plot: {selected_col}",
                    color_discrete_sequence=['#764ba2'],
                    points="all"
                )
            elif "Violin" in plot_type:
                fig = px.violin(
                    viz_sample,
                    y=selected_col,
                    title=f"Violin Plot: {selected_col}",
                    box=True,
                    color_discrete_sequence=['#11998e'],
                    points="all"
                )
            else:  # KDE Plot
                fig = px.histogram(
                    viz_sample,
                    x=selected_col,
                    nbins=50,
                    title=f"Distribution with KDE: {selected_col}",
                    color_discrete_sequence=['#38ef7d'],
                    marginal="violin"
                )
            
            fig.update_layout(
                hovermode='x unified',
                plot_bgcolor='rgba(240, 242, 246, 0.5)',
                paper_bgcolor='white',
                font=dict(size=11),
                title_font_size=14,
                height=550,
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed statistics
        st.markdown("---")
        st.markdown("#### 📈 Detailed Statistics for " + selected_col)
        
        col_stats = {
            'Metric': ['Count', 'Mean', 'Median', 'Std Dev', 'Min', 'Q1', 'Q3', 'Max', 'IQR', 'Skewness'],
            'Value': [
                len(viz_sample[selected_col].dropna()),
                f"{viz_sample[selected_col].mean():.4f}",
                f"{viz_sample[selected_col].median():.4f}",
                f"{viz_sample[selected_col].std():.4f}",
                f"{viz_sample[selected_col].min():.4f}",
                f"{viz_sample[selected_col].quantile(0.25):.4f}",
                f"{viz_sample[selected_col].quantile(0.75):.4f}",
                f"{viz_sample[selected_col].max():.4f}",
                f"{viz_sample[selected_col].quantile(0.75) - viz_sample[selected_col].quantile(0.25):.4f}",
                f"{viz_sample[selected_col].skew():.4f}",
            ]
        }
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(col_stats), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Cleanup
        del viz_sample
        gc.collect()
    else:
        st.info("ℹ️ No numeric columns found for distribution analysis")

st.divider()

# Footer
st.markdown("""
<div style='text-align: center; color: gray; padding: 20px;'>
All analysis performed on sampled data for memory efficiency
</div>
""", unsafe_allow_html=True)
