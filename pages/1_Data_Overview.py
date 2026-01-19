"""
Data Overview Page
Explore and visualize loaded datasets - Memory Optimized
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from utils.helpers import format_number, calculate_completeness
from utils.memory_manager import sample_for_display, sample_for_visualization, memory_manager
from utils.database import AnalyticsDatabase
from utils.dataset_analyzer import dataset_analyzer

st.set_page_config(page_title="Data Overview", page_icon="📊", layout="wide")

# Custom CSS - Enhanced
st.markdown("""
<style>
    .page-container {
        background: #f5f7fa;
    }
    
    .page-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 25px 30px;
        border-radius: 12px;
        margin-bottom: 25px;
    }
    
    .page-header h1 {
        margin: 0;
        font-size: 2em;
        font-weight: bold;
    }
    
    .dataset-card {
        padding: 22px;
        border-radius: 12px;
        border-left: 5px solid #667eea;
        background: white;
        margin: 12px 0;
        box-shadow: 0 3px 12px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
    }
    
    .dataset-card:hover {
        box-shadow: 0 6px 18px rgba(102, 126, 234, 0.15);
        transform: translateX(3px);
    }
    
    .metric-box {
        text-align: center;
        padding: 18px;
        background: linear-gradient(135deg, #f5f7fa 0%, #eff2f7 100%);
        border-radius: 10px;
        margin: 8px;
        border: 1px solid #e8eef8;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    
    .metric-value { font-size: 28px; font-weight: bold; color: #667eea; margin: 8px 0; }
    .metric-label { font-size: 11px; color: #666; margin-top: 8px; font-weight: 500; }
    
    h1 { color: #2c3e50; margin-bottom: 15px; font-weight: bold; }
    h2 { color: #2c3e50; margin-top: 25px; padding-bottom: 12px; border-bottom: 3px solid #667eea; font-weight: bold; }
    h3 { color: #34495e; margin-top: 15px; }
    
    .dataframe-container {
        background: white;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }
    
    .chart-container {
        background: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
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
        <h1>📊 Data Overview & Exploration</h1>
    </div>
    """, unsafe_allow_html=True)

with col2:
    if st.button("🔄 Reload", help="Refresh data from database"):
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

# Build list of available datasets from database
all_datasets = []
for dataset in all_datasets_db:
    all_datasets.append({
        'id': dataset['id'],
        'name': dataset['name'],
        'file_path': dataset['file_path'],
        'sheet': dataset['sheet_name'],
        'rows': dataset['row_count'],
        'quality_score': dataset['quality_score'],
        'display_name': f"{dataset['name'].replace('_', ' ')}"
    })

if not all_datasets:
    st.warning("No datasets found in database")
    st.stop()

# Dataset selection and search section
st.markdown("---")
st.markdown("### 🔍 Select Dataset")

col1, col2 = st.columns([2, 1])

with col1:
    # Search box for filtering datasets
    search_term = st.text_input(
        "🔎 Search datasets by name",
        placeholder="Type to search...",
        key="dataset_search_overview"
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
        key="dataset_selector_overview"
    )
    
    # Find selected dataset
    selected_dataset = next(d for d in filtered_datasets if d['display_name'] == selected_name)

with col2:
    st.markdown("**Dataset Info**")
    st.metric("Rows", format_number(selected_dataset['rows'], 0))

st.markdown("---")

# Load dataset directly from database
try:
    selected_df = db.load_dataset_data(selected_dataset['id'])
    if selected_df is None or selected_df.empty:
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
<div class="dataset-card">
    <h3 style="margin: 0; color: #667eea;">📑 {selected_dataset['display_name']}</h3>
    <small>Quality Score: {selected_dataset['quality_score']:.1%}</small>
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

# Main content
tab1, tab2, tab3, tab4 = st.tabs(["📋 Data Preview", "📈 Statistics", "📊 Visualizations", "🔍 Search & Filter"])

with tab1:
    st.markdown("""
    <div class="analysis-header">
        <h2>📋 Data Preview</h2>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="stat-box">
            <div class="stat-value">""" + format_number(len(selected_df), 0) + """</div>
            <div class="stat-label">📊 Rows</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <div class="stat-box">
            <div class="stat-value">""" + str(len(selected_df.columns)) + """</div>
            <div class="stat-label">📋 Columns</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        completeness = calculate_completeness(selected_df)
        st.markdown("""
        <div class="stat-box">
            <div class="stat-value">""" + f"{completeness['overall_completeness']:.1%}" + """</div>
            <div class="stat-label">✅ Completeness</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        size_mb = selected_df.memory_usage(deep=True).sum() / 1024 / 1024
        st.markdown("""
        <div class="stat-box">
            <div class="stat-value">""" + f"{format_number(size_mb, 2)} MB" + """</div>
            <div class="stat-label">💾 Size</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("#### 📋 First 100 Rows")
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    try:
        # Create a copy for display to avoid altering the original dataframe
        df_to_display = selected_df.head(100).copy()

        # Convert non-numeric and non-boolean columns to string to prevent Arrow errors
        for col in df_to_display.columns:
            if not pd.api.types.is_numeric_dtype(df_to_display[col]) and not pd.api.types.is_bool_dtype(df_to_display[col]):
                df_to_display[col] = df_to_display[col].astype(str)
        
        st.dataframe(df_to_display, use_container_width=True, height=400)
    except Exception as e:
        # Fallback for any other unexpected errors
        st.warning(f"Could not render dataframe as is. Displaying as string. Error: {e}")
        st.dataframe(selected_df.head(100).astype(str), use_container_width=True, height=400)
    st.markdown('</div>', unsafe_allow_html=True)
    
    with st.expander("🔤 Column Information & Data Quality"):
        col_info = pd.DataFrame({
            'Column': selected_df.columns,
            'Type': selected_df.dtypes.astype(str),
            'Non-Null Count': selected_df.count(),
            'Null Count': selected_df.isnull().sum(),
            'Null %': (selected_df.isnull().sum() / len(selected_df) * 100).round(2),
            'Unique Values': selected_df.nunique()
        })
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.dataframe(col_info, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

with tab2:
    st.markdown("""
    <div class="analysis-header">
        <h2>📊 Statistical Summary</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Numeric columns summary - use sampled data for large datasets
    numeric_cols = selected_df.select_dtypes(include=['number']).columns
    
    if len(numeric_cols) > 0:
        st.markdown("#### 📈 Numeric Columns Statistics")
        
        # Use sampled data for describe to save memory
        df_sample = sample_for_display(selected_df, max_rows=10000)
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.dataframe(df_sample[numeric_cols].describe(), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Distribution plots - use heavily sampled data
        st.markdown("#### 📊 Data Distributions")
        
        col1, col2 = st.columns([1, 1.5])
        
        with col1:
            st.markdown("**Select Column**")
            selected_num_col = st.selectbox("Numeric column", numeric_cols)
            
            st.markdown("**Plot Type**")
            plot_type = st.selectbox("Visualization", ["📊 Histogram", "📈 Box Plot", "🎻 Violin Plot"])
        
        with col2:
            st.markdown("**Visualization**")
            # Use sampled data for visualization
            viz_sample = sample_for_visualization(selected_df, max_points=1000)
            
            if "Histogram" in plot_type:
                fig = px.histogram(viz_sample, x=selected_num_col, 
                                 title=f"Distribution of {selected_num_col}",
                                 marginal="box",
                                 color_discrete_sequence=['#667eea'],
                                 labels={selected_num_col: selected_num_col, 'count': 'Frequency'})
            elif "Box" in plot_type:
                fig = px.box(viz_sample, y=selected_num_col,
                            title=f"Box Plot: {selected_num_col}",
                            color_discrete_sequence=['#764ba2'],
                            points="outliers")
            else:
                fig = px.violin(viz_sample, y=selected_num_col,
                              title=f"Violin Plot: {selected_num_col}",
                              box=True,
                              color_discrete_sequence=['#11998e'],
                              points="all")
            
            fig.update_layout(
                plot_bgcolor='rgba(240, 242, 246, 0.5)',
                paper_bgcolor='white',
                font=dict(size=11),
                title_font_size=13,
                height=450,
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Categorical columns
    categorical_cols = selected_df.select_dtypes(include=['object']).columns
    
    if len(categorical_cols) > 0:
        st.markdown("---")
        st.markdown("#### 🏷️ Categorical Columns Analysis")
        
        selected_cat_col = st.selectbox("Select categorical column", categorical_cols)
        
        # Use full data for value counts (aggregated, so memory efficient)
        value_counts = selected_df[selected_cat_col].value_counts().head(15)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Top 15 Values**")
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.dataframe(value_counts, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown("**Value Distribution**")
            fig = px.bar(x=value_counts.index, y=value_counts.values,
                        title=f"Top 15 Values in {selected_cat_col}",
                        color_discrete_sequence=['#667eea'],
                        labels={'x': selected_cat_col, 'y': 'Count'})
            fig.update_layout(
                plot_bgcolor='rgba(240, 242, 246, 0.5)',
                paper_bgcolor='white',
                font=dict(size=11),
                title_font_size=13,
                height=400,
                hovermode='x'
            )
            st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.markdown("""
    <div class="analysis-header">
        <h2>📊 Advanced Visualizations</h2>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        viz_type = st.selectbox(
            "Visualization Type",
            ["📊 Correlation Heatmap", "🔵 Scatter Plot", "📈 Time Series", "❌ Missing Data Pattern"]
        )
    
    # Use sampled data for all visualizations
    viz_sample = sample_for_visualization(selected_df, max_points=2000)
    
    with col2:
        st.markdown("**Visualization**")
        
        if "Correlation" in viz_type:
            # Use sampled data for correlation
            numeric_sample = viz_sample.select_dtypes(include=['number'])
            
            if numeric_sample.shape[1] >= 2:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                corr_matrix = numeric_sample.corr()
                
                fig = go.Figure(data=go.Heatmap(
                    z=corr_matrix.values,
                    x=corr_matrix.columns,
                    y=corr_matrix.columns,
                    colorscale='RdBu',
                    zmid=0
                ))
                fig.update_layout(
                    title="Correlation Matrix Between Numeric Columns",
                    height=500,
                    font=dict(size=10)
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("ℹ️ Need at least 2 numeric columns for correlation analysis")
        
        elif "Scatter" in viz_type:
            numeric_cols = selected_df.select_dtypes(include=['number']).columns
            
            if len(numeric_cols) >= 2:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                
                col_x, col_y = st.columns(2)
                with col_x:
                    x_col = st.selectbox("X-axis", numeric_cols, key='scatter_x')
                with col_y:
                    y_col = st.selectbox("Y-axis", numeric_cols, key='scatter_y', 
                                        index=min(1, len(numeric_cols)-1))
                
                # Optional color column
                color_col = st.selectbox("Color by (optional)", 
                                        ['None'] + list(selected_df.columns))
                
                # Use sampled data for scatter plot
                if color_col == 'None':
                    fig = px.scatter(viz_sample, x=x_col, y=y_col,
                                   title=f"{x_col} vs {y_col}",
                                   color_discrete_sequence=['#667eea'])
                else:
                    fig = px.scatter(viz_sample, x=x_col, y=y_col, color=color_col,
                                   title=f"{x_col} vs {y_col} (colored by {color_col})")
                
                fig.update_layout(
                    plot_bgcolor='rgba(240, 242, 246, 0.5)',
                    paper_bgcolor='white',
                    font=dict(size=11),
                    height=450,
                    hovermode='closest'
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("ℹ️ Need at least 2 numeric columns for scatter plot")
        
        elif "Time Series" in viz_type:
            date_cols = selected_df.select_dtypes(include=['datetime64']).columns
            numeric_cols = selected_df.select_dtypes(include=['number']).columns
            
            if len(date_cols) > 0 and len(numeric_cols) > 0:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                
                col_d, col_v = st.columns(2)
                with col_d:
                    date_col = st.selectbox("Date column", date_cols)
                with col_v:
                    value_col = st.selectbox("Value column", numeric_cols)
                
                # Use sampled and sorted data
                viz_sorted = viz_sample.sort_values(date_col)
                
                fig = px.line(viz_sorted, x=date_col, y=value_col,
                             title=f"Time Series: {value_col}",
                             color_discrete_sequence=['#667eea'])
                fig.update_layout(
                    plot_bgcolor='rgba(240, 242, 246, 0.5)',
                    paper_bgcolor='white',
                    font=dict(size=11),
                    height=450,
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("ℹ️ Need date and numeric columns for time series visualization")
        
        elif "Missing Data" in viz_type:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            
            # Use full data for missing pattern (it's efficient)
            missing_data = selected_df.isnull().sum()
            missing_pct = (missing_data / len(selected_df) * 100).sort_values(ascending=False)
            
            if missing_pct.sum() > 0:
                fig = px.bar(x=missing_pct.index, y=missing_pct.values,
                            title="Missing Data Pattern by Column",
                            labels={'x': 'Column', 'y': 'Missing %'},
                            color_discrete_sequence=['#fa709a'])
                fig.update_layout(
                    plot_bgcolor='rgba(240, 242, 246, 0.5)',
                    paper_bgcolor='white',
                    font=dict(size=11),
                    height=450,
                    hovermode='x'
                )
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("ℹ️ No missing data found in this dataset!")
                st.markdown('</div>', unsafe_allow_html=True)

with tab4:
    st.markdown("""
    <div class="analysis-header">
        <h2>🔍 Search & Filter Data</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Memory check
    if memory_manager.should_cleanup():
        memory_manager.auto_cleanup()
    
    # Search functionality
    st.markdown("#### 🔎 Search Data")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        search_col = st.selectbox("Search in column", selected_df.columns)
    
    with col2:
        search_term = st.text_input("Search term (supports wildcards)")
    
    if search_term:
        mask = selected_df[search_col].astype(str).str.contains(search_term, case=False, na=False, regex=False)
        filtered_df = selected_df[mask]
        
        if len(filtered_df) > 0:
            st.markdown(f'<div style="background: #d4edda; padding: 12px; border-radius: 8px; color: #155724; margin: 10px 0;"><strong>✅ Found {len(filtered_df)} matching rows</strong></div>', unsafe_allow_html=True)
            
            # Use sampled data for display
            display_df = sample_for_display(filtered_df, max_rows=500)
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.dataframe(display_df, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background: #f8d7da; padding: 12px; border-radius: 8px; color: #721c24; margin: 10px 0;"><strong>❌ No rows found</strong></div>', unsafe_allow_html=True)
    
    # Filter functionality
    st.markdown("---")
    st.markdown("#### 🎯 Advanced Filters")
    
    with st.expander("Add Filters", expanded=False):
        filter_col = st.selectbox("Column to filter", selected_df.columns, key='filter_col')
        
        column_dtype = selected_df[filter_col].dtype

        if pd.api.types.is_numeric_dtype(column_dtype):
            col1, col2 = st.columns(2)
            with col1:
                min_val = st.number_input("Min value", value=float(selected_df[filter_col].min()))
            with col2:
                max_val = st.number_input("Max value", value=float(selected_df[filter_col].max()))
            
            filtered_df = selected_df[
                (selected_df[filter_col] >= min_val) & 
                (selected_df[filter_col] <= max_val)
            ]
            
            if len(filtered_df) > 0:
                st.markdown(f'<div style="background: #cfe2ff; padding: 12px; border-radius: 8px; color: #084298; margin: 10px 0;"><strong>📊 Showing {len(filtered_df)} rows</strong></div>', unsafe_allow_html=True)
                # Use sampled data for display
                display_df = sample_for_display(filtered_df, max_rows=500)
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.dataframe(display_df, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        elif pd.api.types.is_object_dtype(column_dtype) or pd.api.types.is_categorical_dtype(column_dtype):
            unique_values = selected_df[filter_col].dropna().unique()
            selected_values = st.multiselect("Select values", unique_values)
            
            if selected_values:
                filtered_df = selected_df[selected_df[filter_col].isin(selected_values)]
                
                # Use sampled data for display
                display_df = sample_for_display(filtered_df, max_rows=500)
                st.dataframe(display_df, use_container_width=True)
        else:
            st.info(f"Filtering for column type {column_dtype} is not supported.")
    
    # Export functionality
    st.subheader("📥 Export Data")
    
    if st.button("Download Current View as CSV"):
        csv = selected_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"{selected_name}_export.csv",
            mime="text/csv"
        )
    
    # Memory cleanup after operations
    if memory_manager.should_cleanup():
        memory_manager.force_cleanup()
