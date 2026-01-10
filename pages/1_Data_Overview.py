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
import gc

sys.path.append(str(Path(__file__).parent.parent))

from utils.helpers import format_number, calculate_completeness
from utils.memory_manager import sample_for_display, sample_for_visualization, memory_manager

st.set_page_config(page_title="Data Overview", page_icon="📊", layout="wide")

# Check if data is loaded
if not st.session_state.get('data_ingested', False):
    st.warning("⚠️ Please load data from the Home page first!")
    st.stop()

st.title("📊 Data Overview & Exploration")

# Memory check and cleanup
if memory_manager.should_cleanup():
    memory_manager.auto_cleanup()

# Sidebar: Dataset selector
with st.sidebar:
    st.header("Dataset Selection")
    
    # Select category
    categories = list(st.session_state.categorized_data.keys())
    selected_category = st.selectbox(
        "Category",
        options=categories,
        format_func=lambda x: x.replace('_', ' ').title()
    )
    
    # Select specific dataset
    datasets_in_category = st.session_state.categorized_data[selected_category]
    
    if datasets_in_category:
        dataset_options = [
            f"{file_path} - {sheet}" 
            for file_path, sheet, _ in datasets_in_category
        ]
        
        selected_idx = st.selectbox(
            "Dataset",
            options=range(len(dataset_options)),
            format_func=lambda i: dataset_options[i]
        )
        
        selected_file, selected_sheet, selected_df = datasets_in_category[selected_idx]
    else:
        st.info(f"No datasets in {selected_category}")
        st.stop()

# Main content
tab1, tab2, tab3, tab4 = st.tabs(["📋 Data Preview", "📈 Statistics", "📊 Visualizations", "🔍 Search & Filter"])

with tab1:
    st.header("Data Preview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Rows", format_number(len(selected_df), 0))
    with col2:
        st.metric("Columns", len(selected_df.columns))
    with col3:
        completeness = calculate_completeness(selected_df)
        st.metric("Completeness", f"{completeness['overall_completeness']:.1%}")
    with col4:
        size_mb = selected_df.memory_usage(deep=True).sum() / 1024 / 1024
        st.metric("Size (MB)", format_number(size_mb, 2))
    
    st.subheader("First 100 Rows")
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
    
    with st.expander("🔤 Column Information"):
        col_info = pd.DataFrame({
            'Column': selected_df.columns,
            'Type': selected_df.dtypes.astype(str),
            'Non-Null Count': selected_df.count(),
            'Null Count': selected_df.isnull().sum(),
            'Unique Values': selected_df.nunique()
        })
        st.dataframe(col_info, use_container_width=True)

with tab2:
    st.header("Statistical Summary")
    
    # Numeric columns summary - use sampled data for large datasets
    numeric_cols = selected_df.select_dtypes(include=['number']).columns
    
    if len(numeric_cols) > 0:
        st.subheader("📊 Numeric Columns")
        
        # Use sampled data for describe to save memory
        df_sample = sample_for_display(selected_df, max_rows=10000)
        st.dataframe(df_sample[numeric_cols].describe(), use_container_width=True)
        
        # Distribution plots - use heavily sampled data
        st.subheader("📈 Distributions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_num_col = st.selectbox("Select numeric column", numeric_cols)
        
        with col2:
            plot_type = st.selectbox("Plot type", ["Histogram", "Box Plot", "Violin Plot"])
        
        # Use sampled data for visualization
        viz_sample = sample_for_visualization(selected_df, max_points=1000)
        
        if plot_type == "Histogram":
            fig = px.histogram(viz_sample, x=selected_num_col, 
                             title=f"Distribution of {selected_num_col} (sampled)",
                             marginal="box")
        elif plot_type == "Box Plot":
            fig = px.box(viz_sample, y=selected_num_col,
                        title=f"Box Plot of {selected_num_col} (sampled)")
        else:
            fig = px.violin(viz_sample, y=selected_num_col,
                          title=f"Violin Plot of {selected_num_col} (sampled)",
                          box=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Categorical columns
    categorical_cols = selected_df.select_dtypes(include=['object']).columns
    
    if len(categorical_cols) > 0:
        st.subheader("📑 Categorical Columns")
        
        selected_cat_col = st.selectbox("Select categorical column", categorical_cols)
        
        # Use full data for value counts (aggregated, so memory efficient)
        value_counts = selected_df[selected_cat_col].value_counts().head(15)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.dataframe(value_counts, use_container_width=True)
        
        with col2:
            fig = px.bar(x=value_counts.index, y=value_counts.values,
                        title=f"Top 15 Values in {selected_cat_col}",
                        labels={'x': selected_cat_col, 'y': 'Count'})
            st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("Advanced Visualizations")
    
    viz_type = st.selectbox(
        "Visualization Type",
        ["Correlation Heatmap", "Scatter Plot", "Time Series", "Missing Data Pattern"]
    )
    
    # Use sampled data for all visualizations
    viz_sample = sample_for_visualization(selected_df, max_points=2000)
    
    if viz_type == "Correlation Heatmap":
        # Use sampled data for correlation
        numeric_sample = viz_sample.select_dtypes(include=['number'])
        
        if numeric_sample.shape[1] >= 2:
            corr_matrix = numeric_sample.corr()
            
            fig = go.Figure(data=go.Heatmap(
                z=corr_matrix.values,
                x=corr_matrix.columns,
                y=corr_matrix.columns,
                colorscale='RdBu',
                zmid=0
            ))
            fig.update_layout(title="Correlation Matrix (sampled)", height=600)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need at least 2 numeric columns for correlation analysis")
    
    elif viz_type == "Scatter Plot":
        numeric_cols = selected_df.select_dtypes(include=['number']).columns
        
        if len(numeric_cols) >= 2:
            col1, col2 = st.columns(2)
            
            with col1:
                x_col = st.selectbox("X-axis", numeric_cols, key='scatter_x')
            with col2:
                y_col = st.selectbox("Y-axis", numeric_cols, key='scatter_y', 
                                    index=min(1, len(numeric_cols)-1))
            
            # Optional color column
            color_col = st.selectbox("Color by (optional)", 
                                    ['None'] + list(selected_df.columns))
            
            # Use sampled data for scatter plot
            if color_col == 'None':
                fig = px.scatter(viz_sample, x=x_col, y=y_col,
                               title=f"{x_col} vs {y_col} (sampled)")
            else:
                fig = px.scatter(viz_sample, x=x_col, y=y_col, color=color_col,
                               title=f"{x_col} vs {y_col} (sampled)")
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need at least 2 numeric columns for scatter plot")
    
    elif viz_type == "Time Series":
        date_cols = selected_df.select_dtypes(include=['datetime64']).columns
        numeric_cols = selected_df.select_dtypes(include=['number']).columns
        
        if len(date_cols) > 0 and len(numeric_cols) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                date_col = st.selectbox("Date column", date_cols)
            with col2:
                value_col = st.selectbox("Value column", numeric_cols)
            
            # Use sampled and sorted data
            viz_sorted = viz_sample.sort_values(date_col)
            
            fig = px.line(viz_sorted, x=date_col, y=value_col,
                         title=f"{value_col} over Time (sampled)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need date and numeric columns for time series")
    
    elif viz_type == "Missing Data Pattern":
        # Use full data for missing pattern (it's efficient)
        missing_data = selected_df.isnull().sum()
        missing_pct = (missing_data / len(selected_df) * 100).sort_values(ascending=False)
        
        fig = px.bar(x=missing_pct.index, y=missing_pct.values,
                    title="Missing Data by Column (%)",
                    labels={'x': 'Column', 'y': 'Missing %'})
        st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("Search & Filter Data")
    
    # Memory check
    if memory_manager.should_cleanup():
        memory_manager.auto_cleanup()
    
    # Search functionality
    search_col = st.selectbox("Search in column", selected_df.columns)
    search_term = st.text_input("Search term")
    
    if search_term:
        mask = selected_df[search_col].astype(str).str.contains(search_term, case=False, na=False)
        filtered_df = selected_df[mask]
        
        st.success(f"Found {len(filtered_df)} matching rows")
        
        # Use sampled data for display
        display_df = sample_for_display(filtered_df, max_rows=500)
        st.dataframe(display_df, use_container_width=True)
    
    # Filter functionality
    st.subheader("Advanced Filters")
    
    with st.expander("Add Filters"):
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
            
            # Use sampled data for display
            display_df = sample_for_display(filtered_df, max_rows=500)
            st.dataframe(display_df, use_container_width=True)
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
            file_name=f"{selected_sheet}_export.csv",
            mime="text/csv"
        )
    
    # Memory cleanup after operations
    if memory_manager.should_cleanup():
        memory_manager.force_cleanup()
