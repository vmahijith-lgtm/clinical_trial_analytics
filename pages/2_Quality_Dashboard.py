"""
Quality Dashboard Page
Monitor and analyze data quality metrics - Memory Optimized
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import gc

sys.path.append(str(Path(__file__).parent.parent))

from src.quality_checks import QualityChecker
from utils.helpers import format_number
from utils.memory_manager import sample_for_display, memory_manager

st.set_page_config(page_title="Quality Dashboard", page_icon="✅", layout="wide")

# Check if data is loaded
if not st.session_state.get('data_ingested', False):
    st.warning("⚠️ Please load data from the Home page first!")
    st.stop()

st.title("✅ Data Quality Dashboard")
st.markdown("Monitor data quality metrics and identify issues across all datasets")

# Memory check and cleanup
if memory_manager.should_cleanup():
    memory_manager.auto_cleanup()

# Initialize quality checker
if 'quality_reports' not in st.session_state:
    st.session_state.quality_reports = []

# Sidebar controls
with st.sidebar:
    st.header("Quality Analysis")
    
    if st.button("🔍 Run Quality Checks", use_container_width=True, type="primary"):
        with st.spinner("Analyzing data quality..."):
            checker = QualityChecker()
            reports = []
            
            # Run quality checks on sampled datasets (memory efficient)
            sample_size = 5000  # Sample size for quality checks
            
            for category, datasets in st.session_state.categorized_data.items():
                for file_path, sheet, df in datasets:
                    # Use sampled data for quality checks
                    df_sample = sample_for_display(df, max_rows=sample_size)
                    report = checker.generate_comprehensive_report(
                        df_sample, 
                        name=f"{category}/{Path(file_path).name}/{sheet}"
                    )
                    reports.append(report)
                    
                    # Periodic cleanup
                    if len(reports) % 10 == 0:
                        gc.collect()
            
            st.session_state.quality_reports = reports
            st.success("✅ Quality checks complete!")
            
            # Cleanup
            del reports
            gc.collect()
            st.rerun()
    
    if st.session_state.quality_reports:
        st.metric("Datasets Analyzed", len(st.session_state.quality_reports))

# Main dashboard
if not st.session_state.quality_reports:
    st.info("👆 Click 'Run Quality Checks' in the sidebar to analyze data quality")
    st.stop()

reports = st.session_state.quality_reports

# Overall metrics
col1, col2, col3, col4 = st.columns(4)

avg_quality = sum(r['overall_quality_score'] for r in reports) / len(reports)
avg_completeness = sum(r['completeness']['overall_completeness'] for r in reports) / len(reports)
avg_consistency = sum(r['consistency']['consistency_score'] for r in reports) / len(reports)
total_issues = sum(len(r['consistency']['issues']) + len(r['accuracy']['issues']) for r in reports)

with col1:
    st.metric(
        "Overall Quality Score",
        f"{avg_quality:.1%}",
        delta=f"{'Good' if avg_quality >= 0.75 else 'Needs Work'}"
    )

with col2:
    st.metric("Avg Completeness", f"{avg_completeness:.1%}")

with col3:
    st.metric("Avg Consistency", f"{avg_consistency:.1%}")

with col4:
    st.metric("Total Issues", total_issues)

st.divider()

# Tabs for different views
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Detailed Analysis", "⚠️ Issues", "📈 Trends"])

with tab1:
    st.header("Quality Score Distribution")
    
    # Create quality score dataframe
    quality_df = pd.DataFrame([
        {
            'Dataset': r['dataset_name'],
            'Quality Score': r['overall_quality_score'],
            'Status': r['overall_status'],
            'Completeness': r['completeness']['overall_completeness'],
            'Consistency': r['consistency']['consistency_score']
        }
        for r in reports
    ])
    
    # Quality score distribution
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.histogram(
            quality_df,
            x='Quality Score',
            nbins=20,
            title="Quality Score Distribution",
            labels={'Quality Score': 'Quality Score'},
            color_discrete_sequence=['#1f77b4']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Status pie chart
        status_counts = quality_df['Status'].value_counts()
        fig = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Quality Status Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Top and bottom performers
    st.subheader("Performance Highlights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🏆 Top 5 Performers**")
        top_5 = quality_df.nlargest(5, 'Quality Score')[['Dataset', 'Quality Score', 'Status']]
        st.dataframe(top_5, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("**⚠️ Needs Attention**")
        bottom_5 = quality_df.nsmallest(5, 'Quality Score')[['Dataset', 'Quality Score', 'Status']]
        st.dataframe(bottom_5, use_container_width=True, hide_index=True)

with tab2:
    st.header("Detailed Quality Analysis")
    
    # Select dataset for detailed view
    dataset_names = [r['dataset_name'] for r in reports]
    selected_dataset = st.selectbox("Select Dataset", dataset_names)
    
    selected_report = next(r for r in reports if r['dataset_name'] == selected_dataset)
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Quality Score",
            f"{selected_report['overall_quality_score']:.1%}",
            selected_report['overall_status']
        )
    
    with col2:
        st.metric(
            "Completeness",
            f"{selected_report['completeness']['overall_completeness']:.1%}",
            "✓" if selected_report['completeness']['passes_threshold'] else "✗"
        )
    
    with col3:
        st.metric(
            "Consistency",
            f"{selected_report['consistency']['consistency_score']:.1%}",
            "✓" if selected_report['consistency']['passes_threshold'] else "✗"
        )
    
    with col4:
        st.metric("Rows", format_number(selected_report['row_count'], 0))
    
    # Completeness by column
    st.subheader("Column Completeness")
    
    col_completeness = pd.DataFrame({
        'Column': list(selected_report['completeness']['column_completeness'].keys()),
        'Completeness': list(selected_report['completeness']['column_completeness'].values())
    }).sort_values('Completeness')
    
    fig = px.bar(
        col_completeness,
        y='Column',
        x='Completeness',
        orientation='h',
        title="Completeness by Column",
        color='Completeness',
        color_continuous_scale='RdYlGn'
    )
    fig.add_vline(x=0.95, line_dash="dash", line_color="red", 
                  annotation_text="Threshold (95%)")
    st.plotly_chart(fig, use_container_width=True)
    
    # Critical issues
    if selected_report['completeness']['critical_issues']:
        st.warning("⚠️ Critical Completeness Issues")
        critical_df = pd.DataFrame(selected_report['completeness']['critical_issues'])
        st.dataframe(critical_df, use_container_width=True, hide_index=True)

with tab3:
    st.header("Issues & Alerts")
    
    # Prioritize issues
    checker = QualityChecker()
    issues_df = checker.prioritize_issues(reports)
    
    if not issues_df.empty:
        # Filter controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            severity_filter = st.multiselect(
                "Severity",
                options=issues_df['severity'].unique(),
                default=issues_df['severity'].unique()
            )
        
        with col2:
            category_filter = st.multiselect(
                "Category",
                options=issues_df['category'].unique(),
                default=issues_df['category'].unique()
            )
        
        with col3:
            min_priority = st.slider(
                "Min Priority",
                min_value=int(issues_df['priority'].min()),
                max_value=int(issues_df['priority'].max()),
                value=int(issues_df['priority'].min())
            )
        
        # Apply filters
        filtered_issues = issues_df[
            (issues_df['severity'].isin(severity_filter)) &
            (issues_df['category'].isin(category_filter)) &
            (issues_df['priority'] >= min_priority)
        ]
        
        st.metric("Issues Found", len(filtered_issues))
        
        # Display issues
        st.dataframe(
            filtered_issues,
            use_container_width=True,
            hide_index=True,
            column_config={
                "priority": st.column_config.ProgressColumn(
                    "Priority",
                    min_value=0,
                    max_value=int(issues_df['priority'].max()),
                )
            }
        )
        
        # Issue summary by type
        col1, col2 = st.columns(2)
        
        with col1:
            type_counts = filtered_issues['type'].value_counts()
            fig = px.bar(
                x=type_counts.values,
                y=type_counts.index,
                orientation='h',
                title="Issues by Type",
                labels={'x': 'Count', 'y': 'Issue Type'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            severity_counts = filtered_issues['severity'].value_counts()
            fig = px.pie(
                values=severity_counts.values,
                names=severity_counts.index,
                title="Issues by Severity",
                color=severity_counts.index,
                color_discrete_map={'high': 'red', 'medium': 'orange', 'low': 'yellow'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.success("🎉 No issues found! Data quality looks good.")

with tab4:
    st.header("Quality Trends")
    
    # Create trend visualization
    trend_data = pd.DataFrame([
        {
            'Dataset': r['dataset_name'],
            'Quality Score': r['overall_quality_score'],
            'Completeness': r['completeness']['overall_completeness'],
            'Consistency': r['consistency']['consistency_score'],
            'Accuracy': r['accuracy']['accuracy_score']
        }
        for r in reports
    ])
    
    # Multi-metric comparison
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        y=trend_data['Quality Score'],
        name='Quality Score',
        mode='lines+markers'
    ))
    
    fig.add_trace(go.Scatter(
        y=trend_data['Completeness'],
        name='Completeness',
        mode='lines+markers'
    ))
    
    fig.add_trace(go.Scatter(
        y=trend_data['Consistency'],
        name='Consistency',
        mode='lines+markers'
    ))
    
    fig.update_layout(
        title="Quality Metrics Across Datasets",
        yaxis_title="Score",
        xaxis_title="Dataset Index",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Statistical summary
    st.subheader("Statistical Summary")
    
    summary_stats = trend_data[['Quality Score', 'Completeness', 'Consistency', 'Accuracy']].describe()
    st.dataframe(summary_stats, use_container_width=True)

# Export functionality
st.divider()
st.subheader("📥 Export Quality Reports")

if st.button("Download Quality Report as CSV"):
    report_df = pd.DataFrame([
        {
            'Dataset': r['dataset_name'],
            'Quality Score': r['overall_quality_score'],
            'Status': r['overall_status'],
            'Completeness': r['completeness']['overall_completeness'],
            'Consistency': r['consistency']['consistency_score'],
            'Accuracy': r['accuracy']['accuracy_score'],
            'Issues': len(r['consistency']['issues']) + len(r['accuracy']['issues'])
        }
        for r in reports
    ])
    
    csv = report_df.to_csv(index=False)
    st.download_button(
        "Download CSV",
        data=csv,
        file_name="quality_report.csv",
        mime="text/csv"
    )