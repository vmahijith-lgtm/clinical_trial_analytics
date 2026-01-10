"""
Analytics Page
Advanced analytics and operational intelligence - Memory Optimized
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import gc

sys.path.append(str(Path(__file__).parent.parent))

from src.analytics_engine import AnalyticsEngine
from utils.helpers import format_number
from utils.memory_manager import sample_for_display, sample_for_visualization, memory_manager

st.set_page_config(page_title="Analytics", page_icon="📈", layout="wide")

# Check if data is loaded
if not st.session_state.get('data_ingested', False):
    st.warning("⚠️ Please load data from the Home page first!")
    st.stop()

st.title("📈 Advanced Analytics & Insights")

# Memory check and cleanup
if memory_manager.should_cleanup():
    memory_manager.auto_cleanup()

# Initialize analytics engine
analytics = AnalyticsEngine()

# Sidebar: Analytics selection
with st.sidebar:
    st.header("Analytics Module")
    
    analysis_type = st.radio(
        "Select Analysis",
        ["Enrollment Analysis", "Site Performance", "Adverse Events", "Bottleneck Detection", "Statistical Summary"]
    )

# Get unified views
unified_views = st.session_state.unified_views

# Main content based on selection
if analysis_type == "Enrollment Analysis":
    st.header("📊 Enrollment Analysis")
    
    # Try to find demographics or enrollment data
    enrollment_df = None
    for category in ['demographics', 'visits', 'other']:
        if category in unified_views and not unified_views[category].empty:
            enrollment_df = unified_views[category]
            break
    
    if enrollment_df is None or enrollment_df.empty:
        st.warning("No enrollment data available")
        st.stop()
    
    # Use sampled data for analysis
    df_sample = sample_for_display(enrollment_df, max_rows=10000)
    
    # Perform enrollment analysis
    enrollment_results = analytics.enrollment_analysis(df_sample)
    
    if 'error' in enrollment_results:
        st.error(enrollment_results['error'])
    else:
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Subjects", enrollment_results['total_subjects'])
        
        with col2:
            st.metric(
                "Enrollment Rate",
                f"{enrollment_results['enrollment_rate_per_month']:.1f}/month"
            )
        
        with col3:
            st.metric("Study Duration", f"{enrollment_results['date_range_days']} days")
        
        with col4:
            st.metric("Active Sites", len(enrollment_results['enrollment_by_site']))
        
        # Enrollment trend - use sampled data
        st.subheader("Enrollment Trend Over Time")
        
        if 'enrollment_trend' in enrollment_results:
            trend_df = enrollment_results['enrollment_trend']
            date_col = trend_df.columns[0]
            
            fig = px.line(
                trend_df,
                x=date_col,
                y='cumulative_enrollment',
                title="Cumulative Enrollment (sampled)",
                labels={'cumulative_enrollment': 'Total Subjects'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Site enrollment
        if enrollment_results['enrollment_by_site']:
            st.subheader("Enrollment by Site")
            
            site_df = pd.DataFrame({
                'Site': list(enrollment_results['enrollment_by_site'].keys()),
                'Subjects': list(enrollment_results['enrollment_by_site'].values())
            }).sort_values('Subjects', ascending=False)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    site_df,
                    x='Site',
                    y='Subjects',
                    title="Subjects by Site",
                    color='Subjects',
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.pie(
                    site_df.head(10),
                    values='Subjects',
                    names='Site',
                    title="Site Distribution (Top 10)"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Cleanup
    del df_sample
    gc.collect()

elif analysis_type == "Site Performance":
    st.header("🏥 Site Performance Analysis")
    
    # Find data with site information
    site_df = None
    for category, df in unified_views.items():
        if not df.empty and any('site' in col.lower() for col in df.columns):
            site_df = df
            break
    
    if site_df is None:
        st.warning("No site data available")
        st.stop()
    
    # Use sampled data for analysis
    df_sample = sample_for_display(site_df, max_rows=10000)
    
    # Perform site analysis
    site_results = analytics.site_performance_analysis(df_sample)
    
    if 'error' in site_results:
        st.error(site_results['error'])
    else:
        # Display metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Sites", site_results['total_sites'])
        
        with col2:
            st.metric(
                "Avg Subjects/Site",
                f"{site_results['avg_subjects_per_site']:.1f}"
            )
        
        with col3:
            st.metric(
                "Median Subjects/Site",
                f"{site_results['median_subjects_per_site']:.0f}"
            )
        
        # Top performers
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏆 Top Performing Sites")
            top_sites = pd.DataFrame({
                'Site': list(site_results['top_performing_sites'].keys()),
                'Subjects': list(site_results['top_performing_sites'].values())
            })
            st.dataframe(top_sites, use_container_width=True, hide_index=True)
        
        with col2:
            st.subheader("⚠️ Underperforming Sites")
            bottom_sites = pd.DataFrame({
                'Site': list(site_results['underperforming_sites'].keys()),
                'Subjects': list(site_results['underperforming_sites'].values())
            })
            st.dataframe(bottom_sites, use_container_width=True, hide_index=True)
        
        # Site distribution
        st.subheader("Site Performance Distribution")
        
        dist_df = pd.DataFrame({
            'Site': list(site_results['site_distribution'].keys()),
            'Count': list(site_results['site_distribution'].values())
        })
        
        fig = px.histogram(
            dist_df,
            x='Count',
            nbins=20,
            title="Distribution of Subjects Across Sites",
            labels={'Count': 'Subjects per Site'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Cleanup
    del df_sample
    gc.collect()

elif analysis_type == "Adverse Events":
    st.header("⚠️ Adverse Events Analysis")
    
    # Find AE data
    ae_df = unified_views.get('adverse_events', pd.DataFrame())
    
    if ae_df.empty:
        st.warning("No adverse events data available")
        st.stop()
    
    # Use sampled data for analysis
    df_sample = sample_for_display(ae_df, max_rows=5000)
    
    # Perform AE analysis
    ae_results = analytics.adverse_events_analysis(df_sample)
    
    if not ae_results.get('data_available', False):
        st.error("Adverse events data not properly formatted")
        st.stop()
    
    # Display metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Events", ae_results['total_events'])
    
    with col2:
        st.metric("Unique Event Types", ae_results['unique_event_types'])
    
    # Most common events
    if 'most_common_events' in ae_results:
        st.subheader("Most Common Adverse Events")
        
        common_events = pd.DataFrame({
            'Event': list(ae_results['most_common_events'].keys()),
            'Count': list(ae_results['most_common_events'].values())
        })
        
        fig = px.bar(
            common_events.head(15),
            x='Count',
            y='Event',
            orientation='h',
            title="Top 15 Adverse Events (sampled)",
            color='Count',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Severity distribution
    if 'severity_distribution' in ae_results:
        st.subheader("Severity Distribution")
        
        col1, col2 = st.columns(2)
        
        with col1:
            severity_df = pd.DataFrame({
                'Severity': list(ae_results['severity_distribution'].keys()),
                'Count': list(ae_results['severity_distribution'].values())
            })
            st.dataframe(severity_df, use_container_width=True, hide_index=True)
        
        with col2:
            fig = px.pie(
                severity_df,
                values='Count',
                names='Severity',
                title="Events by Severity"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Events by site
    if 'events_by_site' in ae_results:
        st.subheader("Events by Site")
        
        site_events = pd.DataFrame({
            'Site': list(ae_results['events_by_site'].keys()),
            'Events': list(ae_results['events_by_site'].values())
        }).sort_values('Events', ascending=False)
        
        fig = px.bar(
            site_events.head(10),
            x='Site',
            y='Events',
            title="Adverse Events by Site (Top 10)"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Cleanup
    del df_sample
    gc.collect()

elif analysis_type == "Bottleneck Detection":
    st.header("🔍 Bottleneck Detection")
    
    # Analyze all unified views for bottlenecks using sampled data
    all_bottlenecks = []
    
    for category, df in unified_views.items():
        if not df.empty:
            # Use sampled data for bottleneck detection
            df_sample = sample_for_display(df, max_rows=5000)
            bottlenecks = analytics.identify_bottlenecks(df_sample)
            for b in bottlenecks:
                b['dataset'] = category
            all_bottlenecks.extend(bottlenecks)
            
            # Cleanup sample
            del df_sample
    
    # Cleanup
    gc.collect()
    
    if not all_bottlenecks:
        st.success("🎉 No significant bottlenecks detected!")
    else:
        st.warning(f"Found {len(all_bottlenecks)} potential bottlenecks")
        
        # Group by severity
        high_severity = [b for b in all_bottlenecks if b.get('severity') == 'high']
        medium_severity = [b for b in all_bottlenecks if b.get('severity') == 'medium']
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Bottlenecks", len(all_bottlenecks))
        with col2:
            st.metric("High Severity", len(high_severity), delta="Critical")
        with col3:
            st.metric("Medium Severity", len(medium_severity))
        
        # Display bottlenecks
        st.subheader("Identified Bottlenecks")
        
        bottleneck_df = pd.DataFrame(all_bottlenecks)
        st.dataframe(
            bottleneck_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Visualize by type
        if 'type' in bottleneck_df.columns:
            type_counts = bottleneck_df['type'].value_counts()
            
            fig = px.bar(
                x=type_counts.values,
                y=type_counts.index,
                orientation='h',
                title="Bottlenecks by Type",
                labels={'x': 'Count', 'y': 'Type'},
                color=type_counts.values,
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Cleanup
    del all_bottlenecks
    gc.collect()

elif analysis_type == "Statistical Summary":
    st.header("📊 Statistical Summary")
    
    # Select dataset
    category = st.selectbox("Select Category", list(unified_views.keys()))
    df = unified_views[category]
    
    if df.empty:
        st.warning("No data available for this category")
        st.stop()
    
    # Use sampled data for statistical analysis
    df_sample = sample_for_display(df, max_rows=10000)
    
    # Get statistical summary
    stats = analytics.statistical_summary(df_sample)
    
    st.subheader("Numeric Variables Summary")
    
    if stats['statistics']:
        # Create summary dataframe
        stats_df = pd.DataFrame(stats['statistics']).T
        st.dataframe(stats_df, use_container_width=True)
        
        # Correlation analysis - use sampled data
        st.subheader("Correlation Analysis")
        
        corr_matrix = analytics.correlation_analysis(df_sample)
        
        if not corr_matrix.empty:
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
        st.info("No numeric columns found for statistical analysis")
    
    # Data insights - use sampled data
    st.subheader("Comprehensive Insights")
    
    insights = analytics.generate_insights(df_sample, name=category)
    
    with st.expander("View Detailed Insights"):
        st.json(insights)
    
    # Cleanup
    del df_sample
    gc.collect()
