"""
AI Insights Page
Generative AI-powered insights and recommendations - Memory Optimized
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
import os
import gc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

sys.path.append(str(Path(__file__).parent.parent))

from src.ai_insights import AIInsightsEngine
from src.analytics_engine import AnalyticsEngine
from utils.memory_manager import sample_for_display, memory_manager

st.set_page_config(page_title="AI Insights", page_icon="🤖", layout="wide")

# Check if data is loaded
if not st.session_state.get('data_ingested', False) or not st.session_state.get('unified_views'):
    st.warning("⚠️ Please load data from the Home page first!")
    st.stop()

st.title("🤖 AI-Powered Insights")
st.markdown("Get intelligent recommendations and insights using Claude AI")

# Memory check and cleanup
if memory_manager.should_cleanup():
    memory_manager.auto_cleanup()

# Initialize AI engine
try:
    ai_engine = AIInsightsEngine()
    analytics = AnalyticsEngine()
    
    if not ai_engine.is_available():
        st.error("⚠️ AI Engine not available. Please configure your API key.")
        
        # Provide helpful error messages
        with st.expander("🔧 API Key Configuration", expanded=True):
            st.markdown("""
            **To enable AI features, you need an Anthropic API key:**
            
            **Option 1: Using .env file**
            Create a `.env` file in the project root with:
            ```bash
            ANTHROPIC_API_KEY=your_api_key_here
            ```
            
            **Option 2: Using Streamlit secrets**
            Create `.streamlit/secrets.toml` with:
            ```toml
            ANTHROPIC_API_KEY = "your_api_key_here"
            ```
            
            **Get your API key from:** https://console.anthropic.com/
            
            After adding your API key, refresh the page.
            """)
        st.stop()
        
except Exception as e:
    st.error(f"Error initializing AI engine: {str(e)}")
    st.info("Please check your API key configuration and try again.")
    st.stop()

# Tabs for different AI features
tab1, tab2, tab3, tab4 = st.tabs(["💡 Quick Insights", "📊 Quality Analysis", "🎯 Recommendations", "💬 Chat with Data"])

with tab1:
    st.header("Quick Insights")
    
    insight_type = st.selectbox(
        "Select Insight Type",
        ["Executive Summary", "Enrollment Analysis", "Risk Assessment", "Trend Predictions"]
    )
    
    if st.button("Generate Insights", type="primary", use_container_width=True):
        with st.spinner("Generating AI insights..."):
            try:
                if insight_type == "Executive Summary":
                    # Gather all analytics using sampled data
                    all_insights = {}
                    for category, df in st.session_state.unified_views.items():
                        if not df.empty:
                            # Use sampled data for AI analysis
                            df_sample = sample_for_display(df, max_rows=5000)
                            all_insights[category] = analytics.generate_insights(df_sample, name=category)
                    
                    summary = ai_engine.generate_executive_summary(all_insights)
                    st.markdown(summary)
                
                elif insight_type == "Enrollment Analysis":
                    # Find enrollment data
                    enrollment_df = None
                    for category in ['demographics', 'visits', 'other']:
                        if category in st.session_state.unified_views:
                            df = st.session_state.unified_views[category]
                            if not df.empty:
                                enrollment_df = df
                                break
                    
                    if enrollment_df is not None:
                        # Use sampled data for analysis
                        df_sample = sample_for_display(enrollment_df, max_rows=10000)
                        enrollment_data = analytics.enrollment_analysis(df_sample)
                        if 'error' not in enrollment_data:
                            insights = ai_engine.generate_enrollment_insights(enrollment_data)
                            st.markdown(insights)
                        else:
                            st.error("Unable to analyze enrollment data")
                    else:
                        st.warning("No enrollment data available")
                
                elif insight_type == "Risk Assessment":
                    # Identify bottlenecks across all datasets using sampled data
                    all_bottlenecks = []
                    for category, df in st.session_state.unified_views.items():
                        if not df.empty:
                            # Use sampled data for bottleneck detection
                            df_sample = sample_for_display(df, max_rows=5000)
                            bottlenecks = analytics.identify_bottlenecks(df_sample)
                            all_bottlenecks.extend(bottlenecks)
                    
                    if all_bottlenecks:
                        recommendations = ai_engine.generate_bottleneck_recommendations(all_bottlenecks)
                        st.markdown(recommendations)
                    else:
                        st.success("No significant risks detected!")
                
                elif insight_type == "Trend Predictions":
                    # Get historical data (metadata only, no full data)
                    historical_data = {}
                    for category, df in st.session_state.unified_views.items():
                        if not df.empty:
                            # Get metadata only, not full data
                            date_cols = df.select_dtypes(include=['datetime64']).columns
                            date_range = 'N/A'
                            if len(date_cols) > 0:
                                try:
                                    # Get min/max without loading full data
                                    date_min = df[date_cols[0]].min()
                                    date_max = df[date_cols[0]].max()
                                    date_range = f"{date_min} to {date_max}"
                                except:
                                    pass
                            
                            historical_data[category] = {
                                'rows': len(df),
                                'columns': len(df.columns),
                                'date_range': date_range
                            }
                    
                    predictions = ai_engine.predict_trends(historical_data)
                    st.markdown(predictions)
                    
                # Cleanup after analysis
                gc.collect()
                    
            except Exception as e:
                st.error(f"Error generating insights: {str(e)}")

with tab2:
    st.header("AI Quality Analysis")
    
    if not st.session_state.get('quality_reports'):
        st.info("Run quality checks first from the Quality Dashboard page")
    else:
        st.write("Select a dataset to get AI-powered quality insights:")
        
        reports = st.session_state.quality_reports
        dataset_names = [r['dataset_name'] for r in reports]
        
        selected_dataset = st.selectbox("Dataset", dataset_names)
        selected_report = next(r for r in reports if r['dataset_name'] == selected_dataset)
        
        # Show quality score
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Quality Score", f"{selected_report['overall_quality_score']:.1%}")
        with col2:
            st.metric("Status", selected_report['overall_status'])
        with col3:
            issues_count = len(selected_report['consistency']['issues']) + len(selected_report['accuracy']['issues'])
            st.metric("Issues", issues_count)
        
        if st.button("Analyze with AI", use_container_width=True):
            with st.spinner("Analyzing quality report..."):
                try:
                    insights = ai_engine.generate_quality_insights(selected_report)
                    
                    st.markdown("### 🤖 AI Analysis")
                    st.markdown(insights)
                    
                    # Show raw issues
                    with st.expander("View Raw Data"):
                        st.json(selected_report)
                        
                except Exception as e:
                    st.error(f"Error analyzing quality: {str(e)}")

with tab3:
    st.header("Bottleneck & Recommendations")
    
    # Collect bottlenecks using sampled data
    all_bottlenecks = []
    for category, df in st.session_state.unified_views.items():
        if not df.empty:
            # Use sampled data for bottleneck detection
            df_sample = sample_for_display(df, max_rows=5000)
            bottlenecks = analytics.identify_bottlenecks(df_sample)
            for b in bottlenecks:
                b['dataset'] = category
            all_bottlenecks.extend(bottlenecks)
    
    # Cleanup
    del df_sample
    gc.collect()
    
    if not all_bottlenecks:
        st.success("🎉 No bottlenecks detected! Your trial is running smoothly.")
    else:
        st.warning(f"Found {len(all_bottlenecks)} potential bottlenecks")
        
        # Show bottlenecks
        bottleneck_df = pd.DataFrame(all_bottlenecks)
        st.dataframe(bottleneck_df, use_container_width=True, hide_index=True)
        
        if st.button("Get AI Recommendations", type="primary", use_container_width=True):
            with st.spinner("Generating recommendations..."):
                try:
                    recommendations = ai_engine.generate_bottleneck_recommendations(all_bottlenecks)
                    
                    st.markdown("### 🎯 AI Recommendations")
                    st.markdown(recommendations)
                    
                    # Action items
                    st.markdown("### ✅ Suggested Actions")
                    st.info("""
                    Based on the AI analysis:
                    1. Review high-severity bottlenecks immediately
                    2. Assign action items to responsible team members
                    3. Set up monitoring for critical areas
                    4. Schedule follow-up review in 1 week
                    """)
                    
                except Exception as e:
                    st.error(f"Error generating recommendations: {str(e)}")
    
    # Cleanup
    del all_bottlenecks
    gc.collect()

with tab4:
    st.header("💬 Chat with Your Data")
    
    st.markdown("""
    Ask questions about your clinical trial data and get AI-powered answers.
    
    **Example questions:**
    - What are the main concerns in this trial?
    - How is enrollment progressing?
    - Which sites need attention?
    - What data quality issues should I prioritize?
    """)
    
    # Chat interface
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    
    # Display chat history
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask a question about your clinical trial data..."):
        # Add user message
        st.session_state.chat_history.append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Prepare context
        context = {
            'datasets': list(st.session_state.unified_views.keys()),
            'total_records': sum(len(df) for df in st.session_state.unified_views.values()),
            'quality_reports': len(st.session_state.get('quality_reports', [])),
        }
        
        # Add analytics context if available
        for category, df in st.session_state.unified_views.items():
            if not df.empty:
                context[f'{category}_summary'] = {
                    'rows': len(df),
                    'columns': list(df.columns[:10])  # First 10 columns
                }
        
        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    response = ai_engine.chat_with_data(prompt, context)
                    st.markdown(response)
                    
                    # Add assistant message
                    st.session_state.chat_history.append({"role": "assistant", "content": response})
                    
                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    st.error(error_msg)
                    st.session_state.chat_history.append({"role": "assistant", "content": error_msg})
    
    # Clear chat button
    if st.button("Clear Chat History"):
        st.session_state.chat_history = []
        st.rerun()

# Sidebar information
with st.sidebar:
    st.header("🤖 AI Features")
    
    st.markdown("""
    **Available AI Capabilities:**
    
    - Executive summaries
    - Quality analysis
    - Enrollment insights
    - Risk assessment
    - Trend predictions
    - Interactive Q&A
    """)
    
    st.divider()
    
    st.markdown("""
    **Tips for Best Results:**
    
    - Be specific in your questions
    - Run quality checks first
    - Review multiple insights
    - Verify AI recommendations
    """)
    
    st.divider()
    
    # Show API status
    if ai_engine.is_available():
        st.success("✅ AI Engine Active")
    else:
        st.error("❌ AI Engine Unavailable")
    
    if st.button("🔄 Refresh AI Engine"):
        st.rerun()


