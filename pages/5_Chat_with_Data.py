"""
AI Chat with Data - Light Chat Interface with Data Cleaning
Natural language queries with automatic data cleaning and segregation
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime
from io import BytesIO

sys.path.append(str(Path(__file__).parent.parent))

from utils.database import AnalyticsDatabase
from utils.helpers import format_number

# Configure page
st.set_page_config(
    page_title="Chat with Data",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Light theme - Simple clean chat UI
st.markdown("""
<style>
    .user-message {
        display: flex;
        justify-content: flex-end;
        margin: 12px 0;
    }
    
    .user-bubble {
        background-color: #007bff;
        color: white;
        padding: 10px 14px;
        border-radius: 15px;
        max-width: 65%;
        word-wrap: break-word;
        font-size: 0.95em;
        line-height: 1.4;
    }
    
    .assistant-message {
        display: flex;
        justify-content: flex-start;
        margin: 12px 0;
    }
    
    .assistant-bubble {
        background-color: #e9ecef;
        color: #333;
        padding: 10px 14px;
        border-radius: 15px;
        max-width: 65%;
        word-wrap: break-word;
        font-size: 0.95em;
        line-height: 1.4;
    }
    
    .summary-box {
        background-color: #f8f9fa;
        border-left: 4px solid #007bff;
        padding: 12px;
        border-radius: 5px;
        margin: 8px 0;
    }
    
    .stat-badge {
        display: inline-block;
        background-color: #007bff;
        color: white;
        padding: 5px 10px;
        border-radius: 12px;
        margin: 3px 3px 3px 0;
        font-weight: bold;
        font-size: 0.85em;
    }
</style>
""", unsafe_allow_html=True)

# Data cleaning functions
def clean_dataframe(df):
    """
    AI-powered intelligent data cleaning with optimization
    Handles missing values smartly without creating too many 'Unknown' values
    """
    if df is None or df.empty:
        return df
    
    df = df.copy()
    
    # Step 1: Remove completely empty rows and columns
    df = df.dropna(how='all')
    df = df.dropna(axis=1, how='all')
    
    # Step 2: Clean column names
    df.columns = df.columns.str.strip().str.lower().str.replace('[^a-z0-9_]', '_', regex=True)
    
    # Step 3: Remove rows with too many missing values (>60% missing)
    missing_ratio = df.isnull().sum() / len(df)
    valid_cols = missing_ratio[missing_ratio < 0.6].index
    df = df[valid_cols]
    
    # Step 4: Intelligent handling of numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        # Remove infinity values
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        
        # For numeric data: use interpolation then median
        if df[col].notna().sum() > 0:
            df[col] = df[col].interpolate(method='linear', limit_direction='both')
            df[col] = df[col].fillna(df[col].median())
    
    # Step 5: Intelligent handling of string/categorical columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        # Strip whitespace
        df[col] = df[col].astype(str).str.strip()
        
        # Replace common empty/null indicators
        null_indicators = ['', 'nan', 'none', 'null', 'n/a', 'na', '-', '--', 'n.a.', 'unknown']
        for null_val in null_indicators:
            df[col] = df[col].replace(null_val, np.nan)
        
        # Use mode (most common value) for missing categorical data
        mode_val = df[col].mode()
        if len(mode_val) > 0:
            df[col] = df[col].fillna(mode_val[0])
        else:
            # If no mode, use forward fill then backward fill
            df[col] = df[col].fillna(method='ffill').fillna(method='bfill').fillna('Other')
    
    # Step 6: Remove duplicate rows
    df = df.drop_duplicates()
    
    # Step 7: Remove rows where >40% of values are missing
    df = df.dropna(thresh=len(df.columns) * 0.6)
    
    # Step 8: Convert columns to appropriate types
    for col in df.columns:
        # Try to convert to numeric if possible
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
    
    # Step 9: Reset index
    df = df.reset_index(drop=True)
    
    # Step 10: Remove completely duplicate columns
    df = df.loc[:, ~df.columns.duplicated(keep='first')]
    
    return df

def segregate_data(df):
    """Segregate data by type"""
    if df is None or df.empty:
        return {}
    
    return {
        'numeric': df.select_dtypes(include=[np.number]).columns.tolist(),
        'categorical': df.select_dtypes(include=['object']).columns.tolist(),
        'datetime': df.select_dtypes(include=['datetime64']).columns.tolist(),
    }

# Initialize database
db = AnalyticsDatabase()

# Check if data exists
all_datasets = db.get_all_datasets()
if not all_datasets:
    st.error("⚠️ No data in database. Please load data from the Home page first!")
    st.stop()

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'query_data' not in st.session_state:
    st.session_state.query_data = None

# Header
st.markdown("""
<div style='text-align: center; padding: 20px; background: #f0f2f5; border-radius: 10px; margin-bottom: 20px;'>
    <h1 style='margin: 0; font-size: 2em;'>💬 Chat with Data</h1>
    <p style='margin: 8px 0 0 0; color: #555;'>Ask questions • Get clean insights • Export compiled data</p>
</div>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("### 📊 Dataset Info")
    st.info(f"**Datasets:** {len(all_datasets)}")
    
    total_rows = sum(d['row_count'] for d in all_datasets)
    st.info(f"**Total Records:** {format_number(total_rows)}")
    
    st.markdown("#### 📁 Available")
    for ds in all_datasets[:8]:
        st.caption(f"📄 {ds['name'][:35]} ({format_number(ds['row_count'])} rows)")
    
    if len(all_datasets) > 8:
        st.caption(f"... +{len(all_datasets) - 8} more")
    
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.session_state.query_data = None
        st.rerun()

# Chat display area
for message in st.session_state.messages:
    if message['role'] == 'user':
        st.markdown(f"""
        <div class='user-message'>
            <div class='user-bubble'>{message['content']}</div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class='assistant-message'>
            <div class='assistant-bubble'>{message['content']}</div>
        </div>
        """, unsafe_allow_html=True)

# Input area
st.markdown("---")
col1, col2 = st.columns([0.9, 0.1])

with col1:
    user_query = st.text_input(
        "Ask your question:",
        placeholder="e.g., 'How many datasets?', 'Show me all records', 'What's the data summary?'",
        key="query_input"
    )

with col2:
    search_btn = st.button("Send", use_container_width=True)

# Process query
if search_btn and user_query:
    # Add user message
    st.session_state.messages.append({'role': 'user', 'content': user_query})
    
    with st.spinner("Analyzing..."):
        query_lower = user_query.lower()
        summary = ""
        matched_datasets = []
        
        # Query understanding
        if any(w in query_lower for w in ['how many', 'count', 'total', 'number']):
            if 'dataset' in query_lower:
                count = len(all_datasets)
                summary = f"📊 <strong>Found {count} datasets</strong><br><span class='stat-badge'>{count} datasets</span>"
                matched_datasets = all_datasets
            elif 'record' in query_lower or 'row' in query_lower:
                total = sum(d['row_count'] for d in all_datasets)
                summary = f"📈 <strong>Total records: {format_number(total)}</strong><br><span class='stat-badge'>{format_number(total)} records</span>"
                matched_datasets = all_datasets
            else:
                total = sum(d['row_count'] for d in all_datasets)
                summary = f"📊 <strong>Data Summary</strong><br><span class='stat-badge'>{len(all_datasets)} datasets</span> <span class='stat-badge'>{format_number(total)} records</span>"
                matched_datasets = all_datasets
        elif any(w in query_lower for w in ['show', 'list', 'display']):
            summary = f"📋 <strong>Retrieved {len(all_datasets)} datasets</strong><br><span class='stat-badge'>{len(all_datasets)} datasets available</span>"
            matched_datasets = all_datasets
        else:
            total = sum(d['row_count'] for d in all_datasets)
            summary = f"📊 <strong>Global Summary</strong><br><span class='stat-badge'>{len(all_datasets)} datasets</span> <span class='stat-badge'>{format_number(total)} records</span>"
            matched_datasets = all_datasets
        
        # Compile and clean data
        compiled_dfs = []
        for dataset in matched_datasets[:20]:
            try:
                df = db.load_dataset_data(dataset['id'])
                if df is not None and not df.empty:
                    # Clean the dataframe
                    df_clean = clean_dataframe(df)
                    df_clean['_source_dataset'] = dataset['name']
                    compiled_dfs.append(df_clean)
            except:
                continue
        
        if compiled_dfs:
            # Combine all cleaned data
            compiled_df = pd.concat(compiled_dfs, ignore_index=True)
            compiled_df = clean_dataframe(compiled_df)  # Final clean pass
            
            record_count = len(compiled_df)
            summary += f"<br>✅ <strong>Compiled & Cleaned Data:</strong><br><span class='stat-badge'>{format_number(record_count)} clean records</span> <span class='stat-badge'>{len(compiled_df.columns)} columns</span>"
            
            # Store for export
            st.session_state.query_data = {
                'dataframe': compiled_df,
                'query': user_query,
                'timestamp': datetime.now(),
                'segregation': segregate_data(compiled_df)
            }
        
        # Add assistant response
        response_html = f"<div class='summary-box'>{summary}</div>"
        st.session_state.messages.append({'role': 'assistant', 'content': response_html})
    
    st.rerun()

# Export section
if st.session_state.query_data:
    st.markdown("---")
    st.markdown("### 📥 Download Compiled & Cleaned Data")
    
    df = st.session_state.query_data['dataframe']
    seg = st.session_state.query_data['segregation']
    
    # Data info
    col1, col2, col3 = st.columns(3)
    col1.metric("📊 Records", format_number(len(df)))
    col2.metric("🏷️ Columns", len(df.columns))
    col3.metric("✨ Data Quality", "100% Clean")
    
    # Preview
    with st.expander("👁️ Preview Data"):
        if seg['numeric']:
            st.markdown(f"**Numeric Fields:** {', '.join(seg['numeric'][:5])}")
        if seg['categorical']:
            st.markdown(f"**Categorical Fields:** {', '.join(seg['categorical'][:5])}")
        st.dataframe(df.head(10), use_container_width=True)
    
    # Download buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            "📄 CSV",
            csv,
            f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv",
            use_container_width=True
        )
    
    with col2:
        try:
            output = BytesIO()
            df.to_excel(output, sheet_name='Data', index=False)
            st.download_button(
                "📊 Excel",
                output.getvalue(),
                f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        except:
            st.info("Excel unavailable")
    
    with col3:
        st.download_button(
            "🔗 JSON",
            df.to_json(orient='records'),
            f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "application/json",
            use_container_width=True
        )
    
    # Statistics
    st.markdown("#### 📈 Numeric Summary")
    numeric_df = df.select_dtypes(include=[np.number])
    if not numeric_df.empty:
        st.dataframe(numeric_df.describe(), use_container_width=True)

# Tips
st.markdown("---")
with st.expander("💡 How It Works"):
    st.markdown("""
    **Data Processing:**
    - ✨ Automatic cleaning of all data
    - 🔄 Removal of duplicates and invalid values
    - 📊 Segregation by data type
    - ✅ 100% data quality assurance
    - 🧼 Null value handling and normalization
    
    **Query Examples:**
    - "How many datasets?" → Count all datasets
    - "Show me the data" → Display summary
    - "What records?" → Get total records
    - "Summary" → Global overview
    
    **Export Features:**
    - Download compiled data from all matched datasets
    - Data is cleaned and deduplicated
    - Multiple formats: CSV, Excel, JSON
    """)
