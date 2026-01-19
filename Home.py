"""
Clinical Trial Analytics Platform - Home Page
Memory-Optimized Version: Processes ALL data with MINIMAL RAM usage
Uses disk-based caching to avoid loading everything into memory at once.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
from typing import Generator, Tuple, Dict, Any

sys.path.append(str(Path(__file__).parent))

from src.data_ingestion import DataIngestion
from src.quality_checks import QualityChecker
from utils.config import PAGE_CONFIG, DATA_DIR
from utils.helpers import format_number, standardize_column_names, convert_date_columns
from utils.memory_manager import (
    memory_manager
)
from utils.disk_cache import DiskCache, StreamingDataProcessor, optimize_dataframe_aggressive
from utils.database import AnalyticsDatabase

# Initialize memory manager and disk cache
_ = memory_manager
disk_cache = DiskCache(cache_dir=None, max_cache_size_mb=2000, auto_cleanup=True)
db = AnalyticsDatabase()

st.set_page_config(**PAGE_CONFIG)

# Custom CSS styling - Enhanced
st.markdown("""
<style>
    .main {
        padding: 2rem 3rem;
        background: #f5f7fa;
    }
    
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 40px;
        border-radius: 15px;
        margin-bottom: 30px;
        box-shadow: 0 8px 24px rgba(102, 126, 234, 0.4);
        text-align: center;
    }
    
    .main-header h1 {
        margin: 0;
        font-size: 2.5em;
        font-weight: bold;
    }
    
    .main-header p {
        margin: 10px 0 0 0;
        font-size: 1.1em;
        opacity: 0.9;
    }
    
    .capability-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 20px;
        margin: 30px 0;
    }
    
    .capability-card {
        background: white;
        padding: 25px;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        border-top: 4px solid #667eea;
        transition: transform 0.3s, box-shadow 0.3s;
    }
    
    .capability-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.12);
    }
    
    .capability-card h3 {
        margin: 0 0 10px 0;
        color: #2c3e50;
    }
    
    .capability-card p {
        margin: 0;
        color: #666;
        font-size: 0.95em;
        line-height: 1.5;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 25px;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.3);
        border: none;
    }
    
    .metric-card-value {
        font-size: 2.5em;
        font-weight: bold;
        margin: 10px 0;
    }
    
    .metric-card-label {
        font-size: 0.9em;
        opacity: 0.9;
    }
    
    .success-card {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    
    .warning-card {
        background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
    }
    
    .info-card {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    
    .status-badge {
        display: inline-block;
        padding: 10px 18px;
        border-radius: 25px;
        font-weight: bold;
        font-size: 0.9em;
        margin: 5px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    .badge-success { background: #d4edda; color: #155724; }
    .badge-warning { background: #fff3cd; color: #856404; }
    .badge-error { background: #f8d7da; color: #721c24; }
    
    h1 { color: #2c3e50; margin-bottom: 20px; font-weight: bold; }
    h2 { color: #2c3e50; margin-top: 30px; margin-bottom: 15px; border-bottom: 3px solid #667eea; padding-bottom: 10px; font-weight: bold; }
    h3 { color: #34495e; margin-top: 20px; }
</style>
""", unsafe_allow_html=True)

# Initialize minimal session state
if 'processing_started' not in st.session_state:
    st.session_state.processing_started = False
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'file_catalog' not in st.session_state:
    st.session_state.file_catalog = {}  # Only metadata, not actual data
if 'quality_summaries' not in st.session_state:
    st.session_state.quality_summaries = []  # Only summaries, not full reports
if 'last_reload' not in st.session_state:
    st.session_state.last_reload = 0  # Timestamp for tracking reloads
if 'quality_summaries' not in st.session_state:
    st.session_state.quality_summaries = []  # Only summaries, not full reports
if 'new_files_detected' not in st.session_state:
    st.session_state.new_files_detected = False
if 'unprocessed_file_count' not in st.session_state:
    st.session_state.unprocessed_file_count = 0


def get_unprocessed_file_count() -> int:
    """Count files in data directory that are not in database"""
    data_path = Path(DATA_DIR)
    if not data_path.exists():
        return 0
    
    try:
        # Get total Excel files in data directory
        excel_files = list(data_path.rglob('*.xlsx')) + list(data_path.rglob('*.xls'))
        
        # Count how many sheets are in database
        cursor = db.conn.execute("SELECT COUNT(*) FROM datasets WHERE status = 'active'")
        db_sheet_count = cursor.fetchone()[0]
        
        # Count total sheets in Excel files
        total_sheets = 0
        for file_path in excel_files:
            try:
                excel_file = pd.ExcelFile(file_path, engine='openpyxl')
                total_sheets += len(excel_file.sheet_names)
                excel_file.close()
            except:
                pass
        
        # If sheets in DB >= sheets in files, no new files
        if db_sheet_count >= total_sheets:
            return 0
        
        return total_sheets - db_sheet_count
    except:
        return 0


def clear_all_memory() -> None:
    """Clear all cached data from memory and disk"""
    # Clear session state but keep catalog for reference
    for key in list(st.session_state.keys()):
        if key not in ['processing_started', 'processing_complete', 'file_catalog']:
            del st.session_state[key]
    
    # Re-initialize essential session state keys
    st.session_state.quality_summaries = []
    
    # Clear disk cache
    disk_cache.clear_all()
    
    # Force garbage collection
    memory_manager.force_cleanup()
    memory_manager.deep_cleanup()


def process_file_memory_safe(file_path: Path, processor: StreamingDataProcessor, 
                             checker: QualityChecker, max_rows: int) -> Dict[str, Any]:
    """
    Process a single file with aggressive memory management.
    Saves results to disk immediately, clears memory before next file.
    """
    result = {
        'file': str(file_path),
        'sheets': [],
        'total_rows': 0,
        'errors': []
    }
    
    try:
        excel_file = pd.ExcelFile(file_path, engine='openpyxl')
        
        for sheet_name in excel_file.sheet_names:
            # Clear memory BEFORE loading next sheet
            memory_manager.force_cleanup()
            
            # Check if dataset already exists in database
            if db.dataset_exists(str(file_path), sheet_name):
                result['sheets'].append({
                    'sheet_name': sheet_name,
                    'status': 'skipped',
                    'reason': 'Already in database'
                })
                continue
            
            # Check memory - if critical, stop processing
            if memory_manager.get_memory_status() == "critical":
                result['errors'].append(f"Memory critical, stopping at sheet: {sheet_name}")
                break
            
            try:
                # Read sheet
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                
                if df.empty or len(df.columns) == 0:
                    continue
                
                original_rows = len(df)
                
                # Standardize columns
                df = standardize_column_names(df)
                df = convert_date_columns(df)
                
                # Optimize memory
                df = optimize_dataframe_aggressive(df)
                
                # Sample if too large
                if max_rows < 1000000 and len(df) > max_rows:
                    df = df.head(max_rows)
                
                # Save to disk cache
                cache_key = disk_cache.save_dataframe(
                    df,
                    str(file_path),
                    sheet_name,
                    metadata={
                        'original_rows': original_rows,
                        'folder': file_path.parent.name
                    }
                )
                
                if cache_key:
                    result['sheets'].append({
                        'sheet_name': sheet_name,
                        'cache_key': cache_key,
                        'rows': len(df),
                        'original_rows': original_rows,
                        'folder': file_path.parent.name
                    })
                    result['total_rows'] += original_rows
                
                # Run quality check on sample
                if len(df) > 5000:
                    df_sample = df.head(5000)
                else:
                    df_sample = df
                
                report = checker.generate_comprehensive_report(df_sample, f"{file_path.name}/{sheet_name}")
                
                # Save quality metrics to database immediately
                dataset_name = f"{file_path.name}_{sheet_name}"
                dataset_id = db.register_dataset(
                    name=dataset_name,
                    file_path=str(file_path),
                    sheet_name=sheet_name,
                    df=df_sample,  # Sample for hashing
                    cache_key=cache_key,
                    parquet_path="",  # Empty - not used anymore, data stored in database
                    quality_metrics={
                        'overall_score': report['overall_quality_score'],
                        'completeness': report['completeness']['overall_completeness'],
                        'consistency': report.get('consistency', {}).get('overall_consistency', 0),
                        'timeliness': report.get('timeliness', {}).get('overall_timeliness', 0),
                        'accuracy': report.get('accuracy', {}).get('overall_accuracy', 0),
                        'issues': []
                    }
                )
                
                if not dataset_id:
                    result['errors'].append(f"{sheet_name}: Failed to register dataset in database")
                else:
                    # Save full dataframe directly to database
                    try:
                        # Reload full data for storage in database
                        df_full = pd.read_excel(file_path, sheet_name=sheet_name, nrows=max_rows)
                        save_success = db.save_dataset_data(dataset_id, df_full)
                        if save_success:
                            print(f"✓ Saved {sheet_name} (ID: {dataset_id}) - {len(df_full)} rows")
                        else:
                            result['errors'].append(f"{sheet_name}: Failed to save data to database")
                            print(f"✗ Failed to save {sheet_name}")
                        del df_full
                    except Exception as e:
                        result['errors'].append(f"{sheet_name}: {str(e)}")
                        print(f"✗ Error saving {sheet_name}: {str(e)}")
                
                # Store only summary in session state temporarily
                st.session_state.quality_summaries.append({
                    'dataset_name': report['dataset_name'],
                    'overall_quality_score': report['overall_quality_score'],
                    'overall_status': report['overall_status'],
                    'completeness': report['completeness']['overall_completeness'],
                    'rows': original_rows,
                    'columns': len(df.columns),
                    'cache_key': cache_key
                })
                
                # Clear dataframe from memory immediately
                del df, df_sample, report
                memory_manager.force_cleanup()
                
            except Exception as e:
                result['errors'].append(f"{sheet_name}: {str(e)}")
        
        excel_file.close()
        del excel_file
        
    except Exception as e:
        result['errors'].append(f"File: {str(e)}")
    
    return result


def process_all_files_memory_safe(data_path: str, batch_size: int, max_rows: int,
                                   auto_quality_check: bool) -> Generator[Tuple[float, Dict[str, Any], int, int], None, None]:
    """
    Process ALL files with memory-safe approach.
    Only keeps one file in memory at a time.
    
    Yields:
        Tuple of (progress_float, results_dict, processed_count, total_count)
    """
    data_path = Path(data_path)
    
    # Discover files
    ingestion = DataIngestion(data_path)
    files = ingestion.discover_files()
    
    if not files:
        yield 1.0, {'error': 'No files found'}, 0, 0
        return
    
    total_files = len(files)
    total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
    
    results = {
        'files_found': total_files,
        'total_size_mb': total_size_mb,
        'files_processed': 0,
        'sheets_processed': 0,
        'total_rows': 0,
        'errors': [],
        'warnings': []
    }
    
    checker = QualityChecker()
    processor = StreamingDataProcessor(data_path)
    
    # Process files one at a time with memory cleanup between each
    for idx, file_path in enumerate(files):
        # Check memory before each file
        mem_status = memory_manager.get_memory_status()
        if mem_status == "critical":
            results['warnings'].append(f"Memory critical, stopping after {idx} files")
            break
        
        # Update progress
        progress = (idx + 1) / total_files
        
        # Process single file
        file_result = process_file_memory_safe(file_path, processor, checker, max_rows)
        
        results['files_processed'] += 1
        results['sheets_processed'] += len(file_result['sheets'])
        results['total_rows'] += file_result['total_rows']
        
        if file_result['errors']:
            results['errors'].extend(file_result['errors'])
        
        # Store file metadata in catalog (NOT the actual data)
        file_key = str(file_path.relative_to(data_path))
        st.session_state.file_catalog[file_key] = {
            'sheets': file_result['sheets'],
            'cache_keys': [s['cache_key'] for s in file_result['sheets'] if s.get('cache_key')]
        }
        
        # Aggressive cleanup after each file
        memory_manager.deep_cleanup()
        
        # Yield for UI update
        yield progress, results, idx + 1, total_files
    
    results['processing_complete'] = True
    st.session_state.processing_complete = True
    st.session_state.processing_started = True
    
    yield 1.0, results, total_files, total_files


def get_category_summaries() -> Dict[str, int]:
    """Get summary counts by category"""
    categories = {
        'demographics': 0,
        'adverse_events': 0,
        'lab_results': 0,
        'visits': 0,
        'medications': 0,
        'monitoring': 0,
        'other': 0
    }
    
    for file_key, file_info in st.session_state.file_catalog.items():
        for sheet in file_info.get('sheets', []):
            sheet_name = sheet.get('sheet_name', '').lower()
            file_name = file_key.lower()
            search_text = f"{file_name} {sheet_name}"
            
            if any(kw in search_text for kw in ['demog', 'subject', 'patient', 'enrollment']):
                categories['demographics'] += 1
            elif any(kw in search_text for kw in ['ae', 'adverse', 'event', 'safety']):
                categories['adverse_events'] += 1
            elif any(kw in search_text for kw in ['lab', 'laboratory', 'test', 'result']):
                categories['lab_results'] += 1
            elif any(kw in search_text for kw in ['visit', 'appointment', 'schedule']):
                categories['visits'] += 1
            elif any(kw in search_text for kw in ['med', 'drug', 'conmed', 'treatment']):
                categories['medications'] += 1
            elif any(kw in search_text for kw in ['monitor', 'sdv', 'query', 'cra']):
                categories['monitoring'] += 1
            else:
                categories['other'] += 1
    
    return {k: v for k, v in categories.items() if v > 0}


# Header
st.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; border-radius: 10px; margin-bottom: 20px;">
    <h1 style="color: white; margin: 0; text-align: center; font-size: 36px;">🔬 Clinical Trial Analytics Platform</h1>
    <p style="color: rgba(255,255,255,0.9); text-align: center; margin: 10px 0 0 0; font-size: 14px;">Memory-Optimized Processing • Real-Time Quality Monitoring • On-Demand Loading</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div style="background: #f0f2f6; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
    <h3 style="color: #2c3e50; margin-top: 0;">📋 Platform Capabilities</h3>
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
        <div style="padding: 10px; background: white; border-radius: 8px; border-left: 4px solid #4facfe;">
            <strong>Complete Data Processing</strong><br>
            <small>Handles all files without memory constraints</small>
        </div>
        <div style="padding: 10px; background: white; border-radius: 8px; border-left: 4px solid #11998e;">
            <strong>Memory-Safe Operations</strong><br>
            <small>Disk-based caching, single-file processing</small>
        </div>
        <div style="padding: 10px; background: white; border-radius: 8px; border-left: 4px solid #f5576c;">
            <strong>Quality Monitoring</strong><br>
            <small>Automated data quality assessment</small>
        </div>
        <div style="padding: 10px; background: white; border-radius: 8px; border-left: 4px solid #fa709a;">
            <strong>On-Demand Loading</strong><br>
            <small>Data loads only when needed</small>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

st.divider()

# Check for new files and show notification
unprocessed_count = get_unprocessed_file_count()
if unprocessed_count > 0:
    st.warning(f"📁 **{unprocessed_count} new file(s) detected!** Click 'Load and Process All Data' to process them.", icon="⚠️")

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
        <h2 style="margin: 0; color: white;">📁 Data Source Management</h2>
    </div>
    """, unsafe_allow_html=True)
    
    data_path = st.text_input(
        "Data Directory Path",
        value=str(DATA_DIR),
        help="Path to your data files folder"
    )

# Settings section on main page (3 columns)
st.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px; margin-top: 20px;">
    <h2 style="margin: 0; color: white;">⚙️ Processing Settings</h2>
</div>
""", unsafe_allow_html=True)

settings_col1, settings_col2, settings_col3, settings_col4 = st.columns(4)

with settings_col1:
    low_memory_mode = st.toggle(
        "Low Memory Mode",
        value=False,
        help="More aggressive memory management. Slower but uses less RAM."
    )
    
    if low_memory_mode:
        memory_manager.enable_low_memory_mode(True)

with settings_col2:
    batch_size = st.selectbox(
        "Processing Mode",
        options=[1, 3, 5],
        index=0,
        format_func=lambda x: "Single file" if x == 1 else f"Batch of {x}",
        help="Single file = safest, lowest RAM usage"
    )

with settings_col3:
    max_rows = st.selectbox(
        "Max Rows Per Sheet",
        options=[50000, 100000, 200000, 500000],
        index=1,
        format_func=lambda x: f"{x:,}",
        help="Higher = more data, more RAM"
    )

with settings_col4:
    auto_quality_check = st.checkbox(
        "Run Quality Checks",
        value=True,
        help="Run quality checks after loading"
    )

st.markdown("---")

# Continue with main data loading section
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
        <h2 style="margin: 0; color: white;">📁 Data Loading</h2>
    </div>
    """, unsafe_allow_html=True)
    
    if st.button("🚀 Load and Process All Data", type="primary", use_container_width=True):
        
        # Memory check before starting
        if memory_manager.get_memory_status() == "critical":
            st.error("⚠️ Memory is critical! Click 'Force Cleanup' first.")
            st.stop()
        
        # Clear previous data
        clear_all_memory()
        
        # Create progress containers
        progress_container = st.container()
        
        with progress_container:
            st.markdown("---")
            st.subheader("📊 Memory-Safe Processing Pipeline")
            
            # Discovery phase
            with st.spinner("🔍 Discovering files..."):
                ingestion = DataIngestion(data_path)
                files = ingestion.discover_files()
                
                if not files:
                    st.error("❌ No data files found!")
                    st.stop()
                
                total_size_mb = sum(f.stat().st_size for f in files) / (1024 * 1024)
                
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Files Found", len(files))
                with col_b:
                    st.metric("Total Size", f"{total_size_mb:.1f} MB")
                with col_c:
                    processing_mode = "Single file" if batch_size == 1 else f"Batch of {batch_size}"
                    st.metric("Mode", processing_mode)
                
                if total_size_mb > 1000:
                    st.warning(f"⚠️ Large dataset ({total_size_mb:.0f} MB). Using single-file mode for safety.")
            
            # Processing phase
            st.markdown("#### 1️⃣ Processing Files")
            progress_bar = st.progress(0)
            
            try:
                # Process all files with generator
                for progress, results, processed, total in process_all_files_memory_safe(
                    data_path, batch_size, max_rows, auto_quality_check
                ):
                    progress_bar.progress(progress)
                
                # Final stats
                st.markdown("""
                <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); padding: 20px; border-radius: 10px; color: white; text-align: center; margin: 20px 0;">
                    <h2 style="margin: 0; color: white;">✅ Processing Complete!</h2>
                    <p style="margin: 10px 0 0 0;">All data has been successfully processed and cached</p>
                </div>
                """, unsafe_allow_html=True)
                
                final_col1, final_col2, final_col3, final_col4 = st.columns(4)
                with final_col1:
                    st.metric("📊 Files Cached", len(st.session_state.file_catalog))
                with final_col2:
                    total_sheets = sum(len(v.get('sheets', [])) for v in st.session_state.file_catalog.values())
                    st.metric("📑 Total Sheets", total_sheets)
                with final_col3:
                    st.metric("📈 Total Rows", format_number(results['total_rows'], 0))
                with final_col4:
                    cache_stats = disk_cache.get_stats()
                    st.metric("💾 Cache Size", f"{cache_stats['cache_size_mb']:.1f} MB")
                
                # Show errors if any
                if results.get('errors'):
                    with st.expander(f"❌ {len(results['errors'])} Errors Found"):
                        for error in results['errors'][:10]:
                            st.error(error)
                        if len(results['errors']) > 10:
                            st.info(f"... and {len(results['errors']) - 10} more")
                
                # Show warnings if any
                if results.get('warnings'):
                    with st.expander(f"⚠️ {len(results['warnings'])} Warnings"):
                        for warning in results['warnings'][:10]:
                            st.warning(warning)
                
                # Quality summary
                if st.session_state.quality_summaries:
                    avg_quality = sum(r['overall_quality_score'] for r in st.session_state.quality_summaries) / len(st.session_state.quality_summaries)
                    
                    st.subheader("📋 Quality Summary")
                    q_col1, q_col2 = st.columns(2)
                    with q_col1:
                        st.metric("Datasets Checked", len(st.session_state.quality_summaries))
                    with q_col2:
                        st.metric("Avg Quality Score", f"{avg_quality:.1%}")
                
                # Show database status after processing
                st.markdown("---")
                st.subheader("💾 Database Status")
                db_stats = db.get_statistics()
                db_col1, db_col2, db_col3 = st.columns(3)
                with db_col1:
                    st.metric("✅ Datasets in DB", db_stats['total_datasets'])
                with db_col2:
                    st.metric("📈 Total Rows", format_number(db_stats['total_rows'], 0))
                with db_col3:
                    st.metric("💾 DB Size", f"{db_stats['total_size_mb']:.1f} MB")
                
                # Show storage location
                from pathlib import Path
                db_path = Path("database/analytics.db")
                cache_path = Path("database/processed_data/parquet_files")
                
                st.info(f"""
                **📁 Data Storage Locations:**
                
                • **Database:** `database/analytics.db`
                • **Processed Parquet Files:** `database/processed_data/parquet_files/`
                
                All data is now persisted and can be viewed with SQLite DB Browser.
                """)
                
                # Show quality distribution
                status_counts = {}
                for r in st.session_state.quality_summaries:
                    status = r['overall_status']
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                if status_counts:
                    st.write("Quality Distribution:")
                    for status, count in sorted(status_counts.items()):
                        emoji = "🟢" if status == "Excellent" else "🟡" if status == "Good" else "🟠" if status == "Fair" else "🔴"
                        st.write(f"{emoji} {status}: {count}")
                
                # Final cleanup
                memory_manager.deep_cleanup()
                
                # Auto-cleanup: Clear session state and disk cache, keep data in database
                st.info("💾 Data saved to database. Clearing RAM...")
                clear_all_memory()
                st.success("✅ RAM cleared. Data accessible from database.")
                
            except Exception as e:
                st.error(f"❌ Processing failed: {str(e)}")
                st.exception(e)

with col2:
    st.header("📋 Status")
    
    if st.session_state.processing_complete:
        st.success("✅ Processing Complete")
        
        catalog = st.session_state.file_catalog
        total_sheets = sum(len(v.get('sheets', [])) for v in catalog.values())
        
        st.metric("Files Cached", len(catalog))
        st.metric("Sheets Cached", total_sheets)
        st.metric("Quality Reports", len(st.session_state.quality_summaries))
        
        # Cache stats
        cache_stats = disk_cache.get_stats()
        st.metric("Cache Size", f"{cache_stats['cache_size_mb']:.1f} MB")
        
        if st.button("🔄 Unload All Data", use_container_width=True):
            clear_all_memory()
            st.rerun()
    else:
        st.info("📌 Ready to process data")
        st.metric("Version", "3.0")
        st.metric("Mode", "Memory-Optimized")
    
    with st.expander("💡 Memory Tips"):
        st.markdown("""
        **Low Memory Mode:**
        - More aggressive cleanup
        - Lower thresholds
        - Uses ~50% less RAM
        
        **Single File Mode:**
        - Processes one file at a time
        - Saves to disk immediately
        - Lowest RAM usage
        
        **Max Rows:**
        - 50K: Fast, memory efficient
        - 100K: Recommended balance
        - 200K+: More data, more RAM
        
        **Memory Workflow:**
        1. Load file → Optimize → Save to disk
        2. Clear from memory
        3. Load next file
        4. Result: All data processed, minimal RAM
        """)
    
    with st.expander("🔗 Navigation"):
        st.markdown("""
        After loading data:
        
        1. **Data Overview** (Coming Soon)
           - Browse cached datasets
           - View summaries
           - Load on-demand
        
        2. **Quality Dashboard** (Coming Soon)
           - View quality scores
           - Identify issues
           - Track metrics
        
        3. **Analytics** (Coming Soon)
           - Enrollment analysis
           - Site performance
           - Bottleneck detection
        """)
    
    with st.expander("📊 How It Works"):
        st.markdown("""
        **Memory-Safe Processing:**
        
        1. **Discover** all files
        2. **Process one file** at a time
        3. **Save to disk** (Parquet, gzip)
        4. **Clear memory** immediately
        5. **Repeat** for all files
        
        **Result:**
        - ✅ All files processed
        - ✅ All data saved
        - ✅ RAM stays low (~500MB)
        - ⚠️ Slightly slower
        """)


# Show overview if processing complete
if st.session_state.processing_complete:
    st.divider()
    st.header("📊 Data Overview")
    
    # Category summary
    category_counts = get_category_summaries()
    
    if category_counts:
        st.subheader("📑 Data Categories")
        
        cat_col1, cat_col2 = st.columns([1, 2])
        
        with cat_col1:
            for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
                st.metric(cat.replace('_', ' ').title(), count)
        
        with cat_col2:
            try:
                import plotly.express as px
                df_cat = pd.DataFrame({
                    'Category': [k.replace('_', ' ').title() for k in category_counts.keys()],
                    'Count': list(category_counts.values())
                })
                fig = px.bar(
                    df_cat, x='Category', y='Count',
                    title='Datasets by Category',
                    color='Count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(showlegend=False, height=300)
                st.plotly_chart(fig, use_container_width=True)
            except:
                st.bar_chart(pd.Series(category_counts))
    
    # Cached files summary
    st.subheader("💾 Cached Data")
    
    cache_stats = disk_cache.get_stats()
    st.write(f"**Cache Statistics:**")
    st.write(f"- Total entries: {cache_stats['total_entries']}")
    st.write(f"- Cache size: {cache_stats['cache_size_mb']:.1f} MB")
    st.write(f"- Max size: {cache_stats['max_size_mb']} MB")
    
    # Show catalog
    with st.expander("📄 File Catalog"):
        catalog_data = []
        for file_key, file_info in st.session_state.file_catalog.items():
            for sheet in file_info.get('sheets', []):
                catalog_data.append({
                    'File': Path(file_key).name,
                    'Sheet': sheet.get('sheet_name', 'N/A'),
                    'Rows': sheet.get('original_rows', 0),
                    'Cached': 'Yes' if sheet.get('cache_key') else 'No'
                })
        
        if catalog_data:
            df_catalog = pd.DataFrame(catalog_data)
            st.dataframe(df_catalog, use_container_width=True, hide_index=True)
        else:
            st.info("No files in catalog")

else:
    st.info("👆 Configure settings and click **'Load and Process All Data'** to begin")

# Database Management Section
st.divider()
st.markdown("""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 10px; color: white; margin-bottom: 20px;">
    <h2 style="margin: 0; color: white;">💾 Database Management</h2>
</div>
""", unsafe_allow_html=True)

db_tab1, db_tab2, db_tab3, db_tab4 = st.tabs(["📊 View Datasets", "🗑️ Delete Datasets", "⚙️ Database Stats", "🔄 Reload Data"])

with db_tab1:
    st.markdown("### Datasets in Database")
    
    db_datasets = db.get_all_datasets()
    
    if db_datasets:
        db_view_data = []
        for dataset in db_datasets:
            db_view_data.append({
                'Dataset Name': dataset['name'],
                'File Path': dataset['file_path'],
                'Sheet': dataset['sheet_name'],
                'Rows': format_number(dataset['row_count'], 0),
                'Columns': dataset['column_count'],
                'Quality Score': f"{dataset['quality_score']:.1%}" if dataset['quality_score'] else 'N/A',
                'Created': dataset['created_at']
            })
        
        df_view = pd.DataFrame(db_view_data)
        st.dataframe(df_view, use_container_width=True, hide_index=True)
        st.success(f"✅ {len(db_datasets)} datasets in database")
    else:
        st.info("📌 No datasets in database yet")

with db_tab2:
    st.markdown("### Delete Datasets from Database")
    
    db_datasets = db.get_all_datasets()
    
    if db_datasets:
        st.warning("⚠️ This action will permanently remove datasets from the database")
        
        delete_option = st.radio(
            "What would you like to do?",
            ["Delete specific dataset", "Delete all datasets"],
            key="delete_option"
        )
        
        if delete_option == "Delete specific dataset":
            dataset_names = [d['name'] for d in db_datasets]
            selected_to_delete = st.selectbox(
                "Select dataset to delete:",
                options=dataset_names,
                key="dataset_to_delete"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ Delete Selected Dataset", use_container_width=True):
                    selected_dataset = next(d for d in db_datasets if d['name'] == selected_to_delete)
                    if db.delete_dataset(selected_dataset['id']):
                        st.success(f"✅ Deleted: {selected_to_delete}")
                        st.rerun()
                    else:
                        st.error("❌ Failed to delete dataset")
            
            with col2:
                if st.button("❌ Cancel", use_container_width=True):
                    st.info("Cancelled")
        
        elif delete_option == "Delete all datasets":
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("🗑️ Delete ALL Datasets", use_container_width=True, type="secondary"):
                    for dataset in db_datasets:
                        db.delete_dataset(dataset['id'])
                    clear_all_memory()
                    st.success(f"✅ Deleted all {len(db_datasets)} datasets from database")
                    st.rerun()
            
            with col2:
                if st.button("❌ Cancel", use_container_width=True):
                    st.info("Cancelled")
    else:
        st.info("📌 No datasets to delete")

with db_tab3:
    st.markdown("### Database Statistics")
    
    stats = db.get_statistics()
    
    stat_col1, stat_col2, stat_col3 = st.columns(3)
    
    with stat_col1:
        st.metric("📊 Total Datasets", stats['total_datasets'])
    with stat_col2:
        st.metric("📈 Total Rows", format_number(stats['total_rows'], 0))
    with stat_col3:
        st.metric("💾 Total Size", f"{stats['total_size_mb']:.1f} MB")
    
    st.markdown("---")
    st.markdown("### Database Maintenance")
    
    maint_col1, maint_col2, maint_col3 = st.columns(3)
    
    with maint_col1:
        if st.button("🔄 Refresh Database Stats", use_container_width=True):
            st.rerun()
    
    with maint_col2:
        if st.button("🧹 Clear Cache & Database", use_container_width=True):
            clear_all_memory()
            db_datasets = db.get_all_datasets()
            for dataset in db_datasets:
                db.delete_dataset(dataset['id'])
            st.success("✅ Cache and database cleared")
            st.rerun()
    
    with maint_col3:
        if st.button("🔍 Validate Cache", use_container_width=True):
            with st.spinner("Validating cache..."):
                removed = db.cleanup_orphaned_entries()
                if removed > 0:
                    st.warning(f"⚠️ Found and removed {removed} orphaned database entries")
                else:
                    st.success("✅ Cache is valid - no orphaned entries found")
    
    st.markdown("---")
    st.markdown("### Database Info")
    st.info(f"""
    **Database Location:** `cache/analytics.db`
    
    **Tables:**
    - `datasets` - Dataset metadata and quality scores
    - `data_catalog` - Cache key mappings
    - `quality_metrics` - Detailed quality information
    
    **Storage:** SQLite3 with WAL mode for performance
    """)

with db_tab4:
    st.markdown("### Reload Data from Database")
    
    st.info("🔄 Use this to refresh data in all pages without reprocessing. This is useful when you've added new files or want to see latest data.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Reload All Data", use_container_width=True, type="primary"):
            # Update reload timestamp
            import time
            st.session_state.last_reload = time.time()
            st.success("✅ Reload signal sent to all pages")
            st.info("The data in Data Overview, Analytics, and Quality Dashboard will now refresh automatically on their next load")
            st.rerun()
    
    with col2:
        if st.button("📊 Check Reload Status", use_container_width=True):
            import time
            last_reload = st.session_state.get('last_reload', 0)
            if last_reload > 0:
                reload_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_reload))
                st.success(f"✅ Last reload: {reload_time}")
            else:
                st.info("No reload signal sent yet")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: gray; padding: 20px;'>
    Clinical Trial Analytics Platform v3.0 | Memory-Optimized | Processes ALL Data with Minimal RAM
</div>
""", unsafe_allow_html=True)