"""
Utility modules for Clinical Trial Analytics Platform
"""

from .memory_manager import (
    memory_manager,
    display_memory_metrics,
    with_memory_cleanup,
    cleanup_after_function,
    sample_for_display,
    sample_for_visualization,
    clear_session_data_except,
    memory_efficient_merge,
    lazy_load_with_cache,
    DataFrameWrapper
)

from .disk_cache import (
    DiskCache,
    StreamingDataProcessor,
    disk_cache,
    optimize_dataframe_aggressive
)

from .config import (
    PAGE_CONFIG,
    DATA_DIR,
    FILE_PATTERNS,
    QUALITY_THRESHOLDS
)

from .helpers import (
    standardize_column_names,
    convert_date_columns,
    format_number,
    calculate_completeness,
    detect_outliers
)

__all__ = [
    # Memory management
    'memory_manager',
    'display_memory_metrics',
    'with_memory_cleanup',
    'cleanup_after_function',
    'sample_for_display',
    'sample_for_visualization',
    'clear_session_data_except',
    'memory_efficient_merge',
    'lazy_load_with_cache',
    'DataFrameWrapper',
    
    # Disk caching
    'DiskCache',
    'StreamingDataProcessor',
    'disk_cache',
    'optimize_dataframe_aggressive',
    
    # Configuration
    'PAGE_CONFIG',
    'DATA_DIR',
    'FILE_PATTERNS',
    'QUALITY_THRESHOLDS',
    
    # Helpers
    'standardize_column_names',
    'convert_date_columns',
    'format_number',
    'calculate_completeness',
    'detect_outliers'
]

