"""
Memory Manager Utility
Provides memory monitoring and automatic cleanup for Streamlit apps
Optimized for low RAM usage with aggressive cleanup strategies
"""

import streamlit as st
import gc
import psutil
import os
from typing import Optional, Callable
from functools import wraps
import time
import pandas as pd
import numpy as np

class MemoryManager:
    """Singleton memory manager for monitoring and controlling memory usage"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not MemoryManager._initialized:
            self.process = psutil.Process(os.getpid())
            self.memory_limit_mb = 2048  # Default 2GB limit
            self.warning_threshold = 60  # Warning at 60% (lowered for safety)
            self.critical_threshold = 75  # Critical at 75% (lowered)
            self.cleanup_callbacks = []
            self.low_memory_mode = False
            self.peak_memory_mb = 0
            MemoryManager._initialized = True
    
    def get_memory_info(self) -> dict:
        """Get current process memory usage"""
        try:
            mem_info = self.process.memory_info()
            rss_mb = mem_info.rss / 1024 / 1024
            
            # Track peak memory
            if rss_mb > self.peak_memory_mb:
                self.peak_memory_mb = rss_mb
            
            return {
                'rss_mb': rss_mb,
                'vms_mb': mem_info.vms / 1024 / 1024,
                'percent': self.process.memory_percent()
            }
        except:
            return {'rss_mb': 0, 'vms_mb': 0, 'percent': 0}
    
    def get_system_memory(self) -> dict:
        """Get system-wide memory info"""
        try:
            mem = psutil.virtual_memory()
            return {
                'total_gb': mem.total / 1024 / 1024 / 1024,
                'available_gb': mem.available / 1024 / 1024 / 1024,
                'percent_used': mem.percent,
                'available_mb': mem.available / 1024 / 1024
            }
        except:
            return {'total_gb': 0, 'available_gb': 0, 'percent_used': 0, 'available_mb': 0}
    
    def get_memory_status(self) -> str:
        """Get memory status string"""
        mem = self.get_memory_info()
        sys_mem = self.get_system_memory()
        
        if self.low_memory_mode:
            # More aggressive thresholds in low memory mode
            if sys_mem['percent_used'] > self.critical_threshold - 10:
                return "critical"
            elif sys_mem['percent_used'] > self.warning_threshold - 10:
                return "warning"
            else:
                return "ok"
        
        if sys_mem['percent_used'] > self.critical_threshold:
            return "critical"
        elif sys_mem['percent_used'] > self.warning_threshold:
            return "warning"
        else:
            return "ok"
    
    def should_cleanup(self) -> bool:
        """Check if cleanup should be triggered"""
        status = self.get_memory_status()
        return status in ["warning", "critical"] or self.low_memory_mode
    
    def force_cleanup(self) -> float:
        """Force garbage collection and return freed memory (MB)"""
        before = self.get_memory_info()['rss_mb']
        
        # Aggressive cleanup sequence
        gc.collect(0)  # Generation 0
        gc.collect(1)  # Generation 1
        gc.collect(2)  # Generation 2 (full collection)
        
        # Try to clean up any cached data in pandas/numpy
        try:
            pd.DataFrame._clear_caches()
        except:
            pass
        
        try:
            np.ndarray.__release__(__builtins__)
        except:
            pass
        
        after = self.get_memory_info()['rss_mb']
        freed = before - after
        
        # If we freed negative (actually used more), report 0
        return max(0, freed)
    
    def deep_cleanup(self) -> float:
        """
        Deep cleanup for critical memory situations.
        Clears all caches and forces full garbage collection.
        """
        before = self.get_memory_info()['rss_mb']
        
        # Clear Streamlit cache if available
        try:
            st.cache_data.clear()
        except:
            pass
        
        # Clear all garbage collection
        gc.collect(2)
        
        # Get memory after cleanup
        after = self.get_memory_info()['rss_mb']
        
        return before - after
    
    def register_cleanup_callback(self, callback: Callable):
        """Register a cleanup callback function"""
        if callback not in self.cleanup_callbacks:
            self.cleanup_callbacks.append(callback)
    
    def trigger_callbacks(self):
        """Trigger all registered cleanup callbacks"""
        for callback in self.cleanup_callbacks:
            try:
                callback()
            except:
                pass
    
    def auto_cleanup(self) -> Optional[str]:
        """Perform automatic cleanup if needed. Returns status message."""
        if self.should_cleanup():
            freed = self.force_cleanup()
            self.trigger_callbacks()
            status = self.get_memory_status()
            
            if freed > 0:
                return f"Auto-cleanup: Freed {freed:.1f} MB. Status: {status}"
            elif status == "critical":
                # Try deep cleanup
                freed = self.deep_cleanup()
                return f"Deep cleanup: Freed {freed:.1f} MB. Status: {status}"
        return None
    
    def check_memory_for_operation(self, estimated_mb: float) -> tuple:
        """Check if there's enough memory for an operation"""
        sys_mem = self.get_system_memory()
        current = self.get_memory_info()['rss_mb']
        
        available = sys_mem['available_mb']
        needed = estimated_mb
        
        if available < needed:
            return (False, f"Not enough memory. Need ~{needed:.0f}MB, have {available:.0f}MB available")
        
        # In low memory mode, be more conservative
        if self.low_memory_mode:
            limit = self.memory_limit_mb * 0.5
        else:
            limit = self.memory_limit_mb
        
        if current + needed > limit:
            # Try cleanup first
            freed = self.force_cleanup()
            if current + needed - freed > limit:
                return (False, f"Memory limit would be exceeded. Current: {current:.0f}MB, Limit: {limit:.0f}MB")
        
        return (True, "OK")
    
    def set_memory_limit(self, limit_mb: int):
        """Set memory limit in MB"""
        self.memory_limit_mb = limit_mb
    
    def enable_low_memory_mode(self, enabled: bool = True):
        """Enable low memory mode for resource-constrained environments"""
        self.low_memory_mode = enabled
        if enabled:
            self.warning_threshold = 50
            self.critical_threshold = 65
        else:
            self.warning_threshold = 60
            self.critical_threshold = 75
    
    def get_memory_advice(self) -> str:
        """Get memory usage advice based on current state"""
        mem = self.get_memory_info()
        sys_mem = self.get_system_memory()
        
        advice = []
        
        if mem['rss_mb'] > 1500:
            advice.append("⚠️ App memory is high (>1.5GB). Consider reducing data load.")
        
        if sys_mem['percent_used'] > 80:
            advice.append("⚠️ System memory is critical. Close other applications.")
        
        if self.low_memory_mode:
            advice.append("ℹ️ Running in low memory mode. Some features may be slower.")
        
        if self.peak_memory_mb > 2000:
            advice.append(f"📊 Peak memory usage: {self.peak_memory_mb:.0f}MB")
        
        return "\n".join(advice) if advice else "Memory usage is normal."
    
    def reset_peak_memory(self):
        """Reset peak memory tracking"""
        self.peak_memory_mb = 0


# Global instance
memory_manager = MemoryManager()


def with_memory_cleanup(func: Callable) -> Callable:
    """Decorator to cleanup memory after function execution"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        memory_manager.force_cleanup()
        return result
    return wrapper


def cleanup_after_function(func: Callable) -> Callable:
    """Decorator for memory cleanup - more aggressive version"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            # Always cleanup, even if there's an error
            memory_manager.force_cleanup()
    return wrapper


def sample_for_display(df, max_rows: int = 1000, random_state: int = 42) -> pd.DataFrame:
    """
    Sample a dataframe for display purposes.
    Uses stratified sampling if possible, otherwise random sampling.
    """
    if len(df) <= max_rows:
        return df
    
    # For dataframes with a 'site' or similar column, try stratified sampling
    strat_col = None
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].nunique() <= 50:
            if col.lower() in ['site', 'country', 'status']:
                strat_col = col
                break
    
    if strat_col and df[strat_col].nunique() > 1:
        # Stratified sampling
        n_samples_per_group = max(1, max_rows // df[strat_col].nunique())
        sampled = df.groupby(strat_col, group_keys=False).apply(
            lambda x: x.sample(n=min(len(x), n_samples_per_group), random_state=random_state)
        )
        return sampled
    
    # Simple random sampling
    return df.sample(n=max_rows, random_state=random_state)


def sample_for_visualization(df, max_points: int = 500) -> pd.DataFrame:
    """Sample dataframe for visualization (smaller sample)"""
    return sample_for_display(df, max_rows=max_points)


def get_streaming_excel_reader(file_path, chunk_size: int = 5000):
    """
    Create a streaming Excel reader that yields dataframes in chunks.
    Use this for processing large files without loading everything at once.
    """
    excel_file = pd.ExcelFile(file_path, engine='openpyxl')
    
    for sheet_name in excel_file.sheet_names:
        chunk_iter = pd.read_excel(
            excel_file, 
            sheet_name=sheet_name, 
            engine='openpyxl',
            chunksize=chunk_size
        )
        for chunk in chunk_iter:
            yield chunk, sheet_name


def clear_session_data_except(keys_to_keep: list = None):
    """
    Clear session state data except for essential keys.
    Essential keys by default: ['data_ingested', 'processing_stats']
    """
    if keys_to_keep is None:
        keys_to_keep = ['data_ingested', 'processing_stats']
    
    keys_to_delete = []
    for key in st.session_state.keys():
        if key not in keys_to_keep:
            keys_to_delete.append(key)
    
    for key in keys_to_delete:
        del st.session_state[key]
    
    gc.collect()
    return len(keys_to_delete)


def memory_efficient_merge(dfs: list, max_dfs: int = 5, chunk_size: int = 10000) -> pd.DataFrame:
    """
    Memory-efficiently merge multiple dataframes.
    Processes in batches to avoid memory spikes.
    """
    if not dfs:
        return pd.DataFrame()
    
    # Limit number of dataframes
    dfs = dfs[:max_dfs]
    
    # Start with first dataframe
    result = dfs[0].copy()
    
    for df in dfs[1:]:
        # Check memory before merge
        mem_status = memory_manager.get_memory_status()
        if mem_status == "critical":
            break
        
        # Merge with alignment
        common_cols = list(set(result.columns) & set(df.columns))
        if common_cols:
            result = pd.merge(result, df, on=common_cols, how='outer')
        else:
            # Just concatenate columns if no common columns
            result = pd.concat([result, df], axis=1)
        
        # Sample if too large
        if len(result) > chunk_size * 10:
            result = result.sample(n=chunk_size * 10, random_state=42)
        
        # Cleanup
        gc.collect()
    
    return result


def display_memory_metrics():
    """Display memory metrics in Streamlit"""
    mem = memory_manager.get_memory_info()
    sys_mem = memory_manager.get_system_memory()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("App Memory", f"{mem['rss_mb']:.0f} MB")
    with col2:
        st.metric("App RAM %", f"{mem['percent']:.1f}%")
    with col3:
        st.metric("System RAM", f"{sys_mem['percent_used']:.0f}%")
    with col4:
        status = memory_manager.get_memory_status()
        emoji = "🔴" if status == "critical" else "🟡" if status == "warning" else "🟢"
        st.metric("Status", f"{emoji} {status.upper()}")
    
    return mem, sys_mem


def lazy_load_with_cache(func, cache_key: str, max_age: int = 3600):
    """
    Lazy loading wrapper that caches results with expiration.
    Use for expensive computations that shouldn't be repeated.
    """
    import time
    
    # Check if we have cached data with timestamp
    cache_key_data = f"{cache_key}_data"
    cache_key_time = f"{cache_key}_timestamp"
    
    now = time.time()
    
    if cache_key_data in st.session_state:
        cached_time = st.session_state.get(cache_key_time, 0)
        if now - cached_time < max_age:
            return st.session_state[cache_key_data]
    
    # Compute and cache
    result = func()
    st.session_state[cache_key_data] = result
    st.session_state[cache_key_time] = now
    
    return result


class DataFrameWrapper:
    """
    Wrapper for dataframes that enables lazy loading and memory-efficient operations.
    """
    
    def __init__(self, df: pd.DataFrame = None, source: str = None):
        self._df = df
        self._source = source
        self._sampled = False
        self._sample_max_rows = 1000
    
    def load(self, df: pd.DataFrame):
        """Load a dataframe with memory optimization"""
        self._df = self._optimize_memory(df)
        return self
    
    def _optimize_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize dataframe memory"""
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type == 'object':
                num_unique = df[col].nunique()
                if num_unique > 0 and num_unique / len(df) < 0.3:
                    try:
                        df[col] = df[col].astype('category')
                    except:
                        pass
            
            elif col_type in ['int64']:
                try:
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                except:
                    pass
            
            elif col_type in ['float64']:
                try:
                    df[col] = df[col].astype('float32')
                except:
                    pass
        
        return df
    
    def get_sampled(self, max_rows: int = None) -> pd.DataFrame:
        """Get a sampled version of the dataframe"""
        if self._sampled:
            return self._df
        
        if max_rows is None:
            max_rows = self._sample_max_rows
        
        self._sampled = True
        self._df = sample_for_display(self._df, max_rows)
        return self._df
    
    def get_full(self) -> pd.DataFrame:
        """Get the full dataframe (use sparingly)"""
        return self._df
    
    def get_stats(self) -> dict:
        """Get dataframe statistics"""
        if self._df is None:
            return {}
        
        return {
            'rows': len(self._df),
            'columns': len(self._df.columns),
            'memory_mb': self._df.memory_usage(deep=True).sum() / 1024 / 1024,
            'is_sampled': self._sampled
        }
    
    def clear(self):
        """Clear the wrapped dataframe"""
        if self._df is not None:
            del self._df
            self._df = None
            gc.collect()

