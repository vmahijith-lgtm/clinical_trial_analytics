"""
Cache Key Generation and Dataframe Optimization - SIMPLIFIED
Actual data is stored in SQLite database, not in parquet files.
"""

import pandas as pd
import hashlib
import gc
from pathlib import Path
from typing import Dict, Optional
import numpy as np

class DiskCache:
    """Simplified cache - only generates cache keys. Data stored in database."""
    
    def __init__(self, cache_dir: str = None, max_cache_size_mb: int = 2000, auto_cleanup: bool = True):
        if cache_dir is None:
            cache_dir = Path(__file__).parent.parent / "database" / "processed_data"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def save_dataframe(self, df: pd.DataFrame, file_path: str, sheet_name: str = None,
                       metadata: Dict = None) -> str:
        """Generate cache key for a dataframe"""
        key_string = f"{file_path}_{sheet_name}" if sheet_name else file_path
        cache_key = hashlib.md5(key_string.encode()).hexdigest()[:16]
        return cache_key
    
    def clear_all(self) -> None:
        """No-op: Cache is managed by database now"""
        pass
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'total_entries': 0,
            'cache_size_mb': 0,
            'max_size_mb': 2000,
            'usage_percent': 0
        }


class StreamingDataProcessor:
    """Process large data files one at a time"""
    
    def __init__(self, data_dir: str = None, cache_dir: str = None):
        self.data_dir = Path(data_dir) if data_dir else Path("data")
        self.cache = DiskCache(cache_dir)
    
    def process_file(self, file_path: Path, process_func) -> Dict:
        """Process a single file"""
        results = {'file': str(file_path), 'sheets': 0, 'rows': 0, 'errors': []}
        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            for sheet in excel_file.sheet_names:
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet, engine='openpyxl')
                    if not df.empty and len(df.columns) > 0:
                        processed_df = process_func(df, sheet)
                        results['sheets'] += 1
                        results['rows'] += len(processed_df)
                        del df, processed_df
                except Exception as e:
                    results['errors'].append(f"{sheet}: {str(e)}")
                gc.collect()
            excel_file.close()
        except Exception as e:
            results['errors'].append(str(e))
        return results


def optimize_dataframe_aggressive(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize dataframe for memory efficiency"""
    df = df.copy()
    
    for col in df.columns:
        try:
            col_type = df[col].dtype
            
            # Optimize object columns
            if col_type == 'object':
                unique_ratio = df[col].nunique() / len(df[col])
                if unique_ratio < 0.1:
                    df[col] = df[col].astype('category')
                else:
                    df[col] = df[col].astype(str).str[:100]
            
            # Optimize numeric columns
            elif col_type == 'int64':
                c_min, c_max = df[col].min(), df[col].max()
                if c_min >= -128 and c_max <= 127:
                    df[col] = df[col].astype('int8')
                elif c_min >= -32768 and c_max <= 32767:
                    df[col] = df[col].astype('int16')
                elif c_min >= -2147483648 and c_max <= 2147483647:
                    df[col] = df[col].astype('int32')
            
            elif col_type == 'float64':
                df[col] = df[col].astype('float32')
        except:
            pass
    
    return df


disk_cache = DiskCache()

