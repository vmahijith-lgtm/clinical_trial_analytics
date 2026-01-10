"""
Disk-Based Cache System
Caches processed data to disk instead of memory to reduce RAM usage.
Uses Parquet format for efficient compression and fast loading.
"""

import pandas as pd
import json
import os
import shutil
import gc
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import pickle
import numpy as np

class DiskCache:
    """
    Disk-based cache for clinical trial data.
    Stores dataframes as Parquet files and metadata as JSON.
    """
    
    def __init__(self, cache_dir: str = None, max_cache_size_mb: int = 2000, auto_cleanup: bool = True):
        """
        Initialize disk cache.
        
        Args:
            cache_dir: Directory for cache files (default: cache/disk_cache)
            max_cache_size_mb: Maximum cache size in MB
            auto_cleanup: Whether to auto-cleanup old entries
        """
        if cache_dir is None:
            cache_dir = Path(__file__).parent.parent / "cache" / "disk_cache"
        else:
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
        
        self.cache_dir = Path(cache_dir)
        self.data_dir = self.cache_dir / "data"
        self.metadata_dir = self.cache_dir / "metadata"
        self.index_file = self.cache_dir / "cache_index.json"
        
        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        self.max_cache_size_mb = max_cache_size_mb
        self.auto_cleanup = auto_cleanup
        
        # Load existing index
        self.index = self._load_index()
    
    def _load_index(self) -> Dict:
        """Load cache index from disk"""
        if self.index_file.exists():
            try:
                with open(self.index_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {'entries': {}, 'last_cleanup': None}
    
    def _save_index(self):
        """Save cache index to disk"""
        with open(self.index_file, 'w') as f:
            json.dump(self.index, f, indent=2, default=str)
    
    def _generate_key(self, file_path: str, sheet_name: str = None) -> str:
        """Generate unique cache key"""
        key_string = f"{file_path}_{sheet_name}" if sheet_name else file_path
        return hashlib.md5(key_string.encode()).hexdigest()[:16]
    
    def _get_entry_path(self, key: str) -> Path:
        """Get path for data file"""
        return self.data_dir / f"{key}.parquet"
    
    def _get_metadata_path(self, key: str) -> Path:
        """Get path for metadata file"""
        return self.metadata_dir / f"{key}.json"
    
    def _get_cache_size(self) -> float:
        """Get current cache size in MB"""
        total_size = 0
        for path in self.data_dir.rglob("*"):
            if path.is_file():
                total_size += path.stat().st_size
        return total_size / (1024 * 1024)
    
    def _cleanup_old_entries(self, retain_count: int = 50):
        """Remove oldest entries to stay within size limit"""
        if self._get_cache_size() < self.max_cache_size_mb * 0.9:
            return 0
        
        # Get entries sorted by access time
        entries = []
        for key, info in self.index['entries'].items():
            data_path = self._get_entry_path(key)
            if data_path.exists():
                access_time = data_path.stat().st_atime
                entries.append((key, access_time, info))
        
        # Sort by access time (oldest first)
        entries.sort(key=lambda x: x[1])
        
        # Remove oldest entries
        removed = 0
        target_size = self.max_cache_size_mb * 0.7
        
        for key, access_time, info in entries[:retain_count]:
            if self._get_cache_size() <= target_size:
                break
            
            try:
                # Remove data file
                data_path = self._get_entry_path(key)
                if data_path.exists():
                    data_path.unlink()
                
                # Remove metadata file
                meta_path = self._get_metadata_path(key)
                if meta_path.exists():
                    meta_path.unlink()
                
                # Remove from index
                del self.index['entries'][key]
                removed += 1
                
            except Exception:
                continue
        
        if removed > 0:
            self._save_index()
        
        self.index['last_cleanup'] = datetime.now().isoformat()
        return removed
    
    def save_dataframe(self, df: pd.DataFrame, file_path: str, sheet_name: str = None, 
                       metadata: Dict = None) -> str:
        """
        Save a dataframe to disk cache.
        
        Args:
            df: DataFrame to save
            file_path: Source file path
            sheet_name: Sheet name (if applicable)
            metadata: Additional metadata to store
            
        Returns:
            Cache key
        """
        key = self._generate_key(file_path, sheet_name)
        
        try:
            # Save dataframe as Parquet (compressed)
            data_path = self._get_entry_path(key)
            df.to_parquet(data_path, compression='gzip', index=False)
            
            # Create metadata
            meta = {
                'key': key,
                'file_path': str(file_path),
                'sheet_name': sheet_name,
                'saved_at': datetime.now().isoformat(),
                'rows': len(df),
                'columns': len(df.columns),
                'memory_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'column_names': list(df.columns),
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
            
            # Add custom metadata
            if metadata:
                meta.update(metadata)
            
            # Save metadata
            meta_path = self._get_metadata_path(key)
            with open(meta_path, 'w') as f:
                json.dump(meta, f, indent=2, default=str)
            
            # Update index
            self.index['entries'][key] = {
                'file_path': str(file_path),
                'sheet_name': sheet_name,
                'saved_at': meta['saved_at'],
                'rows': len(df),
                'size_mb': data_path.stat().st_size / (1024 * 1024)
            }
            
            self._save_index()
            
            # Auto cleanup if needed
            if self.auto_cleanup and self._get_cache_size() > self.max_cache_size_mb:
                self._cleanup_old_entries()
            
            return key
            
        except Exception as e:
            print(f"Error saving to cache: {e}")
            return None
    
    def load_dataframe(self, key: str) -> Optional[pd.DataFrame]:
        """
        Load a dataframe from disk cache.
        
        Args:
            key: Cache key
            
        Returns:
            DataFrame or None if not found
        """
        data_path = self._get_entry_path(key)
        
        if not data_path.exists():
            return None
        
        try:
            df = pd.read_parquet(data_path)
            
            # Update access time
            os.utime(data_path)
            
            return df
            
        except Exception as e:
            print(f"Error loading from cache: {e}")
            return None
    
    def load_dataframe_by_path(self, file_path: str, sheet_name: str = None) -> Optional[pd.DataFrame]:
        """
        Load a dataframe by source path.
        
        Args:
            file_path: Source file path
            sheet_name: Sheet name
            
        Returns:
            DataFrame or None if not found
        """
        key = self._generate_key(file_path, sheet_name)
        return self.load_dataframe(key)
    
    def get_metadata(self, key: str) -> Optional[Dict]:
        """Get metadata for a cached entry"""
        meta_path = self._get_metadata_path(key)
        
        if not meta_path.exists():
            return None
        
        try:
            with open(meta_path, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def find_entries(self, file_path: str = None, sheet_name: str = None) -> List[Dict]:
        """Find cached entries matching criteria"""
        results = []
        
        for key, info in self.index['entries'].items():
            match = True
            
            if file_path and file_path not in info.get('file_path', ''):
                match = False
            if sheet_name and sheet_name != info.get('sheet_name'):
                match = False
            
            if match:
                entry = info.copy()
                entry['key'] = key
                results.append(entry)
        
        return results
    
    def get_catalog_summary(self) -> pd.DataFrame:
        """Get summary of all cached entries"""
        if not self.index['entries']:
            return pd.DataFrame()
        
        data = []
        for key, info in self.index['entries'].items():
            meta = self.get_metadata(key)
            if meta:
                row = {
                    'key': key,
                    'file_path': info.get('file_path', ''),
                    'sheet_name': info.get('sheet_name'),
                    'rows': info.get('rows', 0),
                    'size_mb': info.get('size_mb', 0),
                    'saved_at': meta.get('saved_at', '')
                }
                data.append(row)
        
        return pd.DataFrame(data)
    
    def clear_entry(self, key: str) -> bool:
        """Remove a specific entry from cache"""
        try:
            data_path = self._get_entry_path(key)
            meta_path = self._get_metadata_path(key)
            
            if data_path.exists():
                data_path.unlink()
            if meta_path.exists():
                meta_path.unlink()
            
            if key in self.index['entries']:
                del self.index['entries'][key]
                self._save_index()
            
            return True
        except:
            return False
    
    def clear_all(self):
        """Clear entire cache"""
        try:
            # Remove all files
            for path in self.data_dir.iterdir():
                if path.is_file():
                    path.unlink()
            for path in self.metadata_dir.iterdir():
                if path.is_file():
                    path.unlink()
            
            # Reset index
            self.index = {'entries': {}, 'last_cleanup': None}
            self._save_index()
            
            return True
        except:
            return False
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'total_entries': len(self.index['entries']),
            'cache_size_mb': self._get_cache_size(),
            'max_size_mb': self.max_cache_size_mb,
            'last_cleanup': self.index.get('last_cleanup'),
            'usage_percent': (self._get_cache_size() / self.max_cache_size_mb) * 100
        }


class StreamingDataProcessor:
    """
    Process data files one at a time with disk-based caching.
    Keeps only 1-2 dataframes in memory at any time.
    """
    
    def __init__(self, data_dir: str, cache_dir: str = None):
        self.data_dir = Path(data_dir)
        self.cache = DiskCache(cache_dir)
        self.currently_loaded = {}  # Track what's in memory
    
    def process_file(self, file_path: Path, process_func, chunk_size: int = 50000) -> Dict:
        """
        Process a single file and save results to disk.
        Only keeps one dataframe in memory at a time.
        
        Args:
            file_path: Path to Excel file
            process_func: Function to apply to each sheet
            chunk_size: Maximum rows to process at once
            
        Returns:
            Processing summary
        """
        results = {
            'file': str(file_path),
            'sheets_processed': 0,
            'total_rows': 0,
            'errors': []
        }
        
        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            
            for sheet_name in excel_file.sheet_names:
                # Clear memory before processing each sheet
                self._clear_memory()
                
                try:
                    # Read sheet
                    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                    
                    if df.empty or len(df.columns) == 0:
                        continue
                    
                    # Sample if too large
                    original_rows = len(df)
                    if len(df) > chunk_size:
                        df = df.head(chunk_size)
                    
                    # Apply processing
                    processed_df = process_func(df, sheet_name)
                    
                    # Save to disk cache
                    cache_key = self.cache.save_dataframe(
                        processed_df,
                        str(file_path),
                        sheet_name,
                        metadata={'original_rows': original_rows}
                    )
                    
                    if cache_key:
                        results['sheets_processed'] += 1
                        results['total_rows'] += len(processed_df)
                    
                    # Clear from memory
                    del df, processed_df
                    self._clear_memory()
                    
                except Exception as e:
                    results['errors'].append(f"{sheet_name}: {str(e)}")
            
            excel_file.close()
            
        except Exception as e:
            results['errors'].append(f"File: {str(e)}")
        
        return results
    
    def process_with_quality_check(self, file_path: Path, checker, 
                                   sample_size: int = 5000) -> Dict:
        """
        Process file with quality checks, sampling large sheets.
        """
        results = {
            'file': str(file_path),
            'quality_reports': [],
            'rows_processed': 0,
            'errors': []
        }
        
        try:
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            
            for sheet_name in excel_file.sheet_names:
                self._clear_memory()
                
                try:
                    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                    
                    if df.empty or len(df.columns) == 0:
                        continue
                    
                    original_rows = len(df)
                    
                    # Sample for quality check
                    if len(df) > sample_size:
                        df_sample = df.head(sample_size)
                    else:
                        df_sample = df
                    
                    # Run quality check on sample
                    report = checker.generate_comprehensive_report(
                        df_sample,
                        name=f"{file_path.name}/{sheet_name}"
                    )
                    
                    # Store only summary in memory, full report to disk
                    report_summary = {
                        'dataset_name': report['dataset_name'],
                        'overall_quality_score': report['overall_quality_score'],
                        'overall_status': report['overall_status'],
                        'completeness': report['completeness']['overall_completeness'],
                        'rows': original_rows,
                        'columns': len(df.columns)
                    }
                    
                    results['quality_reports'].append(report_summary)
                    results['rows_processed'] += original_rows
                    
                    del df, df_sample, report
                    self._clear_memory()
                    
                except Exception as e:
                    results['errors'].append(f"{sheet_name}: {str(e)}")
            
            excel_file.close()
            
        except Exception as e:
            results['errors'].append(f"File: {str(e)}")
        
        return results
    
    def _clear_memory(self):
        """Clear memory aggressively"""
        gc.collect()
        if hasattr(gc, 'collect'):
            gc.collect()
        self.currently_loaded.clear()
    
    def get_all_cached(self) -> pd.DataFrame:
        """Get summary of all cached data"""
        return self.cache.get_catalog_summary()
    
    def load_cached(self, key: str) -> Optional[pd.DataFrame]:
        """Load a specific cached dataframe into memory"""
        df = self.cache.load_dataframe(key)
        if df is not None:
            self.currently_loaded[key] = df
        return df
    
    def unload_all(self):
        """Unload all data from memory"""
        self.currently_loaded.clear()
        self._clear_memory()


def optimize_dataframe_aggressive(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggressively optimize a dataframe for minimal memory usage.
    """
    df = df.copy()
    
    for col in df.columns:
        col_type = df[col].dtype
        
        try:
            # Object columns
            if col_type == 'object':
                num_unique = df[col].nunique()
                num_total = len(df[col])
                
                if num_unique > 0 and num_unique / num_total < 0.1:
                    # Very low cardinality - use category
                    df[col] = df[col].astype('category')
                elif num_unique == num_total:
                    # All unique - convert to string
                    df[col] = df[col].astype(str)
                else:
                    # Truncate long strings
                    df[col] = df[col].astype(str).str[:100]
            
            # Integer columns
            elif col_type in ['int64', 'int32']:
                c_min = df[col].min()
                c_max = df[col].max()
                
                if pd.isna(c_min) or pd.isna(c_max):
                    continue
                
                if c_min >= -128 and c_max <= 127:
                    df[col] = df[col].astype('int8')
                elif c_min >= -32768 and c_max <= 32767:
                    df[col] = df[col].astype('int16')
                elif c_min >= -2147483648 and c_max <= 2147483647:
                    df[col] = df[col].astype('int32')
            
            # Float columns
            elif col_type == 'float64':
                df[col] = df[col].astype('float32')
            
        except Exception:
            pass
    
    return df


# Global cache instance
disk_cache = DiskCache()

