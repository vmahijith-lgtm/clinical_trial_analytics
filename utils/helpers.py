"""
Helper functions for the Clinical Trial Analytics Platform
"""

import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st
import hashlib
import pickle
from pathlib import Path
from typing import Any, Optional
import warnings

warnings.filterwarnings('ignore')

try:
    from utils.config import CACHE_DIR
except:
    CACHE_DIR = Path("cache")

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names to lowercase with underscores"""
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_').str.replace('[^a-z0-9_]', '', regex=True)
    return df

def detect_date_columns(df: pd.DataFrame) -> list:
    """Detect columns that contain date information"""
    date_cols = []
    date_keywords = ['date', 'time', 'dt', 'datetime', 'timestamp', 'day', 'month', 'year']
    
    for col in df.columns:
        # First check column name for date keywords
        if any(keyword in col.lower() for keyword in date_keywords):
            date_cols.append(col)
            continue
            
        # Then check data type for object columns
        if df[col].dtype == 'object':
            try:
                # Sample data for testing
                sample = df[col].dropna().head(50)
                if len(sample) == 0:
                    continue
                
                # Try to parse dates
                parsed = pd.to_datetime(sample, errors='coerce', format='mixed')
                valid_dates = parsed.notna().sum()
                
                # If more than 70% are valid dates, consider it a date column
                if valid_dates > len(sample) * 0.7:
                    date_cols.append(col)
            except Exception:
                pass
    
    return date_cols

def convert_date_columns(df: pd.DataFrame, date_cols: list = None) -> pd.DataFrame:
    """Convert specified columns to datetime"""
    if date_cols is None:
        date_cols = detect_date_columns(df)
    
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed')
        except Exception:
            pass
    
    return df

def calculate_completeness(df: pd.DataFrame) -> dict:
    """Calculate completeness statistics for a dataframe"""
    if df.empty:
        return {
            "overall_completeness": 0,
            "total_cells": 0,
            "missing_cells": 0,
            "column_completeness": {}
        }
    
    total_cells = df.size
    missing_cells = df.isna().sum().sum()
    completeness = (total_cells - missing_cells) / total_cells if total_cells > 0 else 0
    
    return {
        "overall_completeness": completeness,
        "total_cells": total_cells,
        "missing_cells": missing_cells,
        "column_completeness": (1 - df.isna().sum() / len(df)).to_dict()
    }

def detect_outliers(series: pd.Series, method: str = 'iqr') -> pd.Series:
    """Detect outliers in a numeric series"""
    if not pd.api.types.is_numeric_dtype(series):
        return pd.Series([False] * len(series), index=series.index)
    
    # Remove NaN values for calculation
    clean_series = series.dropna()
    
    if len(clean_series) == 0:
        return pd.Series([False] * len(series), index=series.index)
    
    if method == 'iqr':
        Q1 = clean_series.quantile(0.25)
        Q3 = clean_series.quantile(0.75)
        IQR = Q3 - Q1
        
        if IQR == 0:
            return pd.Series([False] * len(series), index=series.index)
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return (series < lower_bound) | (series > upper_bound)
    
    elif method == 'zscore':
        mean = clean_series.mean()
        std = clean_series.std()
        
        if std == 0:
            return pd.Series([False] * len(series), index=series.index)
        
        z_scores = np.abs((series - mean) / std)
        return z_scores > 3
    
    return pd.Series([False] * len(series), index=series.index)

def create_data_hash(data: Any) -> str:
    """Create a hash of data for caching purposes"""
    try:
        return hashlib.md5(str(data).encode()).hexdigest()
    except:
        return hashlib.md5(str(datetime.now()).encode()).hexdigest()

@st.cache_data(ttl=3600)
def load_cached_data(cache_key: str) -> Optional[Any]:
    """Load data from cache"""
    cache_file = CACHE_DIR / f"{cache_key}.pkl"
    if cache_file.exists():
        try:
            with open(cache_file, 'rb') as f:
                return pickle.load(f)
        except Exception:
            return None
    return None

def save_to_cache(cache_key: str, data: Any) -> None:
    """Save data to cache"""
    try:
        CACHE_DIR.mkdir(exist_ok=True)
        cache_file = CACHE_DIR / f"{cache_key}.pkl"
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
    except Exception:
        pass

def format_number(num: float, decimals: int = 2) -> str:
    """Format number with commas and specified decimals"""
    if pd.isna(num):
        return "N/A"
    try:
        return f"{num:,.{decimals}f}"
    except:
        return str(num)

def calculate_metrics_change(current: float, previous: float) -> dict:
    """Calculate change and percentage change between two values"""
    if pd.isna(current) or pd.isna(previous):
        return {
            "change": 0,
            "pct_change": 0,
            "direction": "stable"
        }
    
    change = current - previous
    pct_change = (change / previous * 100) if previous != 0 else 0
    
    return {
        "change": change,
        "pct_change": pct_change,
        "direction": "up" if change > 0 else "down" if change < 0 else "stable"
    }

def safe_divide(numerator: float, denominator: float, default: float = 0) -> float:
    """Safely divide two numbers, returning default if denominator is zero"""
    try:
        if denominator == 0 or pd.isna(denominator) or pd.isna(numerator):
            return default
        return numerator / denominator
    except:
        return default

def clean_text(text: str) -> str:
    """Clean and standardize text"""
    if pd.isna(text):
        return ""
    
    text = str(text).strip()
    text = ' '.join(text.split())  # Remove extra whitespace
    return text

def get_memory_usage(df: pd.DataFrame) -> dict:
    """Get memory usage statistics for a dataframe"""
    memory_usage = df.memory_usage(deep=True)
    
    return {
        "total_mb": memory_usage.sum() / (1024 * 1024),
        "per_column_mb": (memory_usage / (1024 * 1024)).to_dict(),
        "shape": df.shape
    }

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize dataframe data types to reduce memory usage"""
    df_optimized = df.copy()
    
    for col in df_optimized.columns:
        col_type = df_optimized[col].dtype
        
        try:
            if col_type == 'object':
                # Try to convert to numeric if possible
                if df_optimized[col].str.replace('.', '', 1).str.replace('-', '', 1).str.isdigit().all():
                    df_optimized[col] = pd.to_numeric(df_optimized[col], errors='coerce')
                else:
                    # Convert to category if beneficial
                    num_unique = df_optimized[col].nunique()
                    if num_unique > 0 and num_unique / len(df_optimized) < 0.5:
                        df_optimized[col] = df_optimized[col].astype('category')
            
            elif col_type in ['int64', 'float64']:
                # Downcast numeric types
                if col_type == 'int64':
                    df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='integer')
                else:
                    df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='float')
        except Exception:
            pass
    
    return df_optimized

def get_column_info(df: pd.DataFrame) -> pd.DataFrame:
    """Get detailed information about DataFrame columns"""
    info_list = []
    
    for col in df.columns:
        info_list.append({
            'column': col,
            'dtype': str(df[col].dtype),
            'non_null': df[col].notna().sum(),
            'null': df[col].isna().sum(),
            'null_pct': (df[col].isna().sum() / len(df) * 100),
            'unique': df[col].nunique(),
            'unique_pct': (df[col].nunique() / len(df) * 100) if len(df) > 0 else 0
        })
    
    return pd.DataFrame(info_list)

def identify_id_columns(df: pd.DataFrame) -> list:
    """Identify columns that are likely ID columns"""
    id_columns = []
    id_keywords = ['id', 'key', 'code', 'number', 'no', 'num']
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Check if column name contains ID keywords
        if any(keyword in col_lower for keyword in id_keywords):
            id_columns.append(col)
            continue
        
        # Check if column has unique values (likely an ID)
        if df[col].nunique() == len(df) and len(df) > 1:
            id_columns.append(col)
    
    return id_columns

def get_duplicates_info(df: pd.DataFrame, subset: list = None) -> dict:
    """Get information about duplicate rows"""
    if subset is None:
        duplicated = df.duplicated()
    else:
        duplicated = df.duplicated(subset=subset)
    
    dup_count = duplicated.sum()
    
    return {
        "has_duplicates": dup_count > 0,
        "duplicate_count": int(dup_count),
        "duplicate_percentage": (dup_count / len(df) * 100) if len(df) > 0 else 0,
        "unique_count": len(df) - dup_count
    }

def summarize_dataframe(df: pd.DataFrame) -> dict:
    """Generate a comprehensive summary of a dataframe"""
    summary = {
        "shape": {
            "rows": len(df),
            "columns": len(df.columns)
        },
        "memory": get_memory_usage(df),
        "completeness": calculate_completeness(df),
        "duplicates": get_duplicates_info(df),
        "column_types": df.dtypes.value_counts().to_dict(),
        "date_columns": detect_date_columns(df),
        "id_columns": identify_id_columns(df)
    }
    
    # Numeric summary
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        summary["numeric_summary"] = {
            "count": len(numeric_cols),
            "columns": list(numeric_cols)
        }
    
    # Categorical summary
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    if len(categorical_cols) > 0:
        summary["categorical_summary"] = {
            "count": len(categorical_cols),
            "columns": list(categorical_cols)
        }
    
    return summary

def validate_dataframe(df: pd.DataFrame) -> dict:
    """Validate dataframe and return issues"""
    issues = []
    
    # Check for empty dataframe
    if df.empty:
        issues.append({"type": "empty", "severity": "high", "message": "DataFrame is empty"})
    
    # Check for duplicate columns
    if len(df.columns) != len(set(df.columns)):
        issues.append({"type": "duplicate_columns", "severity": "high", "message": "Duplicate column names found"})
    
    # Check for all-null columns
    null_cols = df.columns[df.isna().all()].tolist()
    if null_cols:
        issues.append({"type": "all_null_columns", "severity": "medium", "message": f"{len(null_cols)} columns are completely null"})
    
    # Check for single-value columns
    single_val_cols = [col for col in df.columns if df[col].nunique() == 1]
    if single_val_cols:
        issues.append({"type": "single_value_columns", "severity": "low", "message": f"{len(single_val_cols)} columns have only one unique value"})
    
    return {
        "is_valid": len([i for i in issues if i["severity"] == "high"]) == 0,
        "issues": issues,
        "issue_count": len(issues)
    }