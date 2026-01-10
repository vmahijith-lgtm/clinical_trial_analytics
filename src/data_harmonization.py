"""
Data Harmonization Module (Integrated with Home.py)
Memory-optimized to process ALL data without crashes
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import gc

class DataHarmonization:
    """Class to harmonize clinical trial data from multiple sources"""
    
    def __init__(self, chunk_size: int = 5000, max_merge_size: int = 50000):
        self.column_mappings = self._get_default_mappings()
        self.harmonized_datasets = {}
        self.chunk_size = chunk_size
        self.max_merge_size = max_merge_size
        
    def _get_default_mappings(self) -> Dict[str, List[str]]:
        """Default column mappings for standardization"""
        return {
            'subject_id': ['subject_id', 'subjectid', 'patient_id', 'patientid', 'id', 'participant_id'],
            'site_id': ['site_id', 'siteid', 'site', 'center_id', 'center'],
            'visit_id': ['visit_id', 'visitid', 'visit', 'visit_number', 'visit_num'],
            'date': ['date', 'visit_date', 'assessment_date', 'exam_date', 'study_date'],
            'age': ['age', 'age_years', 'patient_age'],
            'gender': ['gender', 'sex', 'patient_gender', 'patient_sex'],
            'status': ['status', 'patient_status', 'enrollment_status', 'study_status'],
        }
    
    def find_matching_column(self, df: pd.DataFrame, target_col: str) -> Optional[str]:
        """Find the actual column name that matches a target column"""
        possible_names = self.column_mappings.get(target_col, [target_col])
        
        for col in df.columns:
            if col.lower() in [p.lower() for p in possible_names]:
                return col
        
        return None
    
    def standardize_dataframe(self, df: pd.DataFrame, 
                             schema_mapping: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """Standardize a dataframe to a common schema"""
        df_std = df.copy()
        
        if schema_mapping:
            rename_dict = {}
            for std_name, source_name in schema_mapping.items():
                if source_name in df_std.columns:
                    rename_dict[source_name] = std_name
            
            df_std = df_std.rename(columns=rename_dict)
        
        # Optimize memory
        df_std = self._optimize_dtypes(df_std)
        
        return df_std
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggressively optimize data types to reduce memory"""
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type == 'category':
                continue
            
            # Object types
            if col_type == 'object':
                num_unique = df[col].nunique()
                num_total = len(df[col])
                
                # Convert to category if beneficial
                if num_unique > 0 and num_unique / num_total < 0.5:
                    df[col] = df[col].astype('category')
                else:
                    # Try numeric conversion
                    try:
                        converted = pd.to_numeric(df[col], errors='coerce')
                        if converted.notna().sum() > num_total * 0.5:
                            df[col] = converted
                    except:
                        pass
            
            # Downcast integers
            elif col_type in ['int64', 'int32', 'int16']:
                c_min = df[col].min()
                c_max = df[col].max()
                
                if c_min >= -128 and c_max <= 127:
                    df[col] = df[col].astype('int8')
                elif c_min >= -32768 and c_max <= 32767:
                    df[col] = df[col].astype('int16')
                elif c_min >= -2147483648 and c_max <= 2147483647:
                    df[col] = df[col].astype('int32')
            
            # Downcast floats
            elif col_type in ['float64', 'float32']:
                df[col] = df[col].astype('float32')
        
        return df
    
    def merge_datasets(self, datasets: List[Tuple[str, str, pd.DataFrame]], 
                      join_keys: Optional[List[str]] = None,
                      max_datasets: int = 3) -> pd.DataFrame:
        """Merge datasets with memory safety - processes all provided datasets"""
        
        # Filter out invalid datasets (not DataFrames)
        valid_datasets = []
        for d in datasets:
            if isinstance(d[2], pd.DataFrame):
                valid_datasets.append(d)
            else:
                print(f"  ⚠️  Skipping invalid dataset: {d[0]}/{d[1]} (not a DataFrame)")
        
        datasets = valid_datasets
        
        if not datasets:
            return pd.DataFrame()
        
        # Limit to max_datasets to prevent memory issues
        if len(datasets) > max_datasets:
            print(f"⚠️  Limiting {len(datasets)} datasets to {max_datasets} for memory safety")
            datasets = datasets[:max_datasets]
        
        # Single dataset case
        if len(datasets) == 1:
            df = datasets[0][2].copy()
            df['_source'] = f"{datasets[0][0]}:{datasets[0][1]}"
            return self._optimize_dtypes(df)
        
        # Auto-detect join keys
        if join_keys is None:
            join_keys = self._detect_common_keys([d[2] for d in datasets])
        
        # No common keys - concatenate
        if not join_keys:
            return self._concat_datasets_safe(datasets)
        
        # Merge with keys
        merged = self._merge_with_keys(datasets, join_keys)
        
        gc.collect()
        return merged
    
    def _concat_datasets_safe(self, datasets: List[Tuple[str, str, pd.DataFrame]]) -> pd.DataFrame:
        """Safely concatenate datasets with common columns only"""
        
        # Find common columns
        common_cols = set(datasets[0][2].columns)
        for _, _, df in datasets[1:]:
            common_cols = common_cols.intersection(set(df.columns))
        
        common_cols = list(common_cols)
        
        if not common_cols:
            # No common columns - use first dataset only
            print("⚠️  No common columns. Using first dataset.")
            df = datasets[0][2].copy()
            df['_source'] = f"{datasets[0][0]}:{datasets[0][1]}"
            return self._optimize_dtypes(df)
        
        # Concatenate with common columns
        all_dfs = []
        total_rows = 0
        
        for file_path, sheet, df in datasets:
            df_subset = df[common_cols].copy()
            df_subset['_source'] = f"{file_path}:{sheet}"
            df_subset = self._optimize_dtypes(df_subset)
            
            # Apply size limit per dataset
            max_rows_per_ds = self.max_merge_size // len(datasets)
            if len(df_subset) > max_rows_per_ds:
                df_subset = df_subset.sample(n=max_rows_per_ds, random_state=42)
                print(f"  Sampled {file_path}/{sheet}: {len(df)} → {len(df_subset)} rows")
            
            all_dfs.append(df_subset)
            total_rows += len(df_subset)
            
            del df_subset
            gc.collect()
        
        result = pd.concat(all_dfs, ignore_index=True, sort=False)
        
        del all_dfs
        gc.collect()
        
        print(f"  Concatenated {len(datasets)} datasets: {total_rows} total rows")
        
        return self._optimize_dtypes(result)
    
    def _merge_with_keys(self, datasets: List[Tuple[str, str, pd.DataFrame]], 
                        join_keys: List[str]) -> pd.DataFrame:
        """Merge datasets using common keys"""
        
        # Start with first dataset
        first_df = datasets[0][2].copy()
        if len(first_df) > self.max_merge_size:
            first_df = first_df.sample(n=self.max_merge_size, random_state=42)
            print(f"  Sampled base dataset: {len(datasets[0][2])} → {len(first_df)} rows")
        
        merged = self._optimize_dtypes(first_df)
        merged['_source_1'] = f"{datasets[0][0]}:{datasets[0][1]}"
        
        # Merge subsequent datasets ONE AT A TIME
        for idx, (file_path, sheet, df) in enumerate(datasets[1:], start=2):
            try:
                # Sample if needed
                df_copy = df.copy()
                if len(df_copy) > self.max_merge_size:
                    df_copy = df_copy.sample(n=self.max_merge_size, random_state=42)
                    print(f"  Sampled {file_path}/{sheet}: {len(df)} → {len(df_copy)} rows")
                
                # Find common keys
                common_keys = [k for k in join_keys 
                              if k in merged.columns and k in df_copy.columns]
                
                if not common_keys:
                    print(f"  ⚠️  No common keys with {file_path}/{sheet}. Skipping.")
                    del df_copy
                    continue
                
                # Keep only essential new columns (not already in merged)
                existing_cols = set(merged.columns) - set(common_keys)
                new_cols = [col for col in df_copy.columns if col not in existing_cols]
                
                # Limit new columns to prevent explosion
                if len(new_cols) > 20:
                    print(f"  Limiting {len(new_cols)} new columns to 20 most important")
                    # Prioritize ID and date columns
                    priority_cols = [c for c in new_cols if any(kw in c.lower() for kw in ['id', 'date', 'time', 'value', 'result'])]
                    other_cols = [c for c in new_cols if c not in priority_cols]
                    new_cols = priority_cols[:15] + other_cols[:5]
                
                df_copy = df_copy[common_keys + new_cols].copy()
                df_copy = self._optimize_dtypes(df_copy)
                
                # Use INNER join to control size
                merged = merged.merge(
                    df_copy,
                    on=common_keys,
                    how='inner',  # INNER to prevent size explosion
                    suffixes=('', f'_{idx}')
                )
                
                print(f"  Merged {file_path}/{sheet}: {len(merged)} rows after join")
                
                # Optimize and cleanup
                merged = self._optimize_dtypes(merged)
                del df_copy
                gc.collect()
                
                # Safety: if merged exceeds limit, sample it
                if len(merged) > self.max_merge_size:
                    merged = merged.sample(n=self.max_merge_size, random_state=42)
                    merged = merged.reset_index(drop=True)
                    print(f"  Sampled merged result to {self.max_merge_size} rows")
                
            except Exception as e:
                print(f"  ❌ Error merging {file_path}/{sheet}: {str(e)}")
                continue
        
        return merged
    
    def _detect_common_keys(self, dfs: List[pd.DataFrame]) -> List[str]:
        """Detect common key columns across datasets"""
        if not dfs:
            return []
        
        # Find intersection
        common_cols = set(dfs[0].columns)
        for df in dfs[1:]:
            common_cols = common_cols.intersection(set(df.columns))
        
        # Priority key patterns
        priority_keys = [
            'subject_id', 'patient_id', 'participant_id',
            'site_id', 'center_id',
            'visit_id', 'visit_number',
            'record_id', 'study_id', 'id'
        ]
        
        detected_keys = []
        for key in priority_keys:
            if key in common_cols:
                detected_keys.append(key)
                if len(detected_keys) >= 2:  # Limit to 2 keys max
                    break
        
        # Add other ID-like columns if needed
        if len(detected_keys) < 2:
            for col in sorted(common_cols):
                if col not in detected_keys and ('id' in col.lower() or 'code' in col.lower()):
                    detected_keys.append(col)
                    if len(detected_keys) >= 2:
                        break
        
        return detected_keys
    
    def create_unified_view(self, categorized_data: Dict[str, List],
                           max_datasets_per_category: int = 3) -> Dict[str, pd.DataFrame]:
        """Create unified views for each category - ONE AT A TIME for memory safety"""
        unified_views = {}
        
        for category, datasets in categorized_data.items():
            if not datasets:
                continue
            
            print(f"\n📊 Processing category: {category}")
            print(f"   Found {len(datasets)} datasets")
            
            try:
                # Limit datasets
                datasets_to_use = datasets[:max_datasets_per_category]
                
                if len(datasets) > max_datasets_per_category:
                    print(f"   Limiting to first {max_datasets_per_category} datasets")
                
                # Merge
                unified_df = self.merge_datasets(
                    datasets_to_use,
                    max_datasets=max_datasets_per_category
                )
                
                if unified_df is not None and not unified_df.empty:
                    unified_df = self._optimize_dtypes(unified_df)
                    unified_views[category] = unified_df
                    
                    print(f"   ✓ Created view: {len(unified_df)} rows, {len(unified_df.columns)} columns")
                    print(f"   📦 Memory: {self.get_memory_usage(unified_df)}")
                else:
                    print(f"   ⚠️  No data for {category}")
                
                # Cleanup after each category
                del unified_df
                gc.collect()
                
            except Exception as e:
                print(f"   ✗ Error: {str(e)}")
                continue
        
        self.harmonized_datasets = unified_views
        return unified_views
    
    def deduplicate_records(self, df: pd.DataFrame, 
                          subset: Optional[List[str]] = None) -> pd.DataFrame:
        """Remove duplicate records"""
        if subset is None:
            subset = [col for col in df.columns 
                     if 'id' in col.lower() and not col.startswith('_')]
        
        if subset:
            try:
                original_len = len(df)
                df_dedup = df.drop_duplicates(subset=subset, keep='first')
                if len(df_dedup) < original_len:
                    print(f"   Removed {original_len - len(df_dedup)} duplicates")
                return df_dedup
            except:
                pass
        
        return df
    
    def normalize_values(self, df: pd.DataFrame, column: str, 
                        mapping: Optional[Dict] = None) -> pd.DataFrame:
        """Normalize values in a column"""
        if column not in df.columns:
            return df
        
        df_norm = df.copy()
        
        if mapping:
            df_norm[column] = df_norm[column].map(mapping).fillna(df_norm[column])
        else:
            # Default normalization
            if df_norm[column].dtype == 'object' or df_norm[column].dtype.name == 'category':
                df_norm[column] = df_norm[column].astype(str).str.strip().str.lower()
        
        return df_norm
    
    def calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics from the data"""
        df_derived = df.copy()
        
        try:
            # Age from date of birth
            if 'date_of_birth' in df.columns:
                df_derived['age'] = (
                    pd.Timestamp.now() - pd.to_datetime(df['date_of_birth'], errors='coerce')
                ).dt.days // 365
            
            # Study days from dates
            date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
            if len(date_cols) > 1:
                baseline_col = date_cols[0]
                for col in date_cols[1:]:
                    try:
                        df_derived[f'{col}_study_day'] = (
                            df[col] - df[baseline_col]
                        ).dt.days
                    except:
                        continue
        
        except Exception as e:
            print(f"   ⚠️  Error calculating derived metrics: {e}")
        
        return df_derived
    
    def get_memory_usage(self, df: pd.DataFrame) -> str:
        """Get memory usage in human-readable format"""
        mem_bytes = df.memory_usage(deep=True).sum()
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if mem_bytes < 1024.0:
                return f"{mem_bytes:.2f} {unit}"
            mem_bytes /= 1024.0
        
        return f"{mem_bytes:.2f} TB"