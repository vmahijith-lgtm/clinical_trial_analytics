"""
Data Ingestion Module
Handles reading and initial processing of clinical trial data from multiple sources
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import streamlit as st
from utils.config import DATA_DIR, FILE_PATTERNS
from utils.helpers import standardize_column_names, convert_date_columns

class DataIngestion:
    """Class to handle data ingestion from multiple Excel files"""
    
    def __init__(self, data_directory: Path = DATA_DIR):
        self.data_directory = Path(data_directory)
        self.files_discovered = []
        self.data_catalog = {}
        
    def discover_files(self) -> List[Path]:
        """Recursively discover all Excel files in the directory"""
        discovered = []
        
        if not self.data_directory.exists():
            st.error(f"Data directory not found: {self.data_directory}")
            return discovered
        
        for pattern in FILE_PATTERNS:
            discovered.extend(list(self.data_directory.rglob(pattern)))
        
        self.files_discovered = discovered
        return discovered
    
    def read_excel_file(self, file_path: Path) -> Dict[str, pd.DataFrame]:
        """Read all sheets from an Excel file with memory optimization"""
        sheets = {}
        
        try:
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            
            for sheet_name in excel_file.sheet_names:
                try:
                    # Read with low_memory option
                    df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                    
                    # Skip empty sheets
                    if df.empty or len(df.columns) == 0:
                        continue
                    
                    # Limit to first 10,000 rows for large datasets
                    if len(df) > 10000:
                        st.warning(f"Large dataset detected in {file_path.name}/{sheet_name}. Loading first 10,000 rows.")
                        df = df.head(10000)
                    
                    # Standardize column names
                    df = standardize_column_names(df)
                    
                    # Convert date columns (sample only)
                    df = convert_date_columns(df)
                    
                    # Optimize memory usage
                    df = self._optimize_dataframe(df)
                    
                    # Add metadata
                    df.attrs['source_file'] = file_path.name
                    df.attrs['sheet_name'] = sheet_name
                    df.attrs['folder'] = file_path.parent.name
                    
                    sheets[sheet_name] = df
                    
                except Exception as e:
                    st.warning(f"Could not read sheet '{sheet_name}' from {file_path.name}: {str(e)}")
                    
        except Exception as e:
            st.error(f"Could not read file {file_path.name}: {str(e)}")
        
        return sheets
    
    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize dataframe memory usage"""
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type == 'object':
                # Convert to category if unique values < 50% of total
                num_unique = df[col].nunique()
                if num_unique > 0 and num_unique / len(df) < 0.5:
                    try:
                        df[col] = df[col].astype('category')
                    except:
                        pass
            
            elif col_type in ['int64', 'float64']:
                # Downcast numeric types
                try:
                    if col_type == 'int64':
                        df[col] = pd.to_numeric(df[col], downcast='integer')
                    else:
                        df[col] = pd.to_numeric(df[col], downcast='float')
                except:
                    pass
        
        return df
    
    def ingest_all_data(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Ingest all discovered files"""
        if not self.files_discovered:
            self.discover_files()
        
        st.info(f"Found {len(self.files_discovered)} files to process")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, file_path in enumerate(self.files_discovered):
            status_text.text(f"Processing: {file_path.name} ({idx+1}/{len(self.files_discovered)})")
            
            sheets = self.read_excel_file(file_path)
            
            if sheets:
                # Use relative path as key
                rel_path = file_path.relative_to(self.data_directory)
                self.data_catalog[str(rel_path)] = sheets
            
            progress_bar.progress((idx + 1) / len(self.files_discovered))
        
        status_text.text("✅ Data ingestion complete!")
        
        return self.data_catalog
    
    def get_catalog_summary(self) -> pd.DataFrame:
        """Generate a summary of the ingested data catalog"""
        summary_data = []
        
        for file_path, sheets in self.data_catalog.items():
            for sheet_name, df in sheets.items():
                summary_data.append({
                    'file_path': file_path,
                    'folder': Path(file_path).parent.name,
                    'file_name': Path(file_path).name,
                    'sheet_name': sheet_name,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'size_kb': df.memory_usage(deep=True).sum() / 1024,
                    'column_names': ', '.join(df.columns[:5].tolist()) + ('...' if len(df.columns) > 5 else '')
                })
        
        return pd.DataFrame(summary_data)
    
    def categorize_data(self) -> Dict[str, List[Tuple[str, str, pd.DataFrame]]]:
        """Categorize data based on content type"""
        categories = {
            'demographics': [],
            'adverse_events': [],
            'lab_results': [],
            'visits': [],
            'medications': [],
            'monitoring': [],
            'other': []
        }
        
        # Keywords for categorization
        keywords = {
            'demographics': ['demog', 'subject', 'patient', 'enrollment'],
            'adverse_events': ['ae', 'adverse', 'event', 'safety'],
            'lab_results': ['lab', 'laboratory', 'test', 'result'],
            'visits': ['visit', 'appointment', 'schedule'],
            'medications': ['med', 'drug', 'conmed', 'treatment'],
            'monitoring': ['monitor', 'sdv', 'query', 'cra']
        }
        
        for file_path, sheets in self.data_catalog.items():
            for sheet_name, df in sheets.items():
                categorized = False
                
                # Check file name and sheet name for keywords
                search_text = f"{file_path} {sheet_name}".lower()
                
                for category, kw_list in keywords.items():
                    if any(kw in search_text for kw in kw_list):
                        categories[category].append((file_path, sheet_name, df))
                        categorized = True
                        break
                
                if not categorized:
                    categories['other'].append((file_path, sheet_name, df))
        
        return categories
    
    def get_all_dataframes(self) -> List[Tuple[str, str, pd.DataFrame]]:
        """Get all dataframes with their metadata"""
        all_dfs = []
        
        for file_path, sheets in self.data_catalog.items():
            for sheet_name, df in sheets.items():
                all_dfs.append((file_path, sheet_name, df))
        
        return all_dfs