"""
Fast and Efficient Database Layer
Uses SQLite for metadata and Parquet for large data storage
Provides quick access to data without loading everything into memory
"""

import sqlite3
import pandas as pd
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import hashlib

class AnalyticsDatabase:
    """
    Fast database for clinical trial data
    - SQLite for metadata and indexing
    - Parquet files for actual data storage
    """
    
    def __init__(self, db_path: str = None):
        """Initialize database"""
        if db_path is None:
            # Use database folder for persistent storage
            db_path = Path(__file__).parent.parent / "database" / "analytics.db"
        else:
            db_path = Path(db_path)
        
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = None
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables"""
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        # Enable foreign keys and WAL mode for better performance
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        
        # Create tables
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            file_path TEXT NOT NULL,
            sheet_name TEXT,
            data_hash TEXT UNIQUE,
            row_count INTEGER,
            column_count INTEGER,
            columns_json TEXT,
            dtypes_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            quality_score REAL,
            completeness REAL,
            status TEXT DEFAULT 'active'
        )
        """)
        
        # Create index for faster lookups
        self.conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_datasets_name ON datasets(name)
        """)
        self.conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_datasets_status ON datasets(status)
        """)
        
        # Create data catalog table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS data_catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id INTEGER NOT NULL,
            cache_key TEXT UNIQUE NOT NULL,
            parquet_path TEXT NOT NULL,
            file_size_mb REAL,
            rows INTEGER,
            columns INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
        """)
        
        self.conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_catalog_dataset ON data_catalog(dataset_id)
        """)
        self.conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_catalog_cache_key ON data_catalog(cache_key)
        """)
        
        # Create quality metrics table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS quality_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id INTEGER NOT NULL,
            completeness REAL,
            consistency REAL,
            timeliness REAL,
            accuracy REAL,
            overall_score REAL,
            issues_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
        """)
        
        # Create data storage table for storing actual dataframe data
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id INTEGER NOT NULL UNIQUE,
            data_pickle BLOB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        )
        """)
        
        self.conn.commit()
    
    def register_dataset(self, name: str, file_path: str, sheet_name: str,
                        df: pd.DataFrame, cache_key: str, parquet_path: str = "",
                        quality_metrics: Dict = None) -> int:
        """Register a dataset in the database"""
        try:
            # Validate inputs
            if not cache_key:
                raise ValueError("cache_key cannot be empty")
            
            # Generate data hash
            data_hash = hashlib.md5(
                pd.util.hash_pandas_object(df, index=True).values
            ).hexdigest()
            
            # Get file size if parquet path provided
            file_size_mb = 0
            if parquet_path and os.path.exists(str(parquet_path)):
                file_size_mb = os.path.getsize(str(parquet_path)) / (1024 * 1024)
            
            # Store columns and dtypes
            columns_json = json.dumps(df.columns.tolist())
            dtypes_json = json.dumps({col: str(df[col].dtype) for col in df.columns})
            
            # Insert dataset
            cursor = self.conn.execute("""
            INSERT INTO datasets (name, file_path, sheet_name, data_hash, row_count, 
                                 column_count, columns_json, dtypes_json, quality_score, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, str(file_path), sheet_name, data_hash, len(df), 
                  len(df.columns), columns_json, dtypes_json, 
                  quality_metrics.get('overall_score', 0) if quality_metrics else 0, 'active'))
            
            dataset_id = cursor.lastrowid
            
            # Register in catalog (parquet_path can be empty, data stored in database)
            self.conn.execute("""
            INSERT INTO data_catalog (dataset_id, cache_key, parquet_path, file_size_mb, rows, columns)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (dataset_id, cache_key, str(parquet_path) if parquet_path else "", file_size_mb, len(df), len(df.columns)))
            
            # Store quality metrics if provided
            if quality_metrics:
                self.conn.execute("""
                INSERT INTO quality_metrics (dataset_id, completeness, consistency, 
                                           timeliness, accuracy, overall_score, issues_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (dataset_id, 
                      quality_metrics.get('completeness', 0),
                      quality_metrics.get('consistency', 0),
                      quality_metrics.get('timeliness', 0),
                      quality_metrics.get('accuracy', 0),
                      quality_metrics.get('overall_score', 0),
                      json.dumps(quality_metrics.get('issues', []))))
            
            self.conn.commit()
            return dataset_id
            
        except sqlite3.IntegrityError as e:
            # Dataset already exists
            existing_id = self.get_dataset_id(name)
            if existing_id:
                return existing_id
            raise ValueError(f"Integrity error registering dataset: {e}")
        except Exception as e:
            error_msg = f"Error registering dataset '{name}': {type(e).__name__}: {str(e)}"
            print(error_msg)
            self.conn.rollback()
            return None
    
    def get_dataset_id(self, name: str) -> Optional[int]:
        """Get dataset ID by name"""
        cursor = self.conn.execute("SELECT id FROM datasets WHERE name = ?", (name,))
        row = cursor.fetchone()
        return row[0] if row else None
    
    def dataset_exists(self, file_path: str, sheet_name: str) -> bool:
        """Check if a dataset already exists in database"""
        dataset_name = f"{Path(file_path).name}_{sheet_name}"
        cursor = self.conn.execute(
            "SELECT id FROM datasets WHERE name = ? AND status = 'active'", 
            (dataset_name,)
        )
        return cursor.fetchone() is not None
    
    def get_dataset_metadata(self, dataset_id: int) -> Optional[Dict]:
        """Get dataset metadata"""
        cursor = self.conn.execute(
            "SELECT * FROM datasets WHERE id = ?", (dataset_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    
    def get_all_datasets(self, status: str = 'active') -> List[Dict]:
        """Get all datasets"""
        cursor = self.conn.execute(
            "SELECT * FROM datasets WHERE status = ? ORDER BY created_at DESC",
            (status,)
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_dataset_catalog(self, dataset_id: int) -> Optional[Dict]:
        """Get dataset cache information"""
        cursor = self.conn.execute(
            "SELECT * FROM data_catalog WHERE dataset_id = ?", (dataset_id,)
        )
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None
    
    def get_quality_metrics(self, dataset_id: int) -> Optional[Dict]:
        """Get quality metrics for a dataset"""
        cursor = self.conn.execute(
            "SELECT * FROM quality_metrics WHERE dataset_id = ? ORDER BY created_at DESC LIMIT 1",
            (dataset_id,)
        )
        row = cursor.fetchone()
        if row:
            result = dict(row)
            result['issues'] = json.loads(result.get('issues_json', '[]'))
            return result
        return None
    
    def search_datasets(self, query: str) -> List[Dict]:
        """Search for datasets by name"""
        cursor = self.conn.execute(
            "SELECT * FROM datasets WHERE name LIKE ? OR file_path LIKE ? ORDER BY created_at DESC",
            (f"%{query}%", f"%{query}%")
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        cursor = self.conn.execute(
            "SELECT COUNT(*) as total_datasets, SUM(row_count) as total_rows FROM datasets WHERE status = 'active'"
        )
        row = cursor.fetchone()
        
        cursor = self.conn.execute(
            "SELECT SUM(file_size_mb) as total_size_mb FROM data_catalog"
        )
        size_row = cursor.fetchone()
        
        return {
            'total_datasets': row['total_datasets'] or 0,
            'total_rows': row['total_rows'] or 0,
            'total_size_mb': size_row['total_size_mb'] or 0
        }
    
    def update_quality_metrics(self, dataset_id: int, metrics: Dict):
        """Update quality metrics for a dataset"""
        self.conn.execute("""
        INSERT INTO quality_metrics (dataset_id, completeness, consistency, 
                                    timeliness, accuracy, overall_score, issues_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (dataset_id,
              metrics.get('completeness', 0),
              metrics.get('consistency', 0),
              metrics.get('timeliness', 0),
              metrics.get('accuracy', 0),
              metrics.get('overall_score', 0),
              json.dumps(metrics.get('issues', []))))
        
        self.conn.execute(
            "UPDATE datasets SET quality_score = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (metrics.get('overall_score', 0), dataset_id)
        )
        self.conn.commit()
    
    def delete_dataset(self, dataset_id: int) -> bool:
        """Delete a dataset"""
        try:
            self.conn.execute("UPDATE datasets SET status = 'deleted' WHERE id = ?", (dataset_id,))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting dataset: {e}")
            return False
    
    def cleanup_orphaned_entries(self) -> int:
        """Remove database entries for files that no longer exist in cache"""
        from pathlib import Path
        
        cache_dir = Path(__file__).parent.parent / "cache" / "disk_cache" / "data"
        removed_count = 0
        
        try:
            active_datasets = self.conn.execute(
                "SELECT id FROM data_catalog WHERE dataset_id IN (SELECT id FROM datasets WHERE status = 'active')"
            ).fetchall()
            
            for row in active_datasets:
                catalog_id = row['id']
                # Get the parquet path
                catalog = self.conn.execute(
                    "SELECT cache_key FROM data_catalog WHERE id = ?", (catalog_id,)
                ).fetchone()
                
                if catalog:
                    parquet_file = cache_dir / f"{catalog['cache_key']}.parquet"
                    
                    # If file doesn't exist, mark dataset as deleted
                    if not parquet_file.exists():
                        dataset_id = self.conn.execute(
                            "SELECT dataset_id FROM data_catalog WHERE id = ?", (catalog_id,)
                        ).fetchone()
                        
                        if dataset_id:
                            self.delete_dataset(dataset_id['dataset_id'])
                            removed_count += 1
            
            return removed_count
        except Exception as e:
            print(f"Error cleaning up orphaned entries: {e}")
            return 0
    
    def save_dataset_data(self, dataset_id: int, df: pd.DataFrame) -> bool:
        """Save dataframe directly to database as pickle"""
        try:
            import pickle
            data_pickle = pickle.dumps(df, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Insert or replace
            self.conn.execute("""
            INSERT OR REPLACE INTO dataset_data (dataset_id, data_pickle)
            VALUES (?, ?)
            """, (dataset_id, data_pickle))
            
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving dataset data: {e}")
            return False
    
    def load_dataset_data(self, dataset_id: int) -> Optional[pd.DataFrame]:
        """Load dataframe directly from database"""
        try:
            import pickle
            cursor = self.conn.execute(
                "SELECT data_pickle FROM dataset_data WHERE dataset_id = ?",
                (dataset_id,)
            )
            row = cursor.fetchone()
            
            if row:
                df = pickle.loads(row[0])
                return df
            return None
        except Exception as e:
            print(f"Error loading dataset data: {e}")
            return None
    
    def has_dataset_data(self, dataset_id: int) -> bool:
        """Check if dataset data exists in database"""
        try:
            cursor = self.conn.execute(
                "SELECT 1 FROM dataset_data WHERE dataset_id = ? LIMIT 1",
                (dataset_id,)
            )
            return cursor.fetchone() is not None
        except:
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

