"""
Analytics Engine Module
Performs advanced analytics on clinical trial data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from scipy import stats

class AnalyticsEngine:
    """Class to perform advanced analytics on clinical trial data"""
    
    def __init__(self):
        self.analytics_cache = {}
        
    def enrollment_analysis(self, df: pd.DataFrame, 
                           subject_col: str = 'subject_id',
                           date_col: str = None) -> Dict:
        """Analyze enrollment patterns"""
        
        # Find date column if not specified
        if date_col is None:
            date_cols = df.select_dtypes(include=['datetime64']).columns
            date_col = date_cols[0] if len(date_cols) > 0 else None
        
        if date_col is None or subject_col not in df.columns:
            return {'error': 'Required columns not found'}
        
        # Total subjects
        total_subjects = df[subject_col].nunique()
        
        # Enrollment over time
        df_sorted = df.sort_values(date_col)
        df_sorted['cumulative_enrollment'] = range(1, len(df_sorted) + 1)
        
        # Enrollment rate (subjects per month)
        date_range = (df[date_col].max() - df[date_col].min()).days / 30
        enrollment_rate = total_subjects / date_range if date_range > 0 else 0
        
        # Group by site if available
        site_col = next((col for col in df.columns if 'site' in col.lower()), None)
        site_enrollment = {}
        if site_col:
            site_enrollment = df.groupby(site_col)[subject_col].nunique().to_dict()
        
        return {
            'total_subjects': total_subjects,
            'enrollment_rate_per_month': enrollment_rate,
            'date_range_days': (df[date_col].max() - df[date_col].min()).days,
            'first_enrollment': df[date_col].min(),
            'last_enrollment': df[date_col].max(),
            'enrollment_by_site': site_enrollment,
            'enrollment_trend': df_sorted[[date_col, 'cumulative_enrollment']].copy()
        }
    
    def site_performance_analysis(self, df: pd.DataFrame) -> Dict:
        """Analyze site performance metrics"""
        
        site_col = next((col for col in df.columns if 'site' in col.lower()), None)
        
        if site_col is None:
            return {'error': 'No site column found'}
        
        # Subjects per site
        subjects_per_site = df.groupby(site_col).size()
        
        # Calculate metrics
        performance = {
            'total_sites': df[site_col].nunique(),
            'avg_subjects_per_site': subjects_per_site.mean(),
            'median_subjects_per_site': subjects_per_site.median(),
            'top_performing_sites': subjects_per_site.nlargest(5).to_dict(),
            'underperforming_sites': subjects_per_site.nsmallest(5).to_dict(),
            'site_distribution': subjects_per_site.to_dict()
        }
        
        # Site efficiency (if date columns available)
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) > 0:
            date_col = date_cols[0]
            site_timeline = df.groupby(site_col)[date_col].agg(['min', 'max'])
            site_timeline['duration_days'] = (site_timeline['max'] - site_timeline['min']).dt.days
            performance['site_duration'] = site_timeline['duration_days'].to_dict()
        
        return performance
    
    def adverse_events_analysis(self, df: pd.DataFrame) -> Dict:
        """Analyze adverse events patterns"""
        
        # Look for AE-related columns
        ae_col = next((col for col in df.columns if 'ae' in col.lower() or 'adverse' in col.lower()), None)
        severity_col = next((col for col in df.columns if 'severity' in col.lower() or 'grade' in col.lower()), None)
        
        if ae_col is None:
            return {'error': 'No adverse event column found', 'data_available': False}
        
        analysis = {
            'data_available': True,
            'total_events': len(df),
            'unique_event_types': df[ae_col].nunique() if ae_col else 0
        }
        
        # Most common events
        if ae_col:
            event_counts = df[ae_col].value_counts()
            analysis['most_common_events'] = event_counts.head(10).to_dict()
        
        # Severity distribution
        if severity_col:
            severity_dist = df[severity_col].value_counts()
            analysis['severity_distribution'] = severity_dist.to_dict()
        
        # Events by site
        site_col = next((col for col in df.columns if 'site' in col.lower()), None)
        if site_col and ae_col:
            events_by_site = df.groupby(site_col)[ae_col].count()
            analysis['events_by_site'] = events_by_site.to_dict()
        
        # Temporal trends
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) > 0:
            date_col = date_cols[0]
            df['month'] = pd.to_datetime(df[date_col]).dt.to_period('M')
            monthly_events = df.groupby('month').size()
            analysis['monthly_trend'] = monthly_events.to_dict()
        
        return analysis
    
    def data_quality_trends(self, quality_reports: List[Dict]) -> Dict:
        """Analyze trends in data quality over time"""
        
        if not quality_reports:
            return {'error': 'No quality reports available'}
        
        # Extract scores
        timestamps = [r['timestamp'] for r in quality_reports]
        overall_scores = [r['overall_quality_score'] for r in quality_reports]
        completeness_scores = [r['completeness']['overall_completeness'] for r in quality_reports]
        consistency_scores = [r['consistency']['consistency_score'] for r in quality_reports]
        
        return {
            'average_quality_score': np.mean(overall_scores),
            'quality_trend': 'improving' if len(overall_scores) > 1 and overall_scores[-1] > overall_scores[0] else 'stable',
            'best_quality_dataset': quality_reports[np.argmax(overall_scores)]['dataset_name'],
            'worst_quality_dataset': quality_reports[np.argmin(overall_scores)]['dataset_name'],
            'completeness_avg': np.mean(completeness_scores),
            'consistency_avg': np.mean(consistency_scores)
        }
    
    def identify_bottlenecks(self, df: pd.DataFrame) -> List[Dict]:
        """Identify operational bottlenecks in the trial"""
        
        bottlenecks = []
        
        # Check for data entry delays
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) >= 2:
            # Calculate time differences between sequential dates
            for i in range(len(date_cols) - 1):
                col1, col2 = date_cols[i], date_cols[i+1]
                df['time_diff'] = (df[col2] - df[col1]).dt.days
                
                # Identify delays > 30 days
                delays = df[df['time_diff'] > 30]
                if len(delays) > 0:
                    bottlenecks.append({
                        'type': 'processing_delay',
                        'between': f"{col1} and {col2}",
                        'affected_records': len(delays),
                        'avg_delay_days': delays['time_diff'].mean(),
                        'severity': 'high' if len(delays) > len(df) * 0.2 else 'medium'
                    })
        
        # Check for incomplete records
        completeness_by_row = df.notna().sum(axis=1) / len(df.columns)
        incomplete_records = (completeness_by_row < 0.7).sum()
        
        if incomplete_records > len(df) * 0.1:
            bottlenecks.append({
                'type': 'data_completeness',
                'affected_records': incomplete_records,
                'percentage': (incomplete_records / len(df)) * 100,
                'severity': 'high'
            })
        
        # Check for unbalanced site distribution
        site_col = next((col for col in df.columns if 'site' in col.lower()), None)
        if site_col:
            site_counts = df[site_col].value_counts()
            if site_counts.std() / site_counts.mean() > 0.5:  # High variability
                bottlenecks.append({
                    'type': 'unbalanced_enrollment',
                    'site_column': site_col,
                    'cv': site_counts.std() / site_counts.mean(),
                    'severity': 'medium'
                })
        
        return bottlenecks
    
    def statistical_summary(self, df: pd.DataFrame) -> Dict:
        """Generate statistical summary of numeric columns"""
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        summary = {
            'numeric_columns': list(numeric_cols),
            'statistics': {}
        }
        
        for col in numeric_cols:
            col_data = df[col].dropna()
            
            if len(col_data) > 0:
                summary['statistics'][col] = {
                    'count': len(col_data),
                    'mean': col_data.mean(),
                    'std': col_data.std(),
                    'min': col_data.min(),
                    'q25': col_data.quantile(0.25),
                    'median': col_data.median(),
                    'q75': col_data.quantile(0.75),
                    'max': col_data.max(),
                    'skewness': stats.skew(col_data),
                    'kurtosis': stats.kurtosis(col_data)
                }
        
        return summary
    
    def correlation_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze correlations between numeric variables"""
        
        numeric_df = df.select_dtypes(include=[np.number])
        
        if numeric_df.shape[1] < 2:
            return pd.DataFrame()
        
        corr_matrix = numeric_df.corr()
        
        return corr_matrix
    
    def generate_insights(self, df: pd.DataFrame, name: str = "Dataset") -> Dict:
        """Generate comprehensive insights from the data"""
        
        insights = {
            'dataset_name': name,
            'basic_stats': {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            }
        }
        
        # Add enrollment analysis if applicable
        subject_col = next((col for col in df.columns if 'subject' in col.lower() or 'patient' in col.lower()), None)
        if subject_col:
            insights['enrollment'] = self.enrollment_analysis(df, subject_col)
        
        # Add site performance if applicable
        if any('site' in col.lower() for col in df.columns):
            insights['site_performance'] = self.site_performance_analysis(df)
        
        # Add bottleneck analysis
        insights['bottlenecks'] = self.identify_bottlenecks(df)
        
        # Add statistical summary
        insights['statistics'] = self.statistical_summary(df)
        
        return insights