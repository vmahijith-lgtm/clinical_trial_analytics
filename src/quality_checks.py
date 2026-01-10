"""
Quality Checks Module
Performs data quality assessments and identifies issues
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta
from utils.config import QUALITY_THRESHOLDS
from utils.helpers import calculate_completeness, detect_outliers

class QualityChecker:
    """Class to perform comprehensive data quality checks"""
    
    def __init__(self):
        self.thresholds = QUALITY_THRESHOLDS
        self.quality_report = {}
        
    def check_completeness(self, df: pd.DataFrame, critical_columns: List[str] = None) -> Dict:
        """Check data completeness"""
        completeness = calculate_completeness(df)
        
        # Check critical columns if specified
        critical_issues = []
        if critical_columns:
            for col in critical_columns:
                if col in df.columns:
                    col_completeness = 1 - (df[col].isna().sum() / len(df))
                    if col_completeness < self.thresholds['completeness_threshold']:
                        critical_issues.append({
                            'column': col,
                            'completeness': col_completeness,
                            'missing_count': df[col].isna().sum()
                        })
        
        return {
            'overall_completeness': completeness['overall_completeness'],
            'passes_threshold': completeness['overall_completeness'] >= self.thresholds['completeness_threshold'],
            'column_completeness': completeness['column_completeness'],
            'critical_issues': critical_issues
        }
    
    def check_consistency(self, df: pd.DataFrame) -> Dict:
        """Check data consistency across related fields"""
        issues = []
        
        # Check for duplicate IDs
        id_columns = [col for col in df.columns if 'id' in col.lower() and not col.startswith('_')]
        for col in id_columns:
            if df[col].duplicated().any():
                dup_count = df[col].duplicated().sum()
                issues.append({
                    'type': 'duplicate_ids',
                    'column': col,
                    'count': dup_count,
                    'severity': 'high'
                })
        
        # Check for inconsistent date ranges
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) >= 2:
            for i in range(len(date_cols)-1):
                for j in range(i+1, len(date_cols)):
                    col1, col2 = date_cols[i], date_cols[j]
                    # Check if dates are in logical order
                    invalid = df[col1] > df[col2]
                    if invalid.any():
                        issues.append({
                            'type': 'date_inconsistency',
                            'columns': f"{col1} > {col2}",
                            'count': invalid.sum(),
                            'severity': 'medium'
                        })
        
        # Check for inconsistent categorical values
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            unique_vals = df[col].dropna().unique()
            if 2 <= len(unique_vals) <= 20:  # Reasonable range for categories
                # Check for similar values (possible typos)
                similar_pairs = self._find_similar_values(unique_vals)
                if similar_pairs:
                    issues.append({
                        'type': 'similar_categorical_values',
                        'column': col,
                        'similar_pairs': similar_pairs,
                        'severity': 'low'
                    })
        
        consistency_score = 1 - (len(issues) / (len(df.columns) * 3))  # Normalized score
        
        return {
            'consistency_score': max(0, consistency_score),
            'passes_threshold': consistency_score >= self.thresholds['consistency_threshold'],
            'issues': issues
        }
    
    def check_timeliness(self, df: pd.DataFrame, date_column: str = None) -> Dict:
        """Check if data is timely"""
        date_cols = df.select_dtypes(include=['datetime64']).columns
        
        if date_column and date_column in df.columns:
            target_col = date_column
        elif len(date_cols) > 0:
            target_col = date_cols[0]
        else:
            return {
                'timeliness_score': None,
                'passes_threshold': None,
                'message': 'No date columns found'
            }
        
        # Calculate days since last update
        latest_date = df[target_col].max()
        if pd.isna(latest_date):
            return {
                'timeliness_score': 0,
                'passes_threshold': False,
                'message': 'No valid dates found'
            }
        
        days_old = (pd.Timestamp.now() - latest_date).days
        
        # Score based on recency
        timeliness_score = max(0, 1 - (days_old / (self.thresholds['timeliness_days'] * 4)))
        
        return {
            'timeliness_score': timeliness_score,
            'passes_threshold': days_old <= self.thresholds['timeliness_days'],
            'latest_date': latest_date,
            'days_since_update': days_old,
            'date_column': target_col
        }
    
    def check_accuracy(self, df: pd.DataFrame) -> Dict:
        """Check data accuracy through outlier detection and validation"""
        issues = []
        
        # Check numeric columns for outliers
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            outliers = detect_outliers(df[col], method='iqr')
            if outliers.any():
                issues.append({
                    'type': 'numeric_outliers',
                    'column': col,
                    'count': outliers.sum(),
                    'outlier_values': df[col][outliers].tolist()[:5],  # Sample
                    'severity': 'medium'
                })
        
        # Check for invalid values (negative values where not expected)
        for col in numeric_cols:
            if 'count' in col.lower() or 'age' in col.lower() or 'duration' in col.lower():
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append({
                        'type': 'invalid_negative_values',
                        'column': col,
                        'count': negative_count,
                        'severity': 'high'
                    })
        
        accuracy_score = 1 - (len(issues) / (len(numeric_cols) * 2)) if numeric_cols.any() else 1
        
        return {
            'accuracy_score': max(0, accuracy_score),
            'issues': issues
        }
    
    def _find_similar_values(self, values: np.ndarray, threshold: float = 0.8) -> List[Tuple[str, str]]:
        """Find similar values that might be typos"""
        from difflib import SequenceMatcher
        
        similar_pairs = []
        values_str = [str(v).lower() for v in values]
        
        for i in range(len(values_str)):
            for j in range(i+1, len(values_str)):
                similarity = SequenceMatcher(None, values_str[i], values_str[j]).ratio()
                if threshold <= similarity < 1.0:
                    similar_pairs.append((values[i], values[j]))
        
        return similar_pairs[:5]  # Return top 5
    
    def generate_comprehensive_report(self, df: pd.DataFrame, name: str = "Dataset") -> Dict:
        """Generate a comprehensive quality report"""
        report = {
            'dataset_name': name,
            'timestamp': datetime.now(),
            'row_count': len(df),
            'column_count': len(df.columns),
            'completeness': self.check_completeness(df),
            'consistency': self.check_consistency(df),
            'timeliness': self.check_timeliness(df),
            'accuracy': self.check_accuracy(df)
        }
        
        # Calculate overall quality score
        scores = [
            report['completeness']['overall_completeness'],
            report['consistency']['consistency_score'],
            report['timeliness']['timeliness_score'] if report['timeliness']['timeliness_score'] else 0.5,
            report['accuracy']['accuracy_score']
        ]
        
        report['overall_quality_score'] = np.mean(scores)
        report['overall_status'] = self._get_status(report['overall_quality_score'])
        
        return report
    
    def _get_status(self, score: float) -> str:
        """Get status based on score"""
        if score >= 0.9:
            return "Excellent"
        elif score >= 0.75:
            return "Good"
        elif score >= 0.6:
            return "Fair"
        else:
            return "Needs Improvement"
    
    def prioritize_issues(self, quality_reports: List[Dict]) -> pd.DataFrame:
        """Prioritize quality issues across all datasets"""
        all_issues = []
        
        for report in quality_reports:
            dataset_name = report.get('dataset_name', 'Unknown')
            
            # Collect consistency issues
            for issue in report.get('consistency', {}).get('issues', []):
                all_issues.append({
                    'dataset': dataset_name,
                    'category': 'Consistency',
                    'type': issue['type'],
                    'severity': issue['severity'],
                    'details': str(issue),
                    'priority': self._calculate_priority(issue['severity'], report['overall_quality_score'])
                })
            
            # Collect accuracy issues
            for issue in report.get('accuracy', {}).get('issues', []):
                all_issues.append({
                    'dataset': dataset_name,
                    'category': 'Accuracy',
                    'type': issue['type'],
                    'severity': issue['severity'],
                    'details': str(issue),
                    'priority': self._calculate_priority(issue['severity'], report['overall_quality_score'])
                })
        
        df_issues = pd.DataFrame(all_issues)
        if not df_issues.empty:
            df_issues = df_issues.sort_values('priority', ascending=False)
        
        return df_issues
    
    def _calculate_priority(self, severity: str, overall_score: float) -> int:
        """Calculate priority score for an issue"""
        severity_score = {'high': 3, 'medium': 2, 'low': 1}.get(severity, 1)
        quality_factor = (1 - overall_score) * 10
        return int(severity_score * quality_factor)