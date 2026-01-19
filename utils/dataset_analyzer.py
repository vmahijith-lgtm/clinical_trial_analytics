"""
Dataset Name Analyzer for Clinical Trial Data
Extracts crucial insights from dataset names and provides categorization
"""

import re
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import pandas as pd


class DatasetNameAnalyzer:
    """
    Analyzes clinical trial dataset names to extract key insights
    """

    def __init__(self):
        # Define data categories and their patterns
        self.data_categories = {
            'safety': {
                'patterns': [
                    r'esae.*dashboard', r'sae.*dashboard', r'adverse.*event',
                    r'safety.*report', r'dm.*safety'
                ],
                'description': 'Safety and Adverse Event Data',
                'icon': '🛡️',
                'color': '#e74c3c'
            },
            'metrics': {
                'patterns': [
                    r'edc.*metrics', r'cpid.*metrics', r'metrics.*urs'
                ],
                'description': 'Electronic Data Capture Metrics',
                'icon': '📊',
                'color': '#3498db'
            },
            'coding': {
                'patterns': [
                    r'globalcodingreport', r'meddra', r'whodd',
                    r'coding.*report', r'global.*coding'
                ],
                'description': 'Medical Coding and Dictionaries',
                'icon': '🏷️',
                'color': '#9b59b6'
            },
            'quality': {
                'patterns': [
                    r'missing.*pages', r'missing.*lab', r'missing.*ranges',
                    r'inactivated', r'quality', r'qc'
                ],
                'description': 'Data Quality and Completeness',
                'icon': '✅',
                'color': '#27ae60'
            },
            'operational': {
                'patterns': [
                    r'visit.*projection', r'visit.*tracker', r'compiled.*edrr',
                    r'edrr', r'projection.*tracker'
                ],
                'description': 'Operational and Study Management',
                'icon': '📋',
                'color': '#f39c12'
            },
            'other': {
                'patterns': [],
                'description': 'Other Data Types',
                'icon': '📁',
                'color': '#95a5a6'
            }
        }

        # Version patterns
        self.version_patterns = [
            r'ursv(\d+\.\d+)',  # URSV2.0, URSV3.0
            r'v(\d+\.\d+)',     # v1.0, v2.0
            r'version\s*(\d+)', # version 1, version 2
        ]

        # Date patterns
        self.date_patterns = [
            r'(\d{1,2})\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*(\d{4})',  # 14 NOV 2025
            r'(\d{1,2})(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(\d{4})',  # 13NOV2025
            r'(\d{4})_(\w{3})_(\d{1,2})',  # 2025_Oct_27
            r'(\d{4})[\-_](\d{1,2})[\-_](\d{1,2})',  # 2025-11-14
        ]

    def extract_study_number(self, filename: str) -> Optional[str]:
        """Extract study number from filename"""
        # Pattern: Study X_ or STUDY X_
        study_match = re.search(r'(?:study|STUDY)\s*(\d+)', filename, re.IGNORECASE)
        if study_match:
            return f"Study {study_match.group(1)}"
        return None

    def extract_data_category(self, filename: str) -> Tuple[str, Dict]:
        """Extract data category and metadata"""
        filename_lower = filename.lower()

        for category, info in self.data_categories.items():
            if category == 'other':
                continue

            for pattern in info['patterns']:
                if re.search(pattern, filename_lower):
                    return category, info

        # Default to other
        return 'other', self.data_categories['other']

    def extract_version_info(self, filename: str) -> Optional[str]:
        """Extract version information"""
        for pattern in self.version_patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def extract_date_info(self, filename: str) -> Optional[str]:
        """Extract date information"""
        for pattern in self.date_patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    # Try to format as date
                    try:
                        if len(groups[0]) == 4:  # YYYY format
                            return f"{groups[0]}-{groups[1]}-{groups[2]}"
                        else:  # DD Mon YYYY format
                            month_map = {
                                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
                            }
                            month = month_map.get(groups[1].lower(), groups[1])
                            return f"{groups[2]}-{month}-{groups[0].zfill(2)}"
                    except:
                        pass
        return None

    def is_updated(self, filename: str) -> bool:
        """Check if file has update indicator"""
        return '_updated' in filename.lower()

    def extract_subtype(self, filename: str, category: str) -> str:
        """Extract more specific subtype within category"""
        filename_lower = filename.lower()

        if category == 'safety':
            if 'esae' in filename_lower:
                return 'eSAE Dashboard'
            elif 'sae' in filename_lower:
                return 'SAE Dashboard'
            else:
                return 'Safety Report'

        elif category == 'metrics':
            if 'edc' in filename_lower:
                return 'EDC Metrics'
            elif 'cpid' in filename_lower:
                return 'CPID Metrics'
            else:
                return 'Study Metrics'

        elif category == 'coding':
            if 'meddra' in filename_lower:
                return 'MedDRA Coding'
            elif 'whodd' in filename_lower:
                return 'WHODD Coding'
            else:
                return 'Medical Coding'

        elif category == 'quality':
            if 'missing' in filename_lower and 'pages' in filename_lower:
                return 'Missing Pages'
            elif 'missing' in filename_lower and ('lab' in filename_lower or 'ranges' in filename_lower):
                return 'Missing Lab Data'
            elif 'inactivated' in filename_lower:
                return 'Inactivated Records'
            else:
                return 'Quality Report'

        elif category == 'operational':
            if 'visit' in filename_lower:
                return 'Visit Tracking'
            elif 'edrr' in filename_lower:
                return 'EDRR Report'
            else:
                return 'Operational Data'

        return 'General'

    def analyze_dataset_name(self, dataset_name: str) -> Dict:
        """
        Comprehensive analysis of a dataset name
        Returns dictionary with all extracted insights
        """
        # Remove .xlsx extension and sheet name for analysis
        clean_name = dataset_name.replace('.xlsx', '').replace('.xls', '')

        # Split by underscore to get components
        parts = clean_name.split('_')

        # Extract study number
        study_number = self.extract_study_number(clean_name)

        # Extract category
        category, category_info = self.extract_data_category(clean_name)

        # Extract version
        version = self.extract_version_info(clean_name)

        # Extract date
        date_info = self.extract_date_info(clean_name)

        # Check if updated
        is_updated = self.is_updated(clean_name)

        # Extract subtype
        subtype = self.extract_subtype(clean_name, category)

        # Create insights summary
        insights = {
            'dataset_name': dataset_name,
            'study_number': study_number,
            'category': category,
            'category_description': category_info['description'],
            'category_icon': category_info['icon'],
            'category_color': category_info['color'],
            'subtype': subtype,
            'version': version,
            'date_info': date_info,
            'is_updated': is_updated,
            'data_freshness': 'Current' if is_updated else 'Historical',
            'key_insights': []
        }

        # Generate key insights
        insights_list = []

        if study_number:
            insights_list.append(f"📊 {study_number} data")

        insights_list.append(f"{category_info['icon']} {category_info['description']}")

        if subtype != 'General':
            insights_list.append(f"📋 {subtype}")

        if version:
            insights_list.append(f"🔢 Version {version}")

        if date_info:
            insights_list.append(f"📅 Updated {date_info}")

        if is_updated:
            insights_list.append("✅ Recently updated")

        insights['key_insights'] = insights_list

        return insights

    def analyze_multiple_datasets(self, dataset_names: List[str]) -> List[Dict]:
        """Analyze multiple dataset names"""
        return [self.analyze_dataset_name(name) for name in dataset_names]

    def get_category_summary(self, analyses: List[Dict]) -> Dict:
        """Get summary statistics by category"""
        summary = {}

        for analysis in analyses:
            category = analysis['category']
            if category not in summary:
                summary[category] = {
                    'count': 0,
                    'studies': set(),
                    'description': analysis['category_description'],
                    'icon': analysis['category_icon'],
                    'color': analysis['category_color']
                }

            summary[category]['count'] += 1
            if analysis['study_number']:
                summary[category]['studies'].add(analysis['study_number'])

        # Convert sets to lists for JSON serialization
        for cat in summary:
            summary[cat]['studies'] = list(summary[cat]['studies'])

        return summary

    def get_study_summary(self, analyses: List[Dict]) -> Dict:
        """Get summary statistics by study"""
        summary = {}

        for analysis in analyses:
            study = analysis['study_number']
            if not study:
                continue

            if study not in summary:
                summary[study] = {
                    'total_datasets': 0,
                    'categories': {},
                    'has_updates': False,
                    'latest_date': None
                }

            summary[study]['total_datasets'] += 1

            # Track categories
            cat = analysis['category']
            summary[study]['categories'][cat] = summary[study]['categories'].get(cat, 0) + 1

            # Track updates
            if analysis['is_updated']:
                summary[study]['has_updates'] = True

            # Track latest date
            if analysis['date_info'] and (not summary[study]['latest_date'] or
                                        analysis['date_info'] > summary[study]['latest_date']):
                summary[study]['latest_date'] = analysis['date_info']

        return summary


# Global analyzer instance
dataset_analyzer = DatasetNameAnalyzer()
