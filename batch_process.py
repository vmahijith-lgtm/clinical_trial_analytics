"""
Batch Processing Script - Process ALL data in manageable chunks
Analyzes complete datasets without memory issues
"""

import pandas as pd
from pathlib import Path
import json
import sys
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

sys.path.append(str(Path(__file__).parent))

from src.data_ingestion import DataIngestion
from src.quality_checks import QualityChecker
from src.analytics_engine import AnalyticsEngine
from utils.config import DATA_DIR
from utils.helpers import standardize_column_names, convert_date_columns

class BatchProcessor:
    """Process large datasets in batches"""
    
    def __init__(self, data_dir=DATA_DIR, batch_size=10):
        self.data_dir = Path(data_dir)
        self.batch_size = batch_size
        self.results = {
            'processing_started': datetime.now().isoformat(),
            'files_processed': 0,
            'sheets_processed': 0,
            'total_rows': 0,
            'total_columns': 0,
            'quality_reports': [],
            'analytics': {},
            'errors': []
        }
        self.current_batch = 0
    
    def process_all_data(self):
        """Process all files in batches"""
        print("\n" + "="*70)
        print("🔬 CLINICAL TRIAL ANALYTICS - BATCH PROCESSOR")
        print("="*70)
        
        print("\n🔍 Discovering files...")
        ingestion = DataIngestion(self.data_dir)
        all_files = ingestion.discover_files()
        
        if not all_files:
            print(f"❌ No files found in {self.data_dir}")
            return None
        
        print(f"✅ Found {len(all_files)} files")
        
        # Show file list
        print("\n📋 Files to process:")
        for i, f in enumerate(all_files[:10], 1):
            print(f"  {i}. {f.name}")
        if len(all_files) > 10:
            print(f"  ... and {len(all_files) - 10} more files")
        
        total_size = sum(f.stat().st_size for f in all_files) / (1024 * 1024)
        print(f"\n📦 Total size: {total_size:.1f} MB")
        print(f"🔄 Processing in batches of {self.batch_size} files")
        
        # Confirm processing
        response = input(f"\n⚠️  Process all {len(all_files)} files? (yes/no): ").lower()
        if response != 'yes':
            print("❌ Processing cancelled")
            return None
        
        # Process in batches
        start_time = datetime.now()
        
        for i in range(0, len(all_files), self.batch_size):
            batch = all_files[i:i + self.batch_size]
            self.current_batch = (i // self.batch_size) + 1
            total_batches = (len(all_files) + self.batch_size - 1) // self.batch_size
            
            print(f"\n{'='*70}")
            print(f"📊 BATCH {self.current_batch}/{total_batches}")
            print(f"{'='*70}")
            
            self.process_batch(batch)
            
            # Save intermediate results
            self.save_results(f"batch_{self.current_batch}")
            
            print(f"\n✅ Batch {self.current_batch} complete!")
            print(f"   Files processed: {len(batch)}")
            print(f"   Total rows so far: {self.results['total_rows']:,}")
        
        # Calculate processing time
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.results['processing_ended'] = end_time.isoformat()
        self.results['duration_seconds'] = duration
        
        # Save final results
        self.save_results("final")
        self.generate_summary_report()
        
        print("\n" + "="*70)
        print("✅ ALL DATA PROCESSED SUCCESSFULLY!")
        print("="*70)
        print(f"⏱️  Duration: {duration/60:.1f} minutes")
        print(f"📊 Files: {self.results['files_processed']}")
        print(f"📄 Sheets: {self.results['sheets_processed']}")
        print(f"📈 Total rows: {self.results['total_rows']:,}")
        print(f"💾 Results saved in: batch_results/")
        
        return self.results
    
    def process_batch(self, files):
        """Process a batch of files"""
        checker = QualityChecker()
        analytics = AnalyticsEngine()
        
        try:
            from tqdm import tqdm
            file_iterator = tqdm(files, desc="Processing files")
        except ImportError:
            file_iterator = files
            print("Processing files...")
        
        for file_path in file_iterator:
            try:
                # Read file
                excel_file = pd.ExcelFile(file_path, engine='openpyxl')
                
                for sheet_name in excel_file.sheet_names:
                    try:
                        df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
                        
                        if df.empty or len(df.columns) == 0:
                            continue
                        
                        # Standardize
                        df = standardize_column_names(df)
                        df = convert_date_columns(df)
                        
                        # Update stats
                        self.results['files_processed'] += 1
                        self.results['sheets_processed'] += 1
                        self.results['total_rows'] += len(df)
                        self.results['total_columns'] += len(df.columns)
                        
                        # Quality check
                        report = checker.generate_comprehensive_report(
                            df, 
                            name=f"{file_path.name}/{sheet_name}"
                        )
                        self.results['quality_reports'].append({
                            'file': file_path.name,
                            'sheet': sheet_name,
                            'quality_score': report['overall_quality_score'],
                            'status': report['overall_status'],
                            'completeness': report['completeness']['overall_completeness'],
                            'rows': len(df),
                            'columns': len(df.columns)
                        })
                        
                        # Analytics
                        insights = analytics.generate_insights(df, name=sheet_name)
                        
                        # Store compact analytics
                        self.results['analytics'][f"{file_path.name}/{sheet_name}"] = {
                            'rows': insights['basic_stats']['rows'],
                            'columns': insights['basic_stats']['columns'],
                            'has_enrollment': 'enrollment' in insights,
                            'has_bottlenecks': len(insights.get('bottlenecks', [])) > 0,
                            'bottleneck_count': len(insights.get('bottlenecks', []))
                        }
                        
                        if not isinstance(file_iterator, list):
                            file_iterator.set_postfix({
                                'sheet': sheet_name[:20],
                                'rows': f"{len(df):,}"
                            })
                        else:
                            print(f"  ✓ {file_path.name}/{sheet_name}: {len(df):,} rows")
                        
                        # Clean up memory
                        del df
                        
                    except Exception as e:
                        error_msg = f"Error in {file_path.name}/{sheet_name}: {str(e)}"
                        self.results['errors'].append(error_msg)
                        print(f"  ✗ {error_msg}")
                
                # Clean up
                excel_file.close()
                del excel_file
                
            except Exception as e:
                error_msg = f"Error reading {file_path.name}: {str(e)}"
                self.results['errors'].append(error_msg)
                print(f"  ✗ {error_msg}")
    
    def save_results(self, suffix):
        """Save results to JSON"""
        output_dir = Path("batch_results")
        output_dir.mkdir(exist_ok=True)
        
        # Save full results
        with open(output_dir / f"full_results_{suffix}.json", 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        # Save quality reports as CSV
        if self.results['quality_reports']:
            df_quality = pd.DataFrame(self.results['quality_reports'])
            df_quality.to_csv(output_dir / f"quality_reports_{suffix}.csv", index=False)
        
        # Save analytics summary
        if self.results['analytics']:
            df_analytics = pd.DataFrame(self.results['analytics']).T
            df_analytics.to_csv(output_dir / f"analytics_{suffix}.csv")
    
    def generate_summary_report(self):
        """Generate human-readable summary"""
        output_dir = Path("batch_results")
        
        summary = f"""
{'='*70}
CLINICAL TRIAL ANALYTICS - PROCESSING SUMMARY
{'='*70}

PROCESSING DETAILS
------------------
Started:  {self.results['processing_started']}
Ended:    {self.results['processing_ended']}
Duration: {self.results['duration_seconds']/60:.1f} minutes

DATA PROCESSED
--------------
Files:    {self.results['files_processed']}
Sheets:   {self.results['sheets_processed']}
Rows:     {self.results['total_rows']:,}
Columns:  {self.results['total_columns']:,}

QUALITY ANALYSIS
----------------
Total Reports: {len(self.results['quality_reports'])}
"""
        
        if self.results['quality_reports']:
            quality_df = pd.DataFrame(self.results['quality_reports'])
            avg_quality = quality_df['quality_score'].mean()
            
            summary += f"""Average Quality Score: {avg_quality:.1%}

Quality Distribution:
"""
            status_counts = quality_df['status'].value_counts()
            for status, count in status_counts.items():
                summary += f"  - {status}: {count}\n"
            
            summary += f"""
Top 5 Quality Datasets:
"""
            top_5 = quality_df.nlargest(5, 'quality_score')[['file', 'sheet', 'quality_score']]
            for idx, row in top_5.iterrows():
                summary += f"  {row['quality_score']:.1%} - {row['file']}/{row['sheet']}\n"
            
            summary += f"""
Lowest 5 Quality Datasets:
"""
            bottom_5 = quality_df.nsmallest(5, 'quality_score')[['file', 'sheet', 'quality_score']]
            for idx, row in bottom_5.iterrows():
                summary += f"  {row['quality_score']:.1%} - {row['file']}/{row['sheet']}\n"
        
        if self.results['errors']:
            summary += f"""
ERRORS ({len(self.results['errors'])})
-------
"""
            for error in self.results['errors'][:10]:
                summary += f"  - {error}\n"
            if len(self.results['errors']) > 10:
                summary += f"  ... and {len(self.results['errors']) - 10} more errors\n"
        
        summary += f"""
{'='*70}
Results saved in: batch_results/
  - full_results_final.json     (Complete data)
  - quality_reports_final.csv   (Quality analysis)
  - analytics_final.csv         (Analytics summary)
  - summary_report.txt          (This file)
{'='*70}
"""
        
        # Save summary
        with open(output_dir / "summary_report.txt", 'w') as f:
            f.write(summary)
        
        print(summary)

def main():
    print("="*70)
    print("Clinical Trial Analytics - Batch Processor")
    print("="*70)
    
    # Configuration
    print("\nConfiguration:")
    batch_size = input("  Batch size (files per batch, default 10): ").strip()
    batch_size = int(batch_size) if batch_size else 10
    
    data_path = input(f"  Data directory (default: {DATA_DIR}): ").strip()
    data_path = Path(data_path) if data_path else DATA_DIR
    
    print(f"\n✅ Configuration:")
    print(f"   Batch size: {batch_size}")
    print(f"   Data path: {data_path}")
    
    processor = BatchProcessor(data_dir=data_path, batch_size=batch_size)
    results = processor.process_all_data()
    
    if results:
        print("\n✅ Processing complete! Check batch_results/ folder for outputs.")
        print("\n💡 Next steps:")
        print("   1. Review: cat batch_results/summary_report.txt")
        print("   2. View quality: open batch_results/quality_reports_final.csv")
        print("   3. Visualize: streamlit run Home.py")

if __name__ == "__main__":
    # Check dependencies
    try:
        from tqdm import tqdm
    except ImportError:
        print("📦 Installing tqdm for progress bars...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm", "-q"])
        print("✅ tqdm installed")
    
    main()