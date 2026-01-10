"""
AI Insights Module
Uses Claude API to generate intelligent insights and recommendations
"""

import json
import logging
from typing import Dict, List
import os
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from multiple locations
load_dotenv()

class AIInsightsEngine:
    """Class to generate AI-powered insights using Claude"""
    
    def __init__(self):
        """Initialize the AI insights engine with proper error handling."""
        self.client = None
        self.api_key_configured = False
        
        # Try multiple ways to get the API key
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        
        # Also check Streamlit secrets if available
        try:
            import streamlit as st
            if 'ANTHROPIC_API_KEY' in st.secrets:
                api_key = st.secrets['ANTHROPIC_API_KEY']
                logger.info("Using API key from Streamlit secrets")
        except Exception:
            pass  # Not in Streamlit context
        
        if not api_key:
            logger.warning("Anthropic API key not found in environment variables or Streamlit secrets. AI insights will be limited.")
            self.client = None
            self.api_key_configured = False
        else:
            try:
                from anthropic import Anthropic
                self.client = Anthropic(api_key=api_key)
                self.api_key_configured = True
                logger.info("Successfully initialized Anthropic client")
            except ImportError as e:
                logger.error(f"Failed to import Anthropic SDK: {str(e)}. Please install with: pip install anthropic")
                self.client = None
            except Exception as e:
                logger.error(f"Error initializing Anthropic client: {str(e)}")
                self.client = None
    
    def is_available(self) -> bool:
        """Check if the AI engine is properly configured and available."""
        return self.client is not None and self.api_key_configured
    
    def generate_quality_insights(self, quality_report: Dict) -> str:
        """Generate insights from quality report"""
        
        if not self.client:
            return "AI insights unavailable: API key not configured or client initialization failed"
        
        prompt = f"""Analyze this clinical trial data quality report and provide:
1. Key findings (3-5 bullet points)
2. Critical issues that need immediate attention
3. Recommendations for improvement
4. Risk assessment

Quality Report:
- Overall Quality Score: {quality_report.get('overall_quality_score', 0):.2%}
- Completeness: {quality_report.get('completeness', {}).get('overall_completeness', 0):.2%}
- Consistency Score: {quality_report.get('consistency', {}).get('consistency_score', 0):.2%}
- Issues Found: {len(quality_report.get('consistency', {}).get('issues', []))}

Be concise, actionable, and focus on clinical trial operations."""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Error generating insights: {str(e)}"
    
    def generate_enrollment_insights(self, enrollment_data: Dict) -> str:
        """Generate insights about enrollment patterns"""
        
        if not self.client:
            return "AI insights unavailable: API key not configured or client initialization failed"
        
        prompt = f"""Analyze this clinical trial enrollment data:

Total Subjects: {enrollment_data.get('total_subjects', 0)}
Enrollment Rate: {enrollment_data.get('enrollment_rate_per_month', 0):.1f} subjects/month
Duration: {enrollment_data.get('date_range_days', 0)} days
Sites: {len(enrollment_data.get('enrollment_by_site', {}))}

Provide:
1. Assessment of enrollment pace
2. Site performance insights
3. Potential concerns or risks
4. Actionable recommendations

Be specific and data-driven."""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Error generating insights: {str(e)}"
    
    def generate_bottleneck_recommendations(self, bottlenecks: List[Dict]) -> str:
        """Generate recommendations for identified bottlenecks"""
        
        if not self.client:
            return "AI insights unavailable: API key not configured or client initialization failed"
        
        if not bottlenecks:
            return "No significant bottlenecks identified."
        
        bottleneck_summary = "\n".join([
            f"- {b['type']}: {b.get('affected_records', 'N/A')} records affected, severity: {b.get('severity', 'unknown')}"
            for b in bottlenecks
        ])
        
        prompt = f"""Analyze these operational bottlenecks in a clinical trial:

{bottleneck_summary}

For each bottleneck:
1. Root cause analysis
2. Immediate actions
3. Long-term solutions
4. Impact on trial timeline

Prioritize by severity and impact."""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1200,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Error generating insights: {str(e)}"
    
    def chat_with_data(self, question: str, context: Dict) -> str:
        """Interactive Q&A about the clinical trial data"""
        
        if not self.client:
            return "AI chat unavailable: API key not configured or client initialization failed"
        
        # Prepare context summary
        context_str = json.dumps(context, indent=2, default=str)[:2000]  # Limit context size
        
        prompt = f"""You are an AI assistant helping analyze clinical trial data.

Data Context:
{context_str}

User Question: {question}

Provide a helpful, accurate response based on the available data. If you need more specific data to answer, explain what's needed."""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Error: {str(e)}"
    
    def generate_executive_summary(self, all_insights: Dict) -> str:
        """Generate an executive summary of all findings"""
        
        if not self.client:
            return "AI insights unavailable: API key not configured or client initialization failed"
        
        summary_data = json.dumps(all_insights, indent=2, default=str)[:3000]
        
        prompt = f"""Create an executive summary for clinical trial stakeholders based on this data analysis:

{summary_data}

Include:
1. Overall trial health status
2. Top 3 priorities requiring attention
3. Key performance indicators
4. Strategic recommendations
5. Risk assessment

Format for executive audience - clear, concise, actionable."""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Error generating summary: {str(e)}"
    
    def predict_trends(self, historical_data: Dict) -> str:
        """Predict future trends based on historical data"""
        
        if not self.client:
            return "AI insights unavailable: API key not configured or client initialization failed"
        
        data_summary = json.dumps(historical_data, indent=2, default=str)[:2000]
        
        prompt = f"""Based on this historical clinical trial data, predict future trends:

{data_summary}

Provide:
1. Enrollment projections (next 3-6 months)
2. Expected quality score trends
3. Potential risk areas
4. Resource allocation recommendations

Be realistic and data-driven."""
        
        try:
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=1200,
                messages=[{"role": "user", "content": prompt}]
            )
            
            return response.content[0].text
            
        except Exception as e:
            return f"Error generating predictions: {str(e)}"
    
    
