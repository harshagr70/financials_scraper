import streamlit as st
import sys
import os
from pathlib import Path

# Add current directory to path to import production_code
sys.path.insert(0, str(Path(__file__).parent))

from production_code.orchestrator import AggregatedFinancialScraper
from production_code.merger_final import build_unified_catalog_all_statements
from production_code.orchestrator import parse_financial_value
from pydantic import BaseModel, Field
import openai
import pandas as pd

# Page configuration
st.set_page_config(
    page_title="Financial Data Scraper",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stDataFrame {
        border-radius: 10px;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Pydantic Schema for Structured Output (same as in production_code/llm_chat.py)
class FinancialQuerySchema(BaseModel):
    """Schema for extracting ticker and years from user query"""
    ticker: str = Field(
        description="Company stock ticker symbol (e.g., AAPL, MSFT, TSLA). Convert company names to tickers."
    )
    num_years: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of years of financial data to retrieve. Must be between 1 and 10. Default is 3 if not specified."
    )

def smart_financial_scraper_no_save(user_query: str, openai_api_key: str = None):
    """
    Wrapper that converts natural language query to ticker and scrapes data.
    Returns data without saving files.
    """
    if not openai_api_key:
        raise ValueError("OpenAI API key is required")
    
    # Initialize OpenAI client
    client = openai.OpenAI(api_key=openai_api_key)
    
    # Step 1: Extract ticker and years using OpenAI with structured output
    try:
        response = client.beta.chat.completions.parse(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are a financial data assistant. Extract the company ticker symbol and number of years from the user's query.

Rules:
- Convert company names to ticker symbols (Apple→AAPL, Microsoft→MSFT, Tesla→TSLA, Amazon→AMZN, Google→GOOGL, etc.)
- If years not specified, use 3
- Number of years must be between 1 and 10"""
                },
                {
                    "role": "user",
                    "content": user_query
                }
            ],
            response_format=FinancialQuerySchema
        )
        
        # Parse the validated response
        parsed_data = response.choices[0].message.parsed
        ticker = parsed_data.ticker
        num_years = parsed_data.num_years
        
    except Exception as e:
        raise Exception(f"Failed to parse query: {e}")
    
    # Step 2: Run the scraper (gets all available data in memory)
    scraper = AggregatedFinancialScraper(ticker=ticker, max_workers=3)
    results = scraper.run()
    
    if not results:
        raise Exception(f"No data found for {ticker}")
    
    # Step 3: Filter to requested years (from latest)
    filtered_data = {
        'ticker': ticker,
        'requested_years': num_years,
        'balance_sheet': results['balance_sheet_data'][:num_years],
        'income_statement': results['income_statement_data'][:num_years],
        'cash_flow': results['cash_flow_data'][:num_years]
    }
    
    return filtered_data

def create_dataframe_from_unified_catalog(unified_catalog, num_years):
    """Convert unified catalog to pandas DataFrame"""
    if not unified_catalog:
        return None
    
    # Extract all years
    all_years = set()
    for item_data in unified_catalog.values():
        all_years.update(item_data.get('values', {}).keys())
    
    # Sort years descending and limit to requested years
    sorted_years = sorted(all_years, reverse=True)[:num_years]
    
    if not sorted_years:
        return None
    
    # Prepare data for DataFrame
    rows = []
    current_section = None
    
    for key, item_data in unified_catalog.items():
        item_label = item_data.get('item_label', '')
        section_label = item_data.get('section_label', '')
        values = item_data.get('values', {})
        
        # Add section header row if section changed
        if section_label and section_label != current_section and section_label != 'Main':
            rows.append({
                'Line Item': f"━━━ {section_label} ━━━",
                **{year: '' for year in sorted_years}
            })
            current_section = section_label
        
        # Create row for this item
        row = {'Line Item': item_label}
        for year in sorted_years:
            if year in values:
                value_data = values[year]
                if isinstance(value_data, dict):
                    display_value = value_data.get('value', '')
                else:
                    display_value = value_data
                
                converted_value, is_numeric = parse_financial_value(display_value)
                if is_numeric:
                    row[year] = converted_value
                else:
                    row[year] = display_value
            else:
                row[year] = '-'
        
        rows.append(row)
    
    if not rows:
        return None
    
    df = pd.DataFrame(rows)
    return df

def display_statement_table(df, statement_name):
    """Display a financial statement as a styled table"""
    if df is None or df.empty:
        st.warning(f"No data available for {statement_name}")
        return
    
    st.markdown(f"### {statement_name}")
    
    # Create a copy for formatting
    display_df = df.copy()
    
    # Format numeric columns with proper formatting
    numeric_cols = [col for col in display_df.columns if col != 'Line Item']
    for col in numeric_cols:
        def format_value(x):
            try:
                if pd.notna(x) and isinstance(x, (int, float)) and x != '' and x != '-':
                    return f"{float(x):,.2f}"
                elif x == '' or x is None:
                    return '-'
                else:
                    return str(x)
            except (ValueError, TypeError):
                return str(x) if x is not None else '-'
        
        display_df[col] = display_df[col].apply(format_value)
    
    # Display with styling
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=500
    )

def main():
    # Header
    st.markdown('<p class="main-header">📊 Financial Data Scraper</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Extract and analyze financial statements from SEC filings</p>', unsafe_allow_html=True)
    
    # Sidebar for inputs
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        api_key = st.text_input(
            "OpenAI API Key",
            type="password",
            help="Enter your OpenAI API key to parse queries"
        )
        
        st.markdown("---")
        
        st.header("📝 Query Examples")
        st.markdown("""
        - "Get Apple financial data for last 3 years"
        - "Show me Microsoft's statements for 2 years"
        - "I need Tesla financials for 4 years"
        """)
    
    # Main input area
    col1, col2 = st.columns([3, 1])
    
    with col1:
        user_query = st.text_input(
            "Enter your query:",
            placeholder="e.g., Get Apple financial data for last 3 years",
            label_visibility="visible"
        )
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        submit_button = st.button("🔍 Search", type="primary", use_container_width=True)
    
    # Initialize session state
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'error' not in st.session_state:
        st.session_state.error = None
    
    # Process query
    if submit_button or st.session_state.results:
        if not api_key:
            st.error("⚠️ Please enter your OpenAI API key in the sidebar")
            st.stop()
        
        if not user_query and not st.session_state.results:
            st.warning("⚠️ Please enter a query")
            st.stop()
        
        if submit_button:
            with st.spinner("🔄 Processing your query..."):
                try:
                    # Call the scraper (without saving files)
                    result = smart_financial_scraper_no_save(
                        user_query=user_query,
                        openai_api_key=api_key
                    )
                    
                    if result:
                        st.session_state.results = result
                        st.session_state.error = None
                        st.success(f"✅ Successfully retrieved data for {result.get('ticker', 'N/A')}!")
                    else:
                        st.session_state.error = "No data found. Please try a different query."
                        st.session_state.results = None
                        
                except Exception as e:
                    st.session_state.error = f"Error: {str(e)}"
                    st.session_state.results = None
        
        # Display results
        if st.session_state.error and not st.session_state.results:
            st.error(f"❌ {st.session_state.error}")
        
        if st.session_state.results:
            result = st.session_state.results
            
            # Summary metrics
            st.markdown("---")
            st.markdown("### 📋 Summary")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Ticker", result.get('ticker', 'N/A'))
            with col2:
                st.metric("Years Requested", result.get('requested_years', 'N/A'))
            with col3:
                st.metric("Balance Sheets", len(result.get('balance_sheet', [])))
            with col4:
                st.metric("Income Statements", len(result.get('income_statement', [])))
            
            # Prepare data for merger
            merger_input = {
                "ticker": result.get('ticker', ''),
                "years": {}
            }
            
            # Aggregate filtered data into merger format
            for bs_data in result.get('balance_sheet', []):
                year = bs_data.get('filing_year')
                if year:
                    if year not in merger_input["years"]:
                        merger_input["years"][year] = {}
                    merger_input["years"][year]["balance_sheet"] = bs_data
            
            for is_data in result.get('income_statement', []):
                year = is_data.get('filing_year')
                if year:
                    if year not in merger_input["years"]:
                        merger_input["years"][year] = {}
                    merger_input["years"][year]["income_statement"] = is_data
            
            for cf_data in result.get('cash_flow', []):
                year = cf_data.get('filing_year')
                if year:
                    if year not in merger_input["years"]:
                        merger_input["years"][year] = {}
                    merger_input["years"][year]["cash_flow_statement"] = cf_data
            
            # Get unified catalogs
            try:
                merged_results = build_unified_catalog_all_statements(merger_input)
                
                st.markdown("---")
                st.markdown("### 📊 Financial Statements")
                
                # Display each statement
                statement_map = {
                    'Balance Sheet': ('balance_sheet', merged_results.get('balance_sheet', {})),
                    'Income Statement': ('income_statement', merged_results.get('income_statement', {})),
                    'Cash Flow Statement': ('cash_flow_statement', merged_results.get('cash_flow_statement', {}))
                }
                
                for statement_name, (stmt_key, unified_catalog) in statement_map.items():
                    if unified_catalog:
                        df = create_dataframe_from_unified_catalog(
                            unified_catalog,
                            result.get('requested_years', 3)
                        )
                        if df is not None:
                            display_statement_table(df, statement_name)
                            st.markdown("---")
                
            except Exception as e:
                st.error(f"Error processing financial statements: {str(e)}")
                st.exception(e)

if __name__ == "__main__":
    main()

