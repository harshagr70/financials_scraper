# ============================================================================
# CELL 1: Core Logic (FIXED VERSION - Only 2020 onwards)
# ============================================================================

import requests
from bs4 import BeautifulSoup
import time
import re
from typing import List, Dict, Optional


def get_cik_from_ticker(ticker: str, headers: dict) -> Optional[str]:
    """
    Get CIK number from ticker using SEC's company_tickers.json

    Args:
        ticker: Stock ticker symbol
        headers: Request headers

    Returns:
        Zero-padded 10-digit CIK string, or None if not found
    """
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        companies = response.json()

        # Search for ticker in the JSON data
        for company_id, company_data in companies.items():
            if company_data['ticker'].upper() == ticker:
                cik = str(company_data['cik_str']).zfill(10)  # Zero-pad to 10 digits
                return cik

        return None

    except Exception as e:
        print(f"Error fetching CIK: {str(e)}")
        return None


def get_10k_filings(ticker: str) -> List[Dict[str, str]]:
    """
    Scrape SEC 10-K filings for a given ticker symbol.

    Args:
        ticker: Company ticker symbol (e.g., 'AAPL', 'MSFT')

    Returns:
        List of dicts containing filing_date, report_year, accession_number, and ix_viewer_url
        Returns empty list if ticker not found or error occurs
    """

    # User-Agent header required by SEC
    headers = {
        'User-Agent': 'harshagr838@gmail.com'
    }

    try:
        # Step 1: Convert ticker to CIK
        cik = get_cik_from_ticker(ticker.upper(), headers)
        if not cik:
            print(f"Ticker '{ticker}' not found")
            return []

        print(f"Found CIK: {cik} for ticker: {ticker}")
        time.sleep(0.5)

        # Step 2: Fetch 10-K filings list
        filings_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=10-K&count=100"
        response = requests.get(filings_url, headers=headers)
        response.raise_for_status()

        # Step 3: Parse HTML table to extract accession numbers and dates
        soup = BeautifulSoup(response.content, 'html.parser')
        filings_table = soup.find('table', class_='tableFile2')

        if not filings_table:
            print("No filings table found")
            return []

        rows = filings_table.find_all('tr')[1:]  # Skip header row
        filings_data = []

        for row in rows:
            if len(filings_data) >= 10:  # Stop after getting 10 filings
                break

            cols = row.find_all('td')
            if len(cols) >= 4:
                filing_type = cols[0].text.strip()
                # Only get 10-K (not 10-K/A amendments)
                if filing_type == '10-K':
                    filing_date = cols[3].text.strip()
                    
                    # Extract year from filing date and check if >= 2020
                    filing_year = int(filing_date.split('-')[0])
                    if filing_year < 2020:
                        continue  # Skip filings before 2020

                    # Extract accession number from Description column using regex
                    description = cols[2].text.strip()
                    acc_match = re.search(r'Acc-no:\s*(\d{10}-\d{2}-\d{6})', description)

                    if acc_match:
                        accession_number = acc_match.group(1)
                        filings_data.append({
                            'accession_number': accession_number,
                            'filing_date': filing_date
                        })

        print(f"Found {len(filings_data)} 10-K filings (2020 onwards)")

        # Steps 4-6: Get IX viewer URLs for each filing
        results = []
        for filing in filings_data:
            time.sleep(0.5)  # Rate limiting

            accession_no_hyphens = filing['accession_number'].replace('-', '')
            accession_with_hyphens = filing['accession_number']

            # Fetch filing index page
            index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_hyphens}/{accession_with_hyphens}-index.htm"

            try:
                index_response = requests.get(index_url, headers=headers)
                index_response.raise_for_status()

                # Parse to find primary HTML document
                index_soup = BeautifulSoup(index_response.content, 'html.parser')

                # Find the document table - try different approaches
                doc_table = index_soup.find('table', class_='tableFile')
                if not doc_table:
                    # Try finding any table that contains document information
                    tables = index_soup.find_all('table')
                    for table in tables:
                        # Look for table with "Document" header
                        header_row = table.find('tr')
                        if header_row and 'document' in header_row.text.lower():
                            doc_table = table
                            break

                if doc_table:
                    primary_htm = None

                    # Get all rows, skip header
                    doc_rows = doc_table.find_all('tr')[1:]

                    for doc_row in doc_rows:
                        doc_cols = doc_row.find_all('td')

                        if len(doc_cols) >= 4:
                            # Column structure: Seq | Description | Document | Type | Size
                            seq_num = doc_cols[0].text.strip()
                            description = doc_cols[1].text.strip()
                            doc_link = doc_cols[2].find('a')
                            doc_type = doc_cols[3].text.strip()

                            if doc_link:
                                doc_name = doc_link.text.strip()

                                # Find first HTML document that is the main 10-K filing
                                # Criteria:
                                # 1. Must be .htm or .html file
                                # 2. Type should be "10-K" or description should be "10-K"
                                # 3. Exclude exhibits (EX-), graphics (GRAPHIC), and XML files
                                # 4. Usually has sequence number 1

                                is_htm = doc_name.lower().endswith(('.htm', '.html'))
                                is_10k = (doc_type.upper() == '10-K' or
                                         description.upper() == '10-K' or
                                         '10-K' in description.upper())
                                is_not_exhibit = not doc_name.lower().startswith('ex')
                                is_not_graphic = 'graphic' not in doc_name.lower()
                                is_not_xml = not doc_name.lower().endswith('.xml')

                                if (is_htm and is_10k and is_not_exhibit and
                                    is_not_graphic and is_not_xml):
                                    primary_htm = doc_name
                                    break

                    # Fallback: if no primary document found with strict criteria,
                    # just get the first .htm file that's not an exhibit or graphic
                    if not primary_htm:
                        for doc_row in doc_rows:
                            doc_cols = doc_row.find_all('td')
                            if len(doc_cols) >= 3:
                                doc_link = doc_cols[2].find('a')
                                if doc_link:
                                    doc_name = doc_link.text.strip()
                                    if (doc_name.lower().endswith(('.htm', '.html')) and
                                        not doc_name.lower().startswith('ex') and
                                        'graphic' not in doc_name.lower() and
                                        not doc_name.lower().endswith('.xml')):
                                        primary_htm = doc_name
                                        break

                    if primary_htm:
                        # Construct IX viewer URL
                        ix_url = f"https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{accession_no_hyphens}/{primary_htm}"

                        # Extract report year from filing date (YYYY-MM-DD)
                        report_year = filing['filing_date'].split('-')[0]

                        results.append({
                            'filing_date': filing['filing_date'],
                            'report_year': report_year,
                            'accession_number': accession_with_hyphens,
                            'ix_viewer_url': ix_url
                        })
                        print(f"  ✓ {report_year}: {accession_with_hyphens}")
                    else:
                        print(f"  ✗ {filing['filing_date']}: Could not find primary document for {accession_with_hyphens}")

            except Exception as e:
                print(f"Error processing filing {filing['accession_number']}: {str(e)}")
                continue

        return results

    except Exception as e:
        print(f"Error: {str(e)}")
        return []