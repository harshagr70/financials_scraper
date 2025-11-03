# CELL 1: Financial Statement Scraper - Complete Code

import json
import re
import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side
from typing import Dict, List, Optional
import time
import pandas as pd
from IPython.display import display

class FinancialStatementScraper:
    """
    A scraper that extracts financial statements from SEC XBRL filings.
    Uses MetaLinks.json metadata for accurate table location.
    """

    def __init__(self, filing_url: str):
        """
        Initialize the scraper with filing URL.

        Args:
            filing_url: URL to the XBRL HTML filing
        """
        self.filing_url = filing_url
        self.session = requests.Session()

        # SEC requires proper User-Agent with contact info
        # IMPORTANT: Replace with your actual email
        self.session.headers.update({
            'User-Agent': 'MyCompany contact@email.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })

        # Extract the actual document URL from the ix viewer URL
        actual_url = self._extract_document_url(filing_url)

        # Load HTML content with retry logic
        print(f"📥 Fetching filing from SEC...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                time.sleep(0.5)
                response = self.session.get(actual_url, timeout=30)
                response.raise_for_status()
                self.html_content = response.text
                break
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    print(f"⚠ Attempt {attempt + 1}/{max_retries}: SEC blocked request (403)")
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        print(f"   Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(
                            "SEC.gov requires a User-Agent header with contact information.\n"
                            "Please update the User-Agent in the code with your email:\n"
                            "  'User-Agent': 'harshagr838@gmail.com'\n"
                            "See: https://www.sec.gov/os/accessing-edgar-data"
                        )
                else:
                    raise

        self.soup = BeautifulSoup(self.html_content, 'html.parser')
        self.tables = self.soup.find_all('table')

        print(f"✓ Loaded HTML with {len(self.tables)} tables")

        # Auto-construct and load MetaLinks.json
        self.metalinks_url = self._construct_metalinks_url(actual_url)
        self.metalinks = self._load_metalinks()

    def _extract_document_url(self, filing_url: str) -> str:
        """Extract the actual document URL from SEC inline XBRL viewer URL."""
        if '/ix?doc=' in filing_url:
            doc_path = filing_url.split('/ix?doc=')[1]
            actual_url = f"https://www.sec.gov{doc_path}"
            print(f"✓ Extracted document URL: {actual_url}")
            return actual_url
        return filing_url

    def _construct_metalinks_url(self, document_url: str) -> str:
        """Construct MetaLinks.json URL from document URL."""
        base_url = document_url.rsplit('/', 1)[0]
        metalinks_url = f"{base_url}/MetaLinks.json"
        return metalinks_url

    def _load_metalinks(self) -> Dict:
        """Load MetaLinks.json from URL and extract report section."""
        try:
            print(f"📥 Fetching MetaLinks.json...")
            time.sleep(0.3)
            response = self.session.get(self.metalinks_url, timeout=30)
            response.raise_for_status()
            metalinks_data = response.json()

            # Extract the report section from the nested structure
            if isinstance(metalinks_data, dict) and 'instance' in metalinks_data:
                # Get the first instance (usually the .htm file)
                instances = metalinks_data.get('instance', {})
                if instances:
                    # Get the first instance key (e.g., 'brka-20241231.htm')
                    first_instance = list(instances.values())[0]
                    reports = first_instance.get('report', {})

                    print(f"✓ Loaded MetaLinks with {len(reports)} roles")
                    return reports

            print(f"⚠ Unexpected MetaLinks format")
            return {}

        except Exception as e:
            print(f"⚠ Failed to load MetaLinks: {e}")
            return {}

    def find_table_by_unique_anchor(self, role_id: str) -> Optional[int]:
        """Find table index using the unique anchor from MetaLinks.json."""
        if role_id not in self.metalinks:
            print(f"⚠ Role {role_id} not found in MetaLinks")
            return None

        role_data = self.metalinks[role_id]

        if not isinstance(role_data, dict):
            print(f"⚠ Invalid role data format for {role_id}")
            return None

        unique_anchor = role_data.get('uniqueAnchor', {})

        if not unique_anchor:
            print(f"⚠ No unique anchor found for role {role_id}")
            return None

        anchor_name = unique_anchor.get('name')
        context_ref = unique_anchor.get('contextRef')

        print(f"🔍 Searching for element name: {anchor_name}")

        # Search for inline XBRL elements with matching name attribute
        # These are typically <ix:nonfraction> or <ix:nonnumeric> tags
        target_elements = []

        # Try different inline XBRL tag names
        for tag_name in ['ix:nonfraction', 'ix:nonnumeric', 'nonfraction', 'nonnumeric']:
            elements = self.soup.find_all(tag_name)
            for elem in elements:
                elem_name = elem.get('name', '')
                # Check if the name attribute matches
                if elem_name == anchor_name:
                    target_elements.append(elem)

        # If name-based search fails, try contextRef as fallback
        if not target_elements:
            print(f"⚠ Name search failed, trying contextRef: {context_ref}")
            target_elements = self.soup.find_all(attrs={'contextref': context_ref})

        # Find parent table for each matching element
        for elem in target_elements:
            parent_table = elem.find_parent('table')
            if parent_table and parent_table in self.tables:
                table_idx = self.tables.index(parent_table)
                print(f"✓ Found anchor table at index {table_idx}")
                return table_idx

        print(f"⚠ Could not locate table for role {role_id}")
        return None

    def find_table_by_pattern(self, keywords: List[str], min_length: int = 800) -> List[int]:
        """Fallback: Find tables by keyword patterns."""
        matching = []
        for idx, table in enumerate(self.tables):
            text = table.get_text()
            if len(text) >= min_length and all(kw.lower() in text.lower() for kw in keywords):
                matching.append(idx)
        return matching

    def extract_table_data(self, table_idx: int) -> List[List[str]]:
        """Extract raw data from a table."""
        if table_idx >= len(self.tables):
            return []

        table = self.tables[table_idx]
        rows = table.find_all('tr')

        data = []
        for row in rows:
            cells = row.find_all(['td', 'th'])
            row_data = [cell.get_text(strip=True) for cell in cells]
            if any(row_data):
                data.append(row_data)

        return data

    def find_related_tables(self, anchor_idx: int, statement_type: str) -> List[int]:
        """Find related tables for multi-table statements."""
        related = [anchor_idx]

        keywords_map = {
            'cash_flow': ['Cash flows', 'Operating activities', 'Investing activities',
                         'Financing activities', 'Net increase', 'Net decrease'],
            'balance_sheet': ['Total assets', 'Cash and cash equivalents',
                            'Total liabilities', 'Shareholders'],
            'income_statement': ['Revenues', 'Net earnings', 'Income', 'Expenses', 'Operating']
        }

        keywords = keywords_map.get(statement_type, [])

        for offset in [-2, -1, 1, 2]:
            check_idx = anchor_idx + offset
            if 0 <= check_idx < len(self.tables) and check_idx not in related:
                text = self.tables[check_idx].get_text()
                if any(kw in text for kw in keywords):
                    related.append(check_idx)

        return sorted(related)

    def save_to_excel(self, data: List[List[str]], statement_name: str, output_path: str):
        """Save extracted data to formatted Excel file."""
        wb = Workbook()
        ws = wb.active
        ws.title = statement_name[:31]

        header_font = Font(name='Calibri', size=11, bold=True)
        normal_font = Font(name='Calibri', size=11)

        for row_idx, row_data in enumerate(data, start=1):
            for col_idx, cell_value in enumerate(row_data, start=1):
                cell = ws.cell(row=row_idx, column=col_idx, value=cell_value)

                if row_idx == 1 or any(kw in str(cell_value).lower()
                                       for kw in ['total', 'net increase', 'net decrease',
                                                 'cash flows', 'activities:', 'assets:', 'liabilities:']):
                    cell.font = header_font
                else:
                    cell.font = normal_font

                if cell_value and isinstance(cell_value, str):
                    clean = cell_value.replace(',', '').replace('$', '').replace('(', '-').replace(')', '').strip()
                    try:
                        cell.value = float(clean)
                        cell.number_format = '#,##0'
                    except ValueError:
                        pass

        for column in ws.columns:
            max_length = 0
            col_letter = column[0].column_letter
            for cell in column:
                try:
                    max_length = max(max_length, len(str(cell.value)))
                except:
                    pass
            ws.column_dimensions[col_letter].width = min(max_length + 2, 70)

        wb.save(output_path)

    def to_dataframe(self, data: List[List[str]]) -> pd.DataFrame:
        """
        Convert extracted table data to a pandas DataFrame with proper column headers.

        Args:
            data: 2D list of table data

        Returns:
            Pandas DataFrame with year/period columns properly labeled
        """
        if not data or len(data) < 2:
            return pd.DataFrame()

        # Find header row containing years/periods
        header_row = None
        header_idx = 0

        for idx, row in enumerate(data[:5]):
            # Look for rows with year patterns (4 digits) or "December 31"
            row_text = ' '.join([str(cell) for cell in row])
            if re.search(r'(20\d{2}|December\s+\d{1,2})', row_text):
                header_row = row
                header_idx = idx
                break

        if header_row is None:
            # No clear header found, use first row
            header_row = data[0]
            header_idx = 0

        # Clean and standardize headers
        cleaned_headers = []
        for cell in header_row:
            cell_str = str(cell).strip()
            # Extract year if present
            year_match = re.search(r'20\d{2}', cell_str)
            if year_match:
                cleaned_headers.append(year_match.group())
            elif cell_str and cell_str not in ['', ' ']:
                cleaned_headers.append(cell_str)
            else:
                cleaned_headers.append('')

        # Get data rows
        data_rows = data[header_idx + 1:]

        # Ensure all rows have the same length
        max_cols = max(len(row) for row in data_rows) if data_rows else len(cleaned_headers)

        # Pad headers if needed
        if len(cleaned_headers) < max_cols:
            cleaned_headers.extend([''] * (max_cols - len(cleaned_headers)))

        # Create column names - first column is description, rest are data columns
        column_names = ['Line Item'] + [f'Value_{i}' if not h else h
                                        for i, h in enumerate(cleaned_headers[1:], 1)]

        # Normalize data rows
        normalized_data = []
        for row in data_rows:
            normalized_row = list(row) + [''] * (max_cols - len(row))
            normalized_data.append(normalized_row[:max_cols])

        # Create DataFrame
        df = pd.DataFrame(normalized_data, columns=column_names[:max_cols])

        # Clean up the dataframe - remove completely empty rows
        df = df[df.apply(lambda x: x.astype(str).str.strip().ne('').any(), axis=1)]

        return df

    def extract_statement(self, role_id: str, statement_name: str, statement_type: str,
                         output_filename: str, display_output: bool = True) -> Dict:
        """
        Generic method to extract any financial statement.

        Args:
            role_id: Role identifier from MetaLinks
            statement_name: Display name for the statement
            statement_type: Type ('cash_flow', 'balance_sheet', 'income_statement')
            output_filename: Excel output filename
            display_output: Whether to display in notebook

        Returns:
            Dictionary with extraction results
        """
        print(f"\n{'='*80}")
        print(f"Extracting: {statement_name}")
        print(f"{'='*80}\n")

        # Try to find using role_id first
        anchor_idx = None
        if role_id:
            anchor_idx = self.find_table_by_unique_anchor(role_id)

        # Fallback to pattern matching
        if anchor_idx is None:
            print("⚠ Anchor search failed, using pattern matching...")

            keywords_map = {
                'cash_flow': ['Cash flows', 'Operating activities', 'Investing activities', 'Financing activities'],
                'balance_sheet': ['Assets', 'Liabilities', 'Cash and cash equivalents', 'Total assets'],
                'income_statement': ['Revenues', 'Net earnings', 'Operating', 'Income']
            }

            keywords = keywords_map.get(statement_type, [statement_name])
            matching = self.find_table_by_pattern(keywords, min_length=1000)

            if matching:
                anchor_idx = matching[0]
                print(f"✓ Found table at index {anchor_idx} using pattern matching")
            else:
                return {'status': 'failed', 'error': f'Could not locate {statement_name}'}

        # Find related tables
        related_tables = [anchor_idx]
        print(f"✓ Found {len(related_tables)} related table(s): {related_tables}\n")

        # Extract all data
        all_data = []
        for idx in related_tables:
            table_data = self.extract_table_data(idx)
            all_data.extend(table_data)

        print(f"✓ Extracted {len(all_data)} rows of data")

        # Save to Excel
        self.save_to_excel(all_data, statement_name[:31], output_filename)
        print(f"✓ Saved to {output_filename}\n")

        # Convert to DataFrame
        df = self.to_dataframe(all_data)

        # Display in notebook if requested
        if display_output and not df.empty:
            print("\n" + "="*80)
            print(f"📊 {statement_name.upper()} - PREVIEW")
            print("="*80 + "\n")
            display(df)

        return {
            'status': 'success',
            'statement_name': statement_name,
            'statement_type': statement_type,
            'table_indices': related_tables,
            'rows_extracted': len(all_data),
            'output_file': output_filename,
            'data': all_data,
            'dataframe': df
        }

    def extract_all_statements(self, display_output: bool = True) -> Dict[str, Dict]:
        """
        Extract all three primary financial statements at once.

        Args:
            display_output: Whether to display tables in notebook

        Returns:
            Dictionary containing results for each statement
        """
        results = {}

        # Statement configurations: (exact_name, statement_type, output_name)
        statements_config = [
            ('consolidated balance sheets', 'balance_sheet', 'Consolidated_Balance_Sheets'),
            ('consolidated statements of operations', 'income_statement', 'Consolidated_Statements_of_Earnings'),
            ('consolidated statements of cashflows', 'cash_flow', 'Consolidated_Statements_of_Cash_Flows')
        ]

        for exact_name, stmt_type, output_name in statements_config:
            # Find the role in MetaLinks
            role_id = None
            statement_name = None

            for rid, role_data in self.metalinks.items():
                if isinstance(role_data, dict):
                    short_name = role_data.get('shortName', '').lower()
                    # EXACT MATCH - must be identical and not parenthetical
                    if short_name == exact_name and 'parenthetical' not in short_name:
                        role_id = rid
                        statement_name = role_data.get('shortName', output_name)
                        print(f"✓ Matched '{exact_name}' to role {rid}")
                        break

            if not statement_name:
                statement_name = output_name.replace('_', ' ')
                print(f"⚠ No MetaLinks match for '{exact_name}', will use pattern matching")

            # Extract the statement
            try:
                result = self.extract_statement(
                    role_id=role_id,
                    statement_name=statement_name,
                    statement_type=stmt_type,
                    output_filename=f'{output_name}.xlsx',
                    display_output=display_output
                )
                results[stmt_type] = result
            except Exception as e:
                print(f"✗ Error extracting {statement_name}: {e}\n")
                results[stmt_type] = {'status': 'error', 'error': str(e)}

        return results


print("✓ Financial Statement Scraper loaded successfully!\n")
print("⚠ IMPORTANT: SEC.gov requires User-Agent with contact email")
print("   Update line 29 with: 'User-Agent': 'YourCompany your.email@example.com'\n")