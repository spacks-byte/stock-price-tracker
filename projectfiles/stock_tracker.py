"""
Hong Kong Stock Price Tracker
Updates Google Sheets with stock data including opening/closing prices and percentage changes
"""

import futu as ft
import pandas as pd
from datetime import datetime, timedelta
import time
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Import configuration from config.py
try:
    from config import GOOGLE_SHEET_URL, GOOGLE_CREDENTIALS_FILE, STOCK_TRACKER_WORKSHEET
    WORKSHEET_TITLE = STOCK_TRACKER_WORKSHEET
except ImportError:
    print("ERROR: config.py not found!")
    print("Please copy config.example.py to config.py and fill in your credentials.")
    exit(1)

# OpenD Configuration
# Set USE_MOOMOO = True if using MooMoo OpenD, False if using Futu OpenD
USE_MOOMOO = False  # Change to True for MooMoo OpenD

# Rate limiting configuration (adjust based on your OpenD version)
if USE_MOOMOO:
    RATE_LIMIT_DELAY = 2.0  # Much slower for MooMoo (seconds between requests)
    MAX_RETRIES = 1  # Fewer retries for MooMoo (quota errors won't benefit from retries)
    FETCH_DAYS = 90  # Fetch fewer days to conserve quota (vs 120)
else:
    RATE_LIMIT_DELAY = 0.5  # Faster for Futu (seconds between requests)
    MAX_RETRIES = 2  # More retries for Futu
    FETCH_DAYS = 120  # Fetch more historical data

class HKStockTracker:
    def __init__(self, host=None, port=None, rate_limit_delay=0.5, max_retries=2):
        """
        Initialize connection to Futu/MooMoo OpenD gateway
        
        Args:
            host: OpenD gateway host (default: 127.0.0.1)
            port: OpenD gateway port (default: 11111)
            rate_limit_delay: Delay between API calls in seconds (default: 0.5, recommend 1.0 for MooMoo)
            max_retries: Maximum number of retries for failed requests (default: 2)
        """
        # OpenD connection
        self.host = host or '127.0.0.1'
        self.port = int(port or 11111)
        self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        
        # Google Sheets connection
        self.gc = self.authenticate_google_sheets()
        self.sheet = self.open_or_create_sheet()
        
        print(f"Connected to Google Sheets and OpenD gateway at {self.host}:{self.port}")
        print(f"Rate limit: {rate_limit_delay}s between requests, Max retries: {max_retries}")
    
    def get_stock_name(self, stock_code):
        """
        Fetch stock name from Futu API
        
        Args:
            stock_code: Stock code (e.g., 'HK.00700')
            
        Returns:
            Stock name or 'N/A' if not found
        """
        try:
            ret, data = self.quote_ctx.get_stock_basicinfo(
                market=ft.Market.HK,
                stock_type=ft.SecurityType.STOCK,
                code_list=[stock_code]
            )
            
            if ret == ft.RET_OK and data is not None and len(data) > 0:
                return data.iloc[0]['name']
            else:
                return 'N/A'
        except Exception as e:
            print(f"  Error fetching stock name: {e}")
            return 'N/A'
    
    def get_stock_data(self, stock_code, days=120):
        """
        Fetch stock data from OpenD API with retry logic
        
        Args:
            stock_code: Stock code (e.g., 'HK.00700' for Tencent)
            days: Number of days of historical data to fetch
            
        Returns:
            Tuple of (DataFrame with historical data, error_message)
        """
        # Calculate start and end dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)  # Extra buffer for market holidays
        
        print(f"  Fetching API data for: {start_date.date()} to {end_date.date()}")
        
        # Retry logic for MooMoo compatibility
        for attempt in range(self.max_retries + 1):
            try:
                ret, data, page_req_key = self.quote_ctx.request_history_kline(
                    code=stock_code,
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d'),
                    ktype=ft.KLType.K_DAY,
                    autype=ft.AuType.QFQ  # Forward adjusted for dividends/splits
                )
                
                if ret != ft.RET_OK:
                    error_msg = f'API Error Code {ret}: {data}'
                    
                    # Check for quota error (MooMoo specific)
                    is_quota_error = (ret == -1 and 'quota' in str(data).lower())
                    
                    if is_quota_error:
                        # Don't retry quota errors - they won't resolve immediately
                        print(f'  QUOTA EXCEEDED: {data}')
                        print(f'  Skipping retries (quota errors require waiting period)')
                        # Add extra delay before next stock to help with quota
                        time.sleep(self.rate_limit_delay * 3)
                        return None, f'Quota Error: {data}'
                    elif attempt < self.max_retries:
                        print(f'  {error_msg} - Retrying ({attempt + 1}/{self.max_retries})...')
                        time.sleep(self.rate_limit_delay * 2)  # Wait longer before retry
                        continue
                    else:
                        print(f'  {error_msg}')
                        return None, error_msg
                elif data is not None and len(data) > 0:
                    time.sleep(self.rate_limit_delay)  # Rate limiting
                    return data, None
                else:
                    error_msg = 'No data returned from API'
                    print(f'  {error_msg}')
                    return None, error_msg
                    
            except Exception as e:
                error_msg = f'Exception: {str(e)}'
                if attempt < self.max_retries:
                    print(f'  {error_msg} - Retrying ({attempt + 1}/{self.max_retries})...')
                    time.sleep(self.rate_limit_delay * 2)
                    continue
                else:
                    print(f'  {error_msg}')
                    return None, error_msg
        
        return None, 'Max retries exceeded'
    
    def calculate_percentage_change(self, current_price, past_price):
        """Calculate percentage change between two prices"""
        if past_price == 0 or pd.isna(past_price):
            return None
        return ((current_price - past_price) / past_price) * 100
    
    def generate_stock_report(self, stock_codes, location_map=None):
        """
        Generate comprehensive stock report for multiple stocks
        
        Args:
            stock_codes: List of stock codes (e.g., ['HK.00700', 'HK.00005'])
            location_map: Dictionary mapping stock codes to their locations
            
        Returns:
            Tuple of (DataFrame with all calculated metrics, dict of missing stocks with reasons)
        """
        results = []
        missing_stocks = {}  # Changed to dict to store error reasons
        
        for stock_code in stock_codes:
            print(f'Processing {stock_code}...')
            
            # Get stock name first
            stock_name = self.get_stock_name(stock_code)
            
            # Get historical data (use configured FETCH_DAYS)
            data, error_msg = self.get_stock_data(stock_code, days=FETCH_DAYS)
            
            if data is None or len(data) == 0:
                location = location_map.get(stock_code, 'Unknown') if location_map else 'Unknown'
                missing_stocks[stock_code] = {'location': location, 'error': error_msg or 'No data'}
                print(f'No data available for {stock_code}')
                continue
            
            # Sort by date descending (most recent first)
            data = data.sort_values('time_key', ascending=False).reset_index(drop=True)
            
            # Get latest data (today/most recent)
            latest = data.iloc[0]
            opening_price = latest['open']
            closing_price = latest['close']
            
            # Calculate percentage changes
            pct_change_1d = self.calculate_percentage_change(
                closing_price, 
                data.iloc[1]['close'] if len(data) > 1 else None
            )
            
            pct_change_3d = self.calculate_percentage_change(
                closing_price,
                data.iloc[3]['close'] if len(data) > 3 else None
            )
            
            pct_change_5d = self.calculate_percentage_change(
                closing_price,
                data.iloc[5]['close'] if len(data) > 5 else None
            )
            
            pct_change_30d = self.calculate_percentage_change(
                closing_price,
                data.iloc[30]['close'] if len(data) > 30 else None
            )
            
            pct_change_60d = self.calculate_percentage_change(
                closing_price,
                data.iloc[60]['close'] if len(data) > 60 else None
            )
            
            pct_change_120d = self.calculate_percentage_change(
                closing_price,
                data.iloc[120]['close'] if len(data) > 120 else None
            )
            
            # Store results with properly formatted date
            date_value = latest['time_key']
            # Convert to date string if it's a datetime object
            if isinstance(date_value, pd.Timestamp):
                date_str = date_value.strftime('%Y-%m-%d')
            elif isinstance(date_value, datetime):
                date_str = date_value.strftime('%Y-%m-%d')
            else:
                date_str = str(date_value).split()[0] if ' ' in str(date_value) else str(date_value)
            
            result = {
                'Stock Code': stock_code,
                'Stock Name': stock_name,
                'Location': location_map.get(stock_code, 'Unknown') if location_map else 'Unknown',
                'Date': date_str,
                'Opening Price': opening_price,
                'Closing Price': closing_price,
                '% Change vs Yesterday': round(pct_change_1d, 2) if pct_change_1d else None,
                '% Change vs 3 Days': round(pct_change_3d, 2) if pct_change_3d else None,
                '% Change vs 5 Days': round(pct_change_5d, 2) if pct_change_5d else None,
                '% Change vs 30 Days': round(pct_change_30d, 2) if pct_change_30d else None,
                '% Change vs 60 Days': round(pct_change_60d, 2) if pct_change_60d else None,
                '% Change vs 120 Days': round(pct_change_120d, 2) if pct_change_120d else None,
            }
            
            results.append(result)
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
        
        # Create DataFrame and sort by % Change vs Yesterday (descending)
        df = pd.DataFrame(results)
        if not df.empty and '% Change vs Yesterday' in df.columns:
            df = df.sort_values('% Change vs Yesterday', ascending=False, na_position='last')
        return df, missing_stocks
    
    def authenticate_google_sheets(self):
        """Authenticate and return Google Sheets client"""
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        return gspread.authorize(creds)
    
    def open_or_create_sheet(self):
        """Open existing worksheet or create new one with headers"""
        sheet = self.gc.open_by_url(GOOGLE_SHEET_URL)
        try:
            worksheet = sheet.worksheet(WORKSHEET_TITLE)
            print(f"Found existing worksheet: {WORKSHEET_TITLE}")
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=WORKSHEET_TITLE, rows="1000", cols="20")
            headers = ['Stock Code', 'Stock Name', 'Location', 'Date', 'Opening Price', 'Closing Price',
                      '% Change vs Yesterday', '% Change vs 3 Days', '% Change vs 5 Days', '% Change vs 30 Days',
                      '% Change vs 60 Days', '% Change vs 120 Days']
            worksheet.append_row(headers)
            print(f"Created new worksheet: {WORKSHEET_TITLE}")
        return worksheet
    
    def update_google_sheet(self, df):
        """
        Update Google Sheets with stock data
        
        Args:
            df: DataFrame with stock data to upload
        """
        if df is None or len(df) == 0:
            print("No data to update in Google Sheets")
            return
        
        try:
            # Clear existing data (keep header row)
            self.sheet.clear()
            
            # Prepare headers
            headers = ['Stock Code', 'Stock Name', 'Location', 'Date', 'Opening Price', 'Closing Price',
                      '% Change vs Yesterday', '% Change vs 3 Days', '% Change vs 5 Days', '% Change vs 30 Days',
                      '% Change vs 60 Days', '% Change vs 120 Days']
            
            # Prepare data rows
            data_rows = []
            for _, row in df.iterrows():
                data_row = [
                    str(row['Stock Code']),
                    str(row['Stock Name']),
                    str(row['Location']),
                    str(row['Date']),
                    row['Opening Price'],
                    row['Closing Price'],
                    row['% Change vs Yesterday'] if pd.notna(row['% Change vs Yesterday']) else '',
                    row['% Change vs 3 Days'] if pd.notna(row['% Change vs 3 Days']) else '',
                    row['% Change vs 5 Days'] if pd.notna(row['% Change vs 5 Days']) else '',
                    row['% Change vs 30 Days'] if pd.notna(row['% Change vs 30 Days']) else '',
                    row['% Change vs 60 Days'] if pd.notna(row['% Change vs 60 Days']) else '',
                    row['% Change vs 120 Days'] if pd.notna(row['% Change vs 120 Days']) else '',
                ]
                data_rows.append(data_row)
            
            # Update sheet with all data at once
            all_rows = [headers] + data_rows
            self.sheet.append_rows(all_rows, value_input_option='USER_ENTERED')
            
            # Apply color formatting to % Change vs Yesterday column (column G, index 6)
            print("Applying color formatting...")
            for i, row in enumerate(df.iterrows(), start=2):  # Start at row 2 (after header)
                _, data = row
                pct_change = data.get('% Change vs Yesterday')
                
                if pd.notna(pct_change):
                    cell_address = f'G{i}'
                    if pct_change >= 10:
                        # Green background for gains >= 10%
                        self.sheet.format(cell_address, {
                            "backgroundColor": {"red": 0.7, "green": 1.0, "blue": 0.7},
                            "textFormat": {"bold": True}
                        })
                    elif pct_change <= -5:
                        # Red background for losses <= -5%
                        self.sheet.format(cell_address, {
                            "backgroundColor": {"red": 1.0, "green": 0.7, "blue": 0.7},
                            "textFormat": {"bold": True}
                        })
            
            print(f"\nGoogle Sheets updated successfully with {len(data_rows)} stocks")
            print(f"View at: {GOOGLE_SHEET_URL}")
            
        except Exception as e:
            print(f"Error updating Google Sheets: {e}")
    
    def save_to_csv(self, df, filename=None):
        """
        Save DataFrame to CSV file (optional backup)
        
        Args:
            df: DataFrame to save
            filename: Output filename (default: hk_stocks_YYYYMMDD.csv)
        """
        if filename is None:
            filename = f"hk_stocks_{datetime.now().strftime('%Y%m%d')}.csv"
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f'Data also saved to CSV: {filename}')
        return filename
    
    def close(self):
        """Close the quote context connection"""
        self.quote_ctx.close()


def load_stock_codes_with_location(filename='fullstocks.txt'):
    """
    Load stock codes with their location information from a text file
    
    Args:
        filename: Path to file with stock codes (one per line with section headers)
        
    Returns:
        Tuple of (list of stock codes, dictionary mapping codes to locations)
    """
    if not os.path.exists(filename):
        print(f"ERROR: {filename} not found!")
        print(f"Please create {filename} with your stock codes (one per line)")
        return [], {}
    
    stock_codes = []
    location_map = {}
    current_location = 'Unknown'
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Check if it's a section header
                if line.startswith('#'):
                    # Extract location name from comment
                    current_location = line.lstrip('#').strip()
                    continue
                
                # Skip empty lines
                if not line:
                    continue
                
                # Extract stock code
                code = line.split()[0] if line.split() else ''
                if code and (code.startswith('HK.') or code.startswith('US.')):
                    if code not in stock_codes:
                        stock_codes.append(code)
                    location_map[code] = current_location
        
        if not stock_codes:
            print(f"WARNING: No valid stock codes found in {filename}")
            print("Stock codes should be in format: HK.XXXXX or US.XXXXX")
        
        return stock_codes, location_map
        
    except Exception as e:
        print(f"ERROR reading {filename}: {e}")
        return [], {}


def load_stock_codes(filename='stocks.txt'):
    """
    Load stock codes from a text file
    
    Args:
        filename: Path to file with stock codes (one per line)
        
    Returns:
        List of stock codes
    """
    if not os.path.exists(filename):
        print(f"ERROR: {filename} not found!")
        print(f"Please create {filename} with your stock codes (one per line)")
        return []
    
    stock_codes = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                # Strip whitespace and split on first space or tab
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    # Extract just the stock code (before any comment)
                    code = line.split()[0] if line.split() else ''
                    if code and code.startswith('HK.'):
                        stock_codes.append(code)
        
        if not stock_codes:
            print(f"WARNING: No valid stock codes found in {filename}")
            print("Stock codes should be in format: HK.XXXXX")
        
        return stock_codes
        
    except Exception as e:
        print(f"ERROR reading {filename}: {e}")
        return []


def main():
    """Main function to run the stock tracker"""
    
    # Load stock codes with locations from fullstocks.txt
    stock_codes, location_map = load_stock_codes_with_location('fullstocks.txt')
    if not stock_codes:
        return
    
    print("Hong Kong Stock Price Tracker")
    print("=" * 60)
    print(f"\nOpenD Type: {'MooMoo' if USE_MOOMOO else 'Futu'}")
    print(f"Rate Limit: {RATE_LIMIT_DELAY}s between requests")
    print(f"Max Retries: {MAX_RETRIES}")
    print(f"Historical Data: {FETCH_DAYS} days")
    print("\nIMPORTANT: Make sure OpenD gateway is running!")
    if USE_MOOMOO:
        print("Download from: https://www.moomoo.com/")
        print("\nNOTE: MooMoo has strict quota limits. If you hit quota errors:")
        print("  - Reduce FETCH_DAYS (currently {})\n".format(FETCH_DAYS))
        print("  - Increase RATE_LIMIT_DELAY (currently {}s)".format(RATE_LIMIT_DELAY))
        print("  - Run with fewer stocks at a time")
        print("  - Wait for quota to reset (can take up to 30 days for full reset)")
    else:
        print("Download from: https://www.futunn.com/download/OpenAPI")
    print("=" * 60)
    
    try:
        # Initialize tracker with appropriate settings
        tracker = HKStockTracker(
            rate_limit_delay=RATE_LIMIT_DELAY,
            max_retries=MAX_RETRIES
        )
        
        # Generate report
        print(f'\nFetching data for {len(stock_codes)} stocks...\n')
        df, missing_stocks = tracker.generate_stock_report(stock_codes, location_map)
        
        # Display results
        print('\n' + '=' * 60)
        print('RESULTS:')
        print('=' * 60)
        print(df.to_string(index=False))
        
        # Update Google Sheets
        tracker.update_google_sheet(df)
        
        # Display missing stocks if any
        if missing_stocks:
            print('\n' + '=' * 60)
            print('MISSING STOCKS (No data from OpenD):')
            print('=' * 60)
            
            quota_errors = 0
            for stock, info in missing_stocks.items():
                print(f'{stock:12} | {info["location"]:20} | {info["error"]}')
                if 'quota' in info['error'].lower():
                    quota_errors += 1
            
            print(f'\nTotal missing: {len(missing_stocks)} out of {len(stock_codes)} stocks')
            
            if quota_errors > 0:
                print(f'Quota errors: {quota_errors}')
                print('\n⚠️  MooMoo QUOTA LIMIT REACHED ⚠️')
                print('Suggestions to reduce quota usage:')
                print('  1. Set FETCH_DAYS to a lower value (try 60 or 30 days)')
                print('  2. Increase RATE_LIMIT_DELAY to 3.0 or higher')
                print('  3. Split your stock list into smaller batches')
                print('  4. Run the script less frequently')
                print('  5. Wait for quota to reset (gradual over 30 days)')
            elif USE_MOOMOO:
                print('\nNote: If using MooMoo OpenD, some stocks may not be available.')
                print('Consider checking market data subscriptions.')
        
        # Close connection
        tracker.close()
        
        print('\nDone!')
        
    except Exception as e:
        print(f'\nError: {e}')
        print('\nTroubleshooting:')
        print('1. Is OpenD gateway running?')
        print('2. Do you have the required market data permissions?')
        print('\nSee SETUP_GUIDE.md for detailed setup instructions.')


if __name__ == '__main__':
    main()
