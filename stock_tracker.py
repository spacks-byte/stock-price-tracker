"""
Hong Kong Stock Price Tracker with Supabase Caching
Generates CSV with stock data including opening/closing prices and percentage changes
Caches data in Supabase to minimize API calls
"""

import futu as ft
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()

class HKStockTracker:
    def __init__(self, host=None, port=None):
        """
        Initialize connection to Futu OpenD gateway and Supabase
        
        Args:
            host: OpenD gateway host (default: from .env or 127.0.0.1)
            port: OpenD gateway port (default: from .env or 11111)
        """
        # Futu OpenD connection
        self.host = host or os.getenv('OPEND_HOST', '127.0.0.1')
        self.port = int(port or os.getenv('OPEND_PORT', 11111))
        self.quote_ctx = ft.OpenQuoteContext(host=self.host, port=self.port)
        
        # Supabase connection
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        
        if not supabase_url or not supabase_key:
            raise ValueError(
                "Missing Supabase credentials. "
                "Please set SUPABASE_URL and SUPABASE_KEY in .env file"
            )
        
        self.supabase: Client = create_client(supabase_url, supabase_key)
        print(f"Connected to Supabase and OpenD gateway at {self.host}:{self.port}")
    
    def get_cached_data(self, stock_code, start_date, end_date):
        """
        Retrieve cached data from Supabase for the given date range
        
        Args:
            stock_code: Stock code (e.g., 'HK.00700')
            start_date: Start date (datetime object)
            end_date: End date (datetime object)
            
        Returns:
            DataFrame with cached data or empty DataFrame
        """
        try:
            response = self.supabase.table('stock_prices').select('*').eq(
                'stock_code', stock_code
            ).gte('date', start_date.strftime('%Y-%m-%d')).lte(
                'date', end_date.strftime('%Y-%m-%d')
            ).order('date', desc=True).execute()
            
            if response.data:
                df = pd.DataFrame(response.data)
                print(f"  Found {len(df)} cached records for {stock_code}")
                return df
            else:
                print(f"  No cached data found for {stock_code}")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"  Error retrieving cached data: {e}")
            return pd.DataFrame()
    
    def save_to_database(self, stock_code, data_df):
        """
        Save stock data to Supabase database
        
        Args:
            stock_code: Stock code
            data_df: DataFrame with columns: time_key, open, close, etc.
        """
        if data_df is None or len(data_df) == 0:
            return
        
        try:
            records = []
            for _, row in data_df.iterrows():
                record = {
                    'stock_code': stock_code,
                    'date': row['time_key'],
                    'opening_price': float(row['open']),
                    'closing_price': float(row['close']),
                    'change_yesterday': None,  # Will be calculated later
                    'change_five': None,
                    'change_thirty': None,
                    'change_sixty': None,
                    'change_onehundredtwenty': None,
                }
                records.append(record)
            
            # Upsert records (insert or update if exists)
            if records:
                self.supabase.table('stock_prices').upsert(
                    records,
                    on_conflict='stock_code,date'
                ).execute()
                print(f"  Saved {len(records)} records to database for {stock_code}")
                
        except Exception as e:
            print(f"  Error saving to database: {e}")
    
    def update_percentage_changes(self, stock_code, date, changes):
        """
        Update percentage change values for a specific stock and date
        
        Args:
            stock_code: Stock code
            date: Date string (YYYY-MM-DD)
            changes: Dict with change values
        """
        try:
            self.supabase.table('stock_prices').update({
                'change_yesterday': changes.get('change_yesterday'),
                'change_five': changes.get('change_five'),
                'change_thirty': changes.get('change_thirty'),
                'change_sixty': changes.get('change_sixty'),
                'change_onehundredtwenty': changes.get('change_onehundredtwenty'),
            }).eq('stock_code', stock_code).eq('date', date).execute()
            
        except Exception as e:
            print(f"  Error updating percentage changes: {e}")
    
    def get_missing_date_ranges(self, cached_df, start_date, end_date):
        """
        Identify missing date ranges from cached data
        
        Args:
            cached_df: DataFrame with cached data
            start_date: Requested start date
            end_date: Requested end date
            
        Returns:
            List of (start, end) tuples for missing ranges
        """
        if cached_df.empty:
            return [(start_date, end_date)]
        
        # Convert cached dates to datetime
        cached_dates = pd.to_datetime(cached_df['date']).dt.date
        
        # Generate expected date range (business days only)
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        all_dates = set(all_dates.date)
        
        missing_dates = all_dates - set(cached_dates)
        
        if not missing_dates:
            return []
        
        # Group consecutive missing dates into ranges
        missing_sorted = sorted(missing_dates)
        ranges = []
        range_start = missing_sorted[0]
        prev_date = missing_sorted[0]
        
        for date in missing_sorted[1:]:
            if (date - prev_date).days > 1:
                ranges.append((
                    datetime.combine(range_start, datetime.min.time()),
                    datetime.combine(prev_date, datetime.min.time())
                ))
                range_start = date
            prev_date = date
        
        # Add final range
        ranges.append((
            datetime.combine(range_start, datetime.min.time()),
            datetime.combine(prev_date, datetime.min.time())
        ))
        
        return ranges
        
    def get_stock_data(self, stock_code, days=120):
        """
        Fetch stock data with intelligent caching
        First checks database, then fetches missing data from API
        
        Args:
            stock_code: Stock code (e.g., 'HK.00700' for Tencent)
            days: Number of days of historical data to fetch
            
        Returns:
            DataFrame with historical data
        """
        # Calculate start and end dates
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)  # Extra buffer for market holidays
        
        print(f"  Checking cache for date range: {start_date.date()} to {end_date.date()}")
        
        # Check cache first
        cached_df = self.get_cached_data(stock_code, start_date, end_date)
        
        # Determine if we need to fetch from API
        if not cached_df.empty:
            # Check if we have all the data we need
            missing_ranges = self.get_missing_date_ranges(cached_df, start_date, end_date)
            
            if not missing_ranges:
                print(f"  Using fully cached data for {stock_code}")
                # Convert cached data to match API format
                result_df = cached_df.rename(columns={
                    'opening_price': 'open',
                    'closing_price': 'close',
                    'date': 'time_key'
                })
                return result_df
            else:
                print(f"  Found {len(missing_ranges)} missing date range(s), fetching from API...")
        else:
            print(f"  No cached data, fetching all from API...")
            missing_ranges = [(start_date, end_date)]
        
        # Fetch missing data from API
        all_api_data = []
        for range_start, range_end in missing_ranges:
            print(f"  Fetching API data for: {range_start.date()} to {range_end.date()}")
            
            ret, data, page_req_key = self.quote_ctx.request_history_kline(
                code=stock_code,
                start=range_start.strftime('%Y-%m-%d'),
                end=range_end.strftime('%Y-%m-%d'),
                ktype=ft.KLType.K_DAY,
                autype=ft.AuType.QFQ  # Forward adjusted for dividends/splits
            )
            
            if ret != ft.RET_OK:
                print(f'  Error getting data from API: {data}')
            elif data is not None and len(data) > 0:
                all_api_data.append(data)
                # Save new data to database
                self.save_to_database(stock_code, data)
            
            time.sleep(0.1)  # Rate limiting
        
        # Combine API data with cached data
        if all_api_data:
            api_df = pd.concat(all_api_data, ignore_index=True)
            if not cached_df.empty:
                # Convert cached data to match API format
                cached_df_converted = cached_df.rename(columns={
                    'opening_price': 'open',
                    'closing_price': 'close',
                    'date': 'time_key'
                })
                combined_df = pd.concat([api_df, cached_df_converted], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['time_key'], keep='first')
                return combined_df
            else:
                return api_df
        elif not cached_df.empty:
            # Only cached data available
            result_df = cached_df.rename(columns={
                'opening_price': 'open',
                'closing_price': 'close',
                'date': 'time_key'
            })
            return result_df
        else:
            print(f'  No data available for {stock_code}')
            return None
    
    def calculate_percentage_change(self, current_price, past_price):
        """Calculate percentage change between two prices"""
        if past_price == 0 or pd.isna(past_price):
            return None
        return ((current_price - past_price) / past_price) * 100
    
    def generate_stock_report(self, stock_codes):
        """
        Generate comprehensive stock report for multiple stocks
        
        Args:
            stock_codes: List of stock codes (e.g., ['HK.00700', 'HK.00005'])
            
        Returns:
            DataFrame with all calculated metrics
        """
        results = []
        
        for stock_code in stock_codes:
            print(f'Processing {stock_code}...')
            
            # Get historical data
            data = self.get_stock_data(stock_code, days=130)
            
            if data is None or len(data) == 0:
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
            
            # Store results
            result = {
                'Stock Code': stock_code,
                'Stock Name': latest.get('name', 'N/A'),
                'Date': latest['time_key'],
                'Opening Price': opening_price,
                'Closing Price': closing_price,
                '% Change vs Yesterday': round(pct_change_1d, 2) if pct_change_1d else None,
                '% Change vs 5 Days': round(pct_change_5d, 2) if pct_change_5d else None,
                '% Change vs 30 Days': round(pct_change_30d, 2) if pct_change_30d else None,
                '% Change vs 60 Days': round(pct_change_60d, 2) if pct_change_60d else None,
                '% Change vs 120 Days': round(pct_change_120d, 2) if pct_change_120d else None,
            }
            
            # Update percentage changes in database
            self.update_percentage_changes(
                stock_code,
                latest['time_key'],
                {
                    'change_yesterday': pct_change_1d,
                    'change_five': pct_change_5d,
                    'change_thirty': pct_change_30d,
                    'change_sixty': pct_change_60d,
                    'change_onehundredtwenty': pct_change_120d,
                }
            )
            
            results.append(result)
            
            # Small delay to avoid rate limiting
            time.sleep(0.1)
        
        return pd.DataFrame(results)
    
    def save_to_csv(self, df, filename=None):
        """
        Save DataFrame to CSV file
        
        Args:
            df: DataFrame to save
            filename: Output filename (default: hk_stocks_YYYYMMDD.csv)
        """
        if filename is None:
            filename = f"hk_stocks_{datetime.now().strftime('%Y%m%d')}.csv"
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f'\nData saved to {filename}')
        return filename
    
    def close(self):
        """Close the quote context connection"""
        self.quote_ctx.close()


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
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("ERROR: .env file not found!")
        print("\nPlease create a .env file with your credentials:")
        print("1. Copy .env.example to .env")
        print("2. Fill in your SUPABASE_URL and SUPABASE_KEY")
        print("\nSee SETUP_GUIDE.md for detailed instructions.")
        return
    
    # Load stock codes from file
    stock_codes = load_stock_codes('stocks.txt')
    if not stock_codes:
        return
    
    print("Hong Kong Stock Price Tracker with Supabase Caching")
    print("=" * 60)
    print("\nIMPORTANT: Make sure OpenD gateway is running!")
    print("Download from: https://www.futunn.com/download/OpenAPI")
    print("=" * 60)
    
    try:
        # Initialize tracker
        tracker = HKStockTracker()
        
        # Generate report
        print(f'\nFetching data for {len(stock_codes)} stocks...\n')
        df = tracker.generate_stock_report(stock_codes)
        
        # Display results
        print('\n' + '=' * 60)
        print('RESULTS:')
        print('=' * 60)
        print(df.to_string(index=False))
        
        # Save to CSV
        filename = tracker.save_to_csv(df)
        
        # Close connection
        tracker.close()
        
        print('\nDone!')
        
    except Exception as e:
        print(f'\nError: {e}')
        print('\nTroubleshooting:')
        print('1. Is OpenD gateway running?')
        print('2. Is the .env file configured correctly?')
        print('3. Is the Supabase database set up? (See database_schema.sql)')
        print('4. Do you have the required market data permissions?')
        print('\nSee SETUP_GUIDE.md for detailed setup instructions.')


if __name__ == '__main__':
    main()
