import pandas as pd
import time
from datetime import datetime, timedelta
import random
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from futu import *

# Import configuration from config.py
try:
    from config import GOOGLE_SHEET_URL, GOOGLE_CREDENTIALS_FILE, TURNOVER_WORKSHEET
    WORKSHEET_TITLE = TURNOVER_WORKSHEET
except ImportError:
    print("ERROR: config.py not found!")
    print("Please copy config.example.py to config.py and fill in your credentials.")
    exit(1)

class TurnoverTracker2:
    def __init__(self):
        self.ticker = "HK.800000"
        self.default_start_date = "2023-01-01"
        self.max_retries = 3
        self.retry_delay = 30
        self.last_api_call = 0
        self.min_api_interval = 60

        self.time_slots = {
            "10:00": "10:00:00",
            "10:30": "10:30:00",
            "11:00": "11:00:00",
            "12:00": "12:00:00",
            "14:00": "14:00:00",
            "15:00": "15:00:00",
            "16:00": "16:00:00"
        }

        self.headers = [
            'Trade Day', 'POS', '10:00', '10:30', '11:00', '12:00',
            '14:00', '15:00', '16:00', 'Total Daily', 'CAS',
            'Opening', 'Closing', 'Change from previous day close',
            'Shanghai Connect Southbound', '%'
        ]

        self.gc = self.authenticate_google_sheets()
        self.sheet = self.open_or_create_sheet()

    def authenticate_google_sheets(self):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        return gspread.authorize(creds)

    def open_or_create_sheet(self):
        sheet = self.gc.open_by_url(GOOGLE_SHEET_URL)
        try:
            worksheet = sheet.worksheet(WORKSHEET_TITLE)
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=WORKSHEET_TITLE, rows="1000", cols="20")
            worksheet.append_row(self.headers)
            # Initialize two fixed rows under header if needed, e.g. empty or with some fixed content
            worksheet.append_row(["Fixed Row 1"] + [""] * (len(self.headers) - 1))
            worksheet.append_row(["Fixed Row 2"] + [""] * (len(self.headers) - 1))
        return worksheet

    def get_all_records(self):
        return self.sheet.get_all_records()

    def get_last_recorded_date(self):
        records = self.get_all_records()
        if not records:
            return None
        # Find the most recent date in the 'Trade Day' column (top row is newest)
        for row in records:
            date_str = row.get('Trade Day')
            if date_str and '/' in date_str:
                try:
                    return datetime.strptime(date_str, "%m/%d/%Y").date()
                except ValueError:
                    continue
        return None

    def determine_date_range(self):
        end_date = datetime.now().date()
        last_recorded = self.get_last_recorded_date()
        if last_recorded is None:
            start_date = datetime.strptime(self.default_start_date, "%Y-%m-%d").date()
            print(f"No existing data found. Fetching from {start_date} to {end_date}")
        elif last_recorded >= end_date:
            print(f"Sheet is up to date. No update needed.")
            return None, None
        else:
            # Start from last recorded date to re-fetch latest day's data (to update it)
            start_date = last_recorded
            print(f"Fetching new data from {start_date} to {end_date}")
        return start_date, end_date

    def fetch_futu_data(self, start_date, end_date):
        for attempt in range(self.max_retries):
            quote_ctx = None
            try:
                time_since_last_call = time.time() - self.last_api_call
                if time_since_last_call < self.min_api_interval:
                    wait_time = self.min_api_interval - time_since_last_call
                    print(f"Rate limiting: waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                time.sleep(random.uniform(2, 5))
                print(f"Making API call (attempt {attempt + 1}/{self.max_retries})...")
                self.last_api_call = time.time()

                quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
                ret, data, page_req_key = quote_ctx.request_history_kline(
                    self.ticker,
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d'),
                    ktype=KLType.K_30M,
                    max_count=100000,
                )

                if ret != RET_OK:
                    print(f"Futu API error: {data}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                    continue

                if data.empty:
                    print(f"No data returned for {self.ticker}")
                    return pd.DataFrame()

                print(f"Successfully fetched {len(data)} records")
                return data

            except Exception as e:
                print(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    print("Failed to fetch data after all retries")
                    return pd.DataFrame()
            finally:
                if quote_ctx:
                    quote_ctx.close()
        return pd.DataFrame()

    def process_daily_data(self, daily_data):
        if daily_data.empty:
            return None
        daily_data['time_key'] = pd.to_datetime(daily_data['time_key'])
        daily_data['time_only'] = daily_data['time_key'].dt.strftime('%H:%M:%S')

        # Opening price at 10:00
        opening_data = daily_data[daily_data['time_only'] == '10:00:00']
        if opening_data.empty:
            print("Missing opening data (10:00) for this day - skipping")
            return None
        opening_price = opening_data.iloc[0]['open']

        # Closing price at 16:00 (or latest)
        closing_data = daily_data[daily_data['time_only'] == '16:00:00']
        if closing_data.empty:
            latest_data = daily_data.iloc[-1]
            closing_price = latest_data['close']
            print(f"Using latest available price {closing_price} at {latest_data['time_only']} as closing")
        else:
            closing_price = closing_data.iloc[0]['close']

        turnover_data = {}
        for slot_name, slot_time in self.time_slots.items():
            exact_time_data = daily_data[daily_data['time_only'] == slot_time]
            if not exact_time_data.empty:
                up_to_time = daily_data[daily_data['time_only'] <= slot_time]
                cumulative_turnover = up_to_time['turnover'].sum() / 1e9
                turnover_data[slot_name] = round(cumulative_turnover, 2)
            else:
                turnover_data[slot_name] = ""

        total_daily = ""
        if turnover_data.get('16:00', '') != "":
            total_daily = turnover_data['16:00']

        # Calculate POS turnover: sum turnover between 09:00:00 and 09:30:00 inclusive
        pos_turnover = daily_data[
            (daily_data['time_only'] >= '09:00:00') & (daily_data['time_only'] <= '09:30:00')
        ]['turnover'].sum() / 1e9
        pos_turnover = round(pos_turnover, 2) if pos_turnover > 0 else ""

        # Calculate CAS turnover: sum turnover between 16:00:00 and 16:10:00 inclusive
        cas_turnover = daily_data[
            (daily_data['time_only'] >= '16:00:00') & (daily_data['time_only'] <= '16:10:00')
        ]['turnover'].sum() / 1e9
        cas_turnover = round(cas_turnover, 2) if cas_turnover > 0 else ""

        trade_date = daily_data.iloc[0]['time_key'].strftime('%m/%d/%Y')

        return {
            'Trade Day': trade_date,
            'POS': pos_turnover,
            '10:00': turnover_data.get('10:00', ''),
            '10:30': turnover_data.get('10:30', ''),
            '11:00': turnover_data.get('11:00', ''),
            '12:00': turnover_data.get('12:00', ''),
            '14:00': turnover_data.get('14:00', ''),
            '15:00': turnover_data.get('15:00', ''),
            '16:00': turnover_data.get('16:00', ''),
            'Total Daily': total_daily,
            'CAS': cas_turnover,
            'Opening': round(opening_price, 2),
            'Closing': round(closing_price, 2),
            'Change from previous day close': '',
            'Shanghai Connect Southbound': '',
            '%': ''
        }

    def process_fetched_data(self, raw_data):
        if raw_data.empty:
            return []
        raw_data['time_key'] = pd.to_datetime(raw_data['time_key'])
        raw_data['date'] = raw_data['time_key'].dt.date
        processed_days = []
        for date, daily_data in raw_data.groupby('date'):
            processed_day = self.process_daily_data(daily_data)
            if processed_day:
                processed_days.append(processed_day)
        processed_days.sort(key=lambda x: datetime.strptime(x['Trade Day'], '%m/%d/%Y'), reverse=True)
        return processed_days

    def calculate_change_from_previous(self, processed_data):
        if not processed_data:
            return processed_data
        # Get previous day's closing price from Sheet if exists
        records = self.get_all_records()
        previous_close = None
        if records:
            processed_dates = [datetime.strptime(d['Trade Day'], "%m/%d/%Y").date() for d in processed_data]
            for row in records:
                date_str = row.get('Trade Day')
                if date_str and '/' in date_str:
                    try:
                        row_date = datetime.strptime(date_str, "%m/%d/%Y").date()
                        if row_date < min(processed_dates):
                            closing_cell = row.get('Closing')
                            if closing_cell and isinstance(closing_cell, (int, float)):
                                previous_close = closing_cell
                                break
                    except (ValueError, TypeError):
                        continue
        for i in range(len(processed_data)):
            if i < len(processed_data) - 1:
                current_close = processed_data[i]['Closing']
                previous_close_val = processed_data[i + 1]['Closing']
                if current_close and previous_close_val:
                    change = round(current_close - previous_close_val, 2)
                    processed_data[i]['Change from previous day close'] = change
            elif previous_close is not None:
                current_close = processed_data[i]['Closing']
                change = round(current_close - previous_close, 2)
                processed_data[i]['Change from previous day close'] = change
        return processed_data

    def insert_new_data_at_top(self, processed_data):
        """
        Insert new rows below the header and two fixed rows in the 'Turnover' worksheet.
        Remove the current latest data row (row 4) before inserting new data.
        """
        if not processed_data:
            print("No new data to append.")
            return

        # Read all existing data (including header and fixed rows)
        all_values = self.sheet.get_all_values()
        if all_values:
            header = all_values[0]  # Row 1
            fixed_rows = all_values[1:3]  # Rows 2 and 3 (two fixed rows)
            existing_rows = all_values[3:]  # Data rows start from row 4
        else:
            header = self.headers
            fixed_rows = ["", ""]  # or some default empty rows if you want
            existing_rows = []

        # Remove the current latest data row (first data row) if it exists
        if existing_rows:
            removed_row = existing_rows.pop(0)
            print(f"Removed latest existing data row: {removed_row}")

        # Prepare new rows (newest first)
        new_rows = [[day_data.get(h, "") for h in self.headers] for day_data in processed_data]

        # Combine all rows: header + fixed rows + new rows + remaining existing rows
        updated_rows = [header] + fixed_rows + new_rows + existing_rows

        # Clear the worksheet and batch write all rows back
        self.sheet.clear()
        self.sheet.append_rows(updated_rows, value_input_option='USER_ENTERED')
        print(f"Inserted {len(new_rows)} new rows below the two fixed rows.")


# Usage Example:
if __name__ == "__main__":
    tracker = TurnoverTracker2()
    start_date, end_date = tracker.determine_date_range()
    if start_date and end_date:
        data = tracker.fetch_futu_data(start_date, end_date)
        processed = tracker.process_fetched_data(data)
        processed = tracker.calculate_change_from_previous(processed)
        tracker.insert_new_data_at_top(processed)
        print("Update complete!")
    else:
        print("No update required.")
