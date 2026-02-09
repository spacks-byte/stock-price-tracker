# Hong Kong Stock Price Tracker

A Python script to generate CSV reports with Hong Kong stock data including opening/closing prices and percentage changes over various time periods. Features intelligent caching with Supabase to minimize API calls and improve performance.

## What This Project Does

This is a Hong Kong stock price tracker that:

1. **Fetches stock data** from Futu OpenAPI
2. **Caches everything** in a Supabase PostgreSQL database
3. **Intelligently queries** - checks database first, only calls API for missing data
4. **Calculates metrics** - percentage changes over 1, 5, 30, 60, and 120 days
5. **Exports to CSV** - generates dated CSV files with all metrics

## Features

- **Intelligent Caching**: Stores data in Supabase and checks database first before calling API
- **Efficient Data Fetching**: Only fetches missing date ranges from API
- Fetches real-time and historical data for Hong Kong stocks
- Calculates percentage changes over multiple time periods:
  - Yesterday
  - Last 5 days
  - Last 30 days
  - Last 60 days
  - Last 120 days
- Exports data to CSV format
- Uses Futu OpenAPI for reliable market data

### Intelligent Caching System

The script implements a smart caching layer:

```
User Request → Check Supabase DB → Missing Data? → Fetch from API → Update DB
                      ↓                                    ↓
                 Use Cached Data                      Return Combined Data
```

**Benefits:**
- First run: Fetches from API, saves to database (~5-10 seconds per stock)
- Subsequent runs: Uses cached data (< 1 second per stock)
- Only fetches missing dates from API (efficient!)

### Performance

**Without Caching (Every Run Calls API):**
- 5 stocks × 130 days = ~25 seconds
- 20 stocks × 130 days = ~100 seconds
- Rate limiting issues with many stocks

**With Caching (Subsequent Runs):**
- 5 stocks (cached) = ~2 seconds
- 20 stocks (cached) = ~5 seconds
- Only new days fetched from API

**Savings:** 90%+ reduction in execution time!

## Project Structure

```
stock-price-tracker/
│
├── stock_tracker.py          # Main Python script with caching logic
├── requirements.txt          # Python dependencies
├── .env.example             # Environment variables template
├── .env                     # Your actual credentials (gitignored)
├── .gitignore              # Git ignore rules
│
├── database_schema.sql     # SQL schema for Supabase
├── README.md              # This documentation
└── SETUP_GUIDE.md        # Step-by-step setup instructions
```

## Prerequisites

### 1. Supabase Setup

1. Create a free account at https://supabase.com
2. Create a new project
3. Run the database schema:
   - Go to SQL Editor in Supabase dashboard
   - Copy and run the contents of `database_schema.sql`
4. Get your credentials:
   - Project URL: Found in Project Settings > API
   - API Key: Use the `anon` public key from Project Settings > API

### 2. Install OpenD Gateway

The Futu OpenAPI requires the OpenD gateway program to be running on your computer.

1. Download OpenD: https://www.futunn.com/download/OpenAPI
2. Install and start OpenD
3. Log in with your Futu account
4. Default port is 11111 (can be changed in OpenD settings)

### 3. Futu Account

You need a Futu/Futubull account to use the API. Sign up at https://www.futunn.com/

### 4. Market Data Permissions

Ensure you have permissions for Hong Kong market data through your Futu account.

## Installation

1. Clone or download this repository

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Create environment file:
```bash
# Copy the example file
copy .env.example .env
```

4. Edit `.env` and add your credentials:
```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key-here
OPEND_HOST=127.0.0.1
OPEND_PORT=11111
```

## Usage

### Basic Usage

1. Start the OpenD gateway application
2. Run the script:
```bash
python stock_tracker.py
```

### Customize Stock List

Edit the `stock_codes` list in `stock_tracker.py`:

```python
stock_codes = [
    'HK.00700',  # Tencent
    'HK.00005',  # HSBC Holdings
    'HK.00941',  # China Mobile
    # Add your stocks here
]
```

### Programmatic Usage

You can also use the tracker in your own Python scripts:

```python
from stock_tracker import HKStockTracker

tracker = HKStockTracker()
df = tracker.generate_stock_report(['HK.00700', 'HK.00005'])
tracker.save_to_csv(df, 'my_stocks.csv')
tracker.close()
```

### Stock Code Format

Hong Kong stocks use the format: `HK.{5-digit code}`

Examples:
- Tencent: `HK.00700`
- HSBC: `HK.00005`
- China Mobile: `HK.00941`

### Output

The script generates a CSV file named `hk_stocks_YYYYMMDD.csv` with the following columns:

- Stock Code
- Stock Name
- Date
- Opening Price
- Closing Price
- % Change vs Yesterday
- % Change vs 5 Days
- % Change vs 30 Days
- % Change vs 60 Days
- % Change vs 120 Days

### How Caching Works

1. **First Run**: Fetches all data from Futu API and stores in Supabase
2. **Subsequent Runs**: 
   - Checks Supabase database first
   - Only fetches missing dates from API
   - Updates database with new data
3. **Benefits**:
   - Faster execution (no unnecessary API calls)
   - Reduces API rate limiting issues
   - Historical data preserved

## Database Schema

The Supabase table `stock_prices` has the following structure:

| Column | Type | Description |
|--------|------|-------------|
| id | bigint (auto) | Primary key |
| stock_code | text | Stock identifier (e.g., 'HK.00700') |
| date | date | Trading date |
| opening_price | double precision | Opening price |
| closing_price | double precision | Closing price |
| change_yesterday | double precision | % change vs 1 day |
| change_five | double precision | % change vs 5 days |
| change_thirty | double precision | % change vs 30 days |
| change_sixty | double precision | % change vs 60 days |
| change_onehundredtwenty | double precision | % change vs 120 days |
| created_at | timestamp | When record was created |
| updated_at | timestamp | When record was updated |

**Constraints:**
- Primary key: `id`
- Unique constraint: `(stock_code, date)` - prevents duplicate entries
- Indexes on `stock_code`, `date`, and `(stock_code, date)` for fast queries

## Core Components

### HKStockTracker Class

Main class that handles all operations:

```python
tracker = HKStockTracker()  # Connects to OpenD and Supabase
```

**Methods:**
- `get_cached_data()` - Retrieves data from Supabase
- `save_to_database()` - Stores new data in Supabase
- `get_missing_date_ranges()` - Identifies gaps in cached data
- `get_stock_data()` - Smart fetching with cache-first logic
- `calculate_percentage_change()` - Computes % changes
- `update_percentage_changes()` - Updates calculated metrics in DB
- `generate_stock_report()` - Creates full report for multiple stocks
- `save_to_csv()` - Exports to CSV file

### Dependencies

- `futu-api-py` - Futu OpenAPI Python SDK
- `pandas` - Data manipulation and CSV export
- `supabase` - Supabase Python client
- `python-dotenv` - Environment variable management

## How It Works

### Data Flow - First Run
```
1. User runs script
2. Script checks Supabase (empty)
3. Fetches all data from Futu API
4. Saves raw data to database
5. Calculates percentage changes
6. Updates database with calculations
7. Exports to CSV
```

### Data Flow - Subsequent Runs
```
1. User runs script
2. Script checks Supabase
3. Finds most data cached
4. Identifies missing dates (e.g., today's data)
5. Fetches only missing dates from API
6. Updates database
7. Combines cached + new data
8. Calculates percentage changes
9. Updates database with calculations
10. Exports to CSV
```

## Configuration

Environment variables in `.env`:

```env
# Required
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key

# Optional (defaults shown)
OPEND_HOST=127.0.0.1
OPEND_PORT=11111
```

You can also customize the connection programmatically:

```python
tracker = HKStockTracker(host='127.0.0.1', port=11111)
```

## Security

- **Environment Variables:** All credentials in `.env` (gitignored)
- **Supabase RLS:** Row Level Security policies protect data
- **API Keys:** Never committed to version control
- **Public Key:** Uses Supabase `anon` key (safe for client-side)

## Future Enhancements

Potential improvements:

- [ ] Web dashboard for visualization
- [ ] Email alerts for significant price changes
- [ ] Support for US stocks and other markets
- [ ] Scheduled daily updates (cron job)
- [ ] Technical indicators (RSI, MACD, etc.)
- [ ] Portfolio tracking
- [ ] Real-time updates via WebSocket

## Troubleshooting

### "Missing Supabase credentials"
- Ensure `.env` file exists in the project root
- Check that `SUPABASE_URL` and `SUPABASE_KEY` are set correctly

### "Error: Connection refused"
- Ensure OpenD gateway is running
- Check that the port matches (default: 11111)

### "No data available"
- Verify you have permissions for HK market data
- Check that the stock code is correct
- Ensure the stock is trading

### Database Errors
- Verify the database schema is properly created in Supabase
- Check that Row Level Security policies allow your operations
- Ensure your API key has correct permissions

### Rate Limiting
The script includes delays between requests to avoid rate limiting. If you need to process many stocks, the caching system will significantly reduce API calls on subsequent runs.

## Resources

- **Futu OpenAPI Docs:** https://openapi.futunn.com/futu-api-doc/en/intro/intro.html
- **Supabase Docs:** https://supabase.com/docs
- **Python-dotenv:** https://pypi.org/project/python-dotenv/
- **Pandas:** https://pandas.pydata.org/docs/
- **Hong Kong Stock Exchange:** https://www.hkex.com.hk/

## License

This project is for educational purposes. Please comply with Futu OpenAPI terms of service and Supabase usage policies.
