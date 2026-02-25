# Stock Price Tracker

Hong Kong Stock Price Tracker and HSI Turnover Updater with Google Sheets integration.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure credentials:**
   - Copy `projectfiles/config.example.py` to `projectfiles/config.py`
   - Edit `config.py` with your Google Sheets URL and credentials file name
   - Place your Google credentials JSON file in the `projectfiles/` folder

3. **Add your stock list:**
   - Edit `projectfiles/fullstocks.txt` with your stock codes

4. **Make sure OpenD is running:**
   - For Futu: Download from https://www.futunn.com/download/OpenAPI
   - For MooMoo: Download from https://www.moomoo.com/

## Usage

### Run Everything (Recommended)
```batch
run_all.bat
```
Runs both HSI Turnover Updater and Stock Price Tracker in sequence.

### Run Individual Scripts
```batch
run_hsi_turnover.bat     # HSI Turnover only
run_stockpricing.bat     # Stock Price Tracker only
```

## Configuration

### OpenD Type
Edit `projectfiles/stock_tracker.py` line 20:
- `USE_MOOMOO = False` for Futu OpenD (default)
- `USE_MOOMOO = True` for MooMoo OpenD

### MooMoo Users
If using MooMoo OpenD and hitting quota limits:
- Reduce `FETCH_DAYS` (line 25) to 60 or 30 days
- Increase `RATE_LIMIT_DELAY` (line 24) to 3.0+ seconds
- Process fewer stocks at a time

## Files Not Committed to Git

For security, these files are in `.gitignore`:
- `projectfiles/config.py` - Your Google Sheets configuration
- `*.json` - Your Google credentials file
- `projectfiles/fullstocks.txt` - Your stock list (optional)

## Features

- Fetches real-time stock data from Futu/MooMoo OpenD
- Calculates percentage changes: 1, 3, 5, 30, 60, 120 days
- Color-coded results: Green for +10%+, Red for -5% or worse
- Automatic sorting by daily performance
- Location tracking (which broker holds each stock)
- Missing stock reporting with error details
