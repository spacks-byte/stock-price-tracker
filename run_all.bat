@echo off
echo ================================================================
echo Running HSI Turnover Updater and Stock Price Tracker
echo ================================================================
echo.

echo [1/2] Running HSI Turnover Updater...
echo ----------------------------------------------------------------
cd /d "%~dp0projectfiles"
python HSITurnoverUpdater.py
if %errorlevel% neq 0 (
    echo ERROR: HSI Turnover Updater failed!
    pause
    exit /b %errorlevel%
)
echo.
echo HSI Turnover Updater completed successfully!
echo.

echo [2/2] Running Stock Price Tracker...
echo ----------------------------------------------------------------
python stock_tracker.py
if %errorlevel% neq 0 (
    echo ERROR: Stock Price Tracker failed!
    pause
    exit /b %errorlevel%
)
echo.
echo ================================================================
echo All tasks completed successfully!
echo ================================================================
pause
