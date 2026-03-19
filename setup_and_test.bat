@echo off
REM =============================================================================
REM Agentic Event Store - Setup and Test Script (Windows)
REM The Ledger - Automated Installation & Testing
REM =============================================================================

REM Set Python path - update if your Python is in different location
set PYTHON=C:\Users\think\AppData\Local\Programs\Python\Python311\python.exe

echo.
echo ============================================================
echo   Agentic Event Store - Setup and Test
echo ============================================================
echo.

REM Check if Python is installed
echo [1/6] Checking Python installation...
"%PYTHON%" --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed at %PYTHON%
    echo Please install Python 3.11+ or update PYTHON path in this script
    pause
    exit /b 1
)
echo   Python found!

REM Check if PostgreSQL is installed
echo [2/6] Checking PostgreSQL installation...
where psql >nul 2>&1
if errorlevel 1 (
    echo WARNING: PostgreSQL not found in PATH
    echo.
    echo Option 1: Install PostgreSQL from https://www.postgresql.org/download/
    echo.
    echo Option 2: Use Docker:
    echo   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name apex_postgres postgres:15
    echo.
    set /p USE_DOCKER=Use Docker to start PostgreSQL? (Y/N): 
    if /i "%USE_DOCKER%"=="Y" (
        echo Starting PostgreSQL with Docker...
        docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name apex_postgres postgres:15
        echo   Waiting for PostgreSQL to start...
        timeout /t 15 /nobreak >nul
        goto :setup_db
    )
    pause
    exit /b 1
)
echo   PostgreSQL found!

:setup_db
REM Create databases using SQL
echo [3/6] Setting up PostgreSQL databases...
py -c "import psycopg2; print('psycopg2 available')" >nul 2>&1
if errorlevel 1 (
    pip install psycopg2-binary --quiet
)

REM Try to create databases
"%PYTHON%" -c "
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    conn = psycopg2.connect(host='localhost', user='postgres', password='postgres')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Create apex_ledger
    try:
        cur.execute('CREATE DATABASE apex_ledger')
        print('Created apex_ledger database')
    except:
        pass

    # Create apex_ledger_test
    try:
        cur.execute('CREATE DATABASE apex_ledger_test')
        print('Created apex_ledger_test database')
    except:
        pass

    cur.close()
    conn.close()
    print('Databases ready!')
except Exception as e:
    print(f'Could not connect to PostgreSQL: {e}')
" >nul 2>&1

echo   Databases ready!

REM Install Python dependencies
echo [4/6] Installing Python dependencies...
"%PYTHON%" -m pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
pip install pytest pytest-asyncio pytest-cov psycopg2-binary --quiet
echo   Dependencies installed!

REM Run unit tests
echo [5/6] Running unit tests...
echo.
echo ----------------------------------------------------------
echo   UNIT TESTS (no database required)
echo ----------------------------------------------------------
"%PYTHON%" -m pytest tests/test_integrity.py -v --tb=short

if errorlevel 1 (
    echo.
    echo UNIT TESTS FAILED!
    pause
    exit /b 1
)

REM Run integration tests
echo.
echo ----------------------------------------------------------
echo   INTEGRATION TESTS (requires PostgreSQL)
echo ----------------------------------------------------------
"%PYTHON%" -m pytest tests/test_event_store.py -v --tb=short

if errorlevel 1 (
    echo.
    echo INTEGRATION TESTS FAILED!
    echo.
    echo Troubleshooting:
    echo   - Make sure PostgreSQL is running
    echo   - Check credentials: user=postgres, password=postgres
    echo   - Start PostgreSQL: pg_ctl -D "C:\Program Files\PostgreSQL\15\data" start
    echo.
    pause
    exit /b 1
)

echo [6/6] All tests completed!
echo.
echo ============================================================
echo   SUCCESS - All tests passed!
echo ============================================================
echo.
echo To run the event store:
echo   py -m ledger.server
echo.
echo To run with custom database:
echo   set DB_HOST=localhost DB_NAME=your_db
echo   py -m ledger.server
echo.
pause
exit /b 0
