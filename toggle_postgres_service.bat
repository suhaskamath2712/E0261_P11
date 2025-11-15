REM Toggle PostgreSQL service on/off
REM Check if PostgreSQL service is running
sc query postgresql-x64-17 | find "RUNNING" > nul
if %errorlevel%==0 (
    REM If running, stop the service
    echo Stopping PostgreSQL service...
    net stop postgresql-x64-17
) else (
    REM If not running, start the service
    echo Starting PostgreSQL service...
    net start postgresql-x64-17
)