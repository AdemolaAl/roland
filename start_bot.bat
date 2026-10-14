@echo off
REM Auto Approval Bot Starter Script for Windows

echo 🤖 Starting Auto Approval Bot...

REM Check if .env file exists
if not exist .env (
    echo ❌ Error: .env file not found!
    echo Please create a .env file with your bot credentials
    echo Copy .env.example to .env and fill in your values
    pause
    exit /b 1
)

REM Check if virtual environment exists
if not exist venv (
    echo 📦 Creating virtual environment...
    python -m venv venv
)

REM Activate virtual environment
echo 📦 Activating virtual environment...
call venv\Scripts\activate

REM Check if requirements are installed
python -c "import pyrogram" 2>nul || (
    echo 📥 Installing requirements...
    pip install -r requirements.txt
)

REM Load environment variables from .env file
echo 🔧 Loading environment variables...
for /f "usebackq tokens=*" %%i in (.env) do (
    set "line=%%i"
    if not "!line:~0,1!"=="#" (
        if not "!line!"=="" (
            set "%%i"
        )
    )
)

REM Start the bot
echo 🚀 Starting bot...
python bot.py

pause