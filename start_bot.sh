#!/bin/bash

# Auto Approval Bot Starter Script

echo "🤖 Starting Auto Approval Bot..."

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found!"
    echo "Please create a .env file with your bot credentials"
    echo "Copy .env.example to .env and fill in your values"
    exit 1
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo "❌ Python is not installed. Please install Python 3.8+ first."
    exit 1
fi

# Use python3 if available, otherwise python
PYTHON_CMD=$(command -v python3 || command -v python)

# Check if virtual environment exists
if [ ! -d "env" ]; then
    echo "📦 Creating virtual environment..."
    $PYTHON_CMD -m venv env
fi

# Activate virtual environment based on OS
echo "📦 Activating virtual environment..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" ]]; then
    # Windows (Git Bash/MSYS)
    source env/Scripts/activate 2>/dev/null || {
        echo "❌ Failed to activate virtual environment on Windows"
        echo "Try running: source env/Scripts/activate"
        exit 1
    }
else
    # Linux/Mac
    source env/bin/activate 2>/dev/null || {
        echo "❌ Failed to activate virtual environment"
        echo "Try running: source env/bin/activate"
        exit 1
    }
fi

# Check if requirements are installed
if ! python -c "import pyrogram" 2>/dev/null; then
    echo "📥 Installing requirements..."
    pip install -r requirements.txt
fi

# Load environment variables using a different approach that handles multi-line values
echo "🔧 Loading environment variables..."
if [ -f .env ]; then
    # This method properly handles multi-line variables
    set -a  # automatically export all variables
    source .env
    set +a  # turn off automatic export
else
    echo "❌ Failed to load .env file"
    exit 1
fi

# Verify critical environment variables
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ Error: BOT_TOKEN not set in .env file"
    exit 1
fi

if [ -z "$API_ID" ]; then
    echo "❌ Error: API_ID not set in .env file"
    exit 1
fi

if [ -z "$API_HASH" ]; then
    echo "❌ Error: API_HASH not set in .env file"
    exit 1
fi

# Start the bot
echo "🚀 Starting bot..."
echo "📡 Bot Name: ${BOT_NAME:-Auto Approval Bot}"
echo "🔗 Channel ID: ${CHID:-Not Set}"
echo "👤 Admin IDs: ${SUDO:-Not Set}"
echo "----------------------------------------"

# Run the bot with proper error handling
python bot.py || {
    echo "❌ Bot crashed or stopped"
    exit 1
}