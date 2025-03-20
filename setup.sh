#!/bin/bash

# Step 1: Create the virtual environment
uv venv

# Step 2: Activate the virtual environment
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
else
    echo "Error: Virtual environment not found at .venv/bin/activate"
    exit 1
fi

# Step 3: Install dependencies
uv sync || { echo "Failed to sync dependencies"; exit 1; }

# Step 4: Remove any old .env, then create a new .env file
rm -f .env
touch .env

# Step 5: Run exportpathlinux.py to retrieve DAGSTER_HOME
BASH_VERSION_MAJOR=$(echo "$BASH_VERSION" | cut -d. -f1)

if [ "$BASH_VERSION_MAJOR" -ge 4 ]; then
    # Use readarray for Bash 4+
    readarray -t PATHS < <(uv run scripts/exportpathlinux.py)
else
    # Fallback for older Bash versions (macOS default Bash 3.2)
    PATHS=()
    while IFS= read -r line; do
        PATHS+=("$line")
    done < <(uv run scripts/exportpathlinux.py)
fi

DAGSTER_HOME="${PATHS[0]}"

if [ -z "$DAGSTER_HOME" ]; then
    echo "Error: Failed to retrieve DAGSTER_HOME"
    exit 1
fi

# Step 6: Append DAGSTER_HOME to .env
echo "DAGSTER_HOME=$DAGSTER_HOME" >> .env

# Step 7: Generate dagster.yaml in DAGSTER_HOME
mkdir -p "$DAGSTER_HOME"
uv run scripts/generate_dagsteryaml.py "$DAGSTER_HOME" > "$DAGSTER_HOME/dagster.yaml"

# Step 8: Launch Dagster development server
echo "Starting Dagster development server..."
dagster dev
