#!/bin/bash
DB_PATH="/home/cusco/git/cardmarket_prices/src/db.sqlite3"
FREE_SPACE=$(df $DB_PATH --output=avail | tail -n1 | awk '{print int($1/1024/1024)}')

echo "Checking disk space..."
if [ "$FREE_SPACE" -lt 16 ]; then
    echo "ERROR: Only ${FREE_SPACE}GB free. Need at least 16GB for VACUUM."
    exit 1
fi

echo "Updating Statistics (ANALYZE)..."
sqlite3 "$DB_PATH" "ANALYZE;"

echo "Defragmenting File (VACUUM)..."
sqlite3 "$DB_PATH" "VACUUM;"

echo "Final Optimization..."
sqlite3 "$DB_PATH" "PRAGMA optimize;"

echo "Done. Current size: $(du -sh $DB_PATH | cut -f1)"
