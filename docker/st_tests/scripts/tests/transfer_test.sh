#!/bin/sh
set -e -x

CONFIG="/tmp/configs/transfer_test_config.json"
TESTDATA="transfer"

# Upload 50 random files to the failover storage
mkdir transfer
counter=1
while [ $((counter)) -le 50 ]
do
  head -c 1M </dev/urandom >"$TESTDATA/$counter"
  wal-g --config=$CONFIG st put "$TESTDATA/$counter" "a/b/$counter" --target=failover
  counter=$((counter+1))
done

# Upload some garbage files to the failover storage, which aren't to be transferred
garbage="1 a/2 aa/3 b/4"
echo "$garbage" | tr ' ' '\n' | while read -r file; do
  mkdir -p "$(dirname "$TESTDATA/garbage/$file")"
  head -c 1M </dev/urandom >"$TESTDATA/garbage/$file"
  wal-g --config=$CONFIG st put "$TESTDATA/garbage/$file" "$file" --target=failover
done

# Ensure there's no files in the primary storage initially
test "1" -eq "$(wal-g --config=$CONFIG st ls -r "a/" | wc -l)"

# Also upload every 10th of the target files to the primary storage
counter=1
while [ $((counter)) -le 50 ]
do
  head -c 10M </dev/urandom >"$TESTDATA/$counter"
  wal-g --config=$CONFIG st put "$TESTDATA/$counter" "a/b/$counter"
  counter=$((counter+10))
done

# Call the command to transfer files from the failover storage to the primary one
wal-g --config=$CONFIG st transfer "a/" --source=failover --target=default

# Check that all the target files are moved to the primary storage
wal-g --config=$CONFIG st ls -r "a/b/"
test "51" -eq "$(wal-g --config=$CONFIG st ls -r "a/b/" | wc -l)"

# Check that garbage files aren't moved to the primary storage
echo "$garbage" | tr ' ' '\n' | while read -r file; do
  wal-g --config=$CONFIG st check read "$file" || test "1" -eq $?
done
