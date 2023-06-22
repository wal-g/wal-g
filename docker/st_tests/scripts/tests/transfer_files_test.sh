#!/bin/bash
set -e -x

CONFIG="/tmp/configs/transfer_files_test_config.json"
TESTDATA="transfer_files"

echo "Upload 50 random files to the failover storage"
mkdir $TESTDATA
for i in {1..50}
do
  head -c 1M </dev/urandom >"$TESTDATA/$i"
  wal-g --config=$CONFIG st put "$TESTDATA/$i" "a/b/$i" --target=failover
done

echo "Upload some garbage files to the failover storage, which aren't to be transferred"
garbage=( "1" "a" "aa/3" "b/4" )
for file in "${garbage[@]}"
do
  mkdir -p "$(dirname "$TESTDATA/garbage/$file")"
  head -c 1M </dev/urandom >"$TESTDATA/garbage/$file"
  wal-g --config=$CONFIG st put "$TESTDATA/garbage/$file" "$file" --target=failover
done

echo "Ensure there's no files in the primary storage initially"
test "1" -eq "$(wal-g --config=$CONFIG st ls -r "a/" | wc -l)"

echo "Also upload only some of the target files to the primary storage"
for i in 1 3 7 15 25 34 50
do
  head -c 1M </dev/urandom >"$TESTDATA/$i"
  wal-g --config=$CONFIG st put "$TESTDATA/$i" "a/b/$i"
done

echo "Call the command to transfer files from the failover storage to the primary one"
wal-g --config=$CONFIG st transfer files "a/" --source=failover --target=default

echo "Check that all the target files are moved to the primary storage"
wal-g --config=$CONFIG st ls -r "a/b/"
test "51" -eq "$(wal-g --config=$CONFIG st ls -r "a/b/" | wc -l)"

echo "Check that garbage files aren't moved to the primary storage"
for file in "${garbage[@]}"
do
  wal-g --config=$CONFIG st check read "$file.br" && EXIT_STATUS=$? || EXIT_STATUS=$?
  test "1" -eq $EXIT_STATUS
done
