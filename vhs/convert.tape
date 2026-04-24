# Format conversion: csv to parquet to ndjson to arrow round trips.

Output vhs/out/convert.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "golars convert vhs/fixtures/people.csv /tmp/people.parquet"
Sleep 300ms
Enter
Sleep 1200ms

Type "golars convert /tmp/people.parquet /tmp/people.ndjson"
Sleep 300ms
Enter
Sleep 1200ms

Type "head -3 /tmp/people.ndjson"
Sleep 300ms
Enter
Sleep 2s

Type "clear"
Enter

Type "golars convert /tmp/people.ndjson /tmp/people.arrow"
Sleep 300ms
Enter
Sleep 1200ms

Type "golars schema /tmp/people.arrow"
Sleep 300ms
Enter
Sleep 2500ms
