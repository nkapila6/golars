# Composability: golars as a Unix-friendly tool. Shows the
# output-format switch working inside shell pipelines with jq, awk,
# and a round-trip back through golars.

Output vhs/out/pipes.gif

Source vhs/_common.tape
Require golars
Require jq
Require awk

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

# NDJSON out, jq filters down. Standard data-eng pipeline.
Type `golars sql --ndjson 'SELECT dept, name, salary FROM people' vhs/fixtures/people.csv \`
Sleep 100ms
Enter
Type `  | jq 'select(.salary > 130000)'`
Sleep 400ms
Enter
Sleep 2800ms

Type "clear"
Enter

# CSV out, awk for a quick sum across all rows.
Type `golars sql --csv 'SELECT salary FROM people' vhs/fixtures/people.csv \`
Sleep 100ms
Enter
Type `  | awk -F, 'NR>1 {s+=$1} END {print s}'`
Sleep 400ms
Enter
Sleep 2200ms

Type "clear"
Enter

# Round-trip: CSV to Parquet, peek back out as markdown.
Type "golars convert vhs/fixtures/people.csv /tmp/people.parquet"
Sleep 300ms
Enter
Sleep 1200ms
Type "golars peek -o markdown /tmp/people.parquet 4"
Sleep 300ms
Enter
Sleep 2s
