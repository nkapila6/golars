# File-level diff with a key column.

Output vhs/out/diff.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "# v2 of the same trade log: one qty bumped, one new row."
Sleep 300ms
Enter
Type "diff <(head -3 vhs/fixtures/trades.csv) <(head -3 vhs/fixtures/trades-v2.csv)"
Sleep 300ms
Enter
Sleep 1200ms

Type "clear"
Enter

Type "# Use the timestamp as the row key so we see per-trade changes."
Sleep 300ms
Enter
Type "golars diff --key ts vhs/fixtures/trades.csv vhs/fixtures/trades-v2.csv"
Sleep 400ms
Enter
Sleep 3500ms
