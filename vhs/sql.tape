# SQL frontend demo. Shows a WHERE/GROUP BY/ORDER BY query and the
# output-format switch that makes the CLI pipe-friendly.

Output vhs/out/sql.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "golars schema vhs/fixtures/trades.csv"
Sleep 300ms
Enter
Sleep 1500ms

Type `golars sql 'SELECT symbol, side, SUM(qty) AS qty FROM trades`
Sleep 100ms
Enter
Type `            WHERE price > 200`
Sleep 100ms
Enter
Type `            GROUP BY symbol, side ORDER BY qty DESC' vhs/fixtures/trades.csv`
Sleep 400ms
Enter
Sleep 2500ms

Type "clear"
Enter

# The --csv shortcut pipes straight into awk/xsv/jq/whatever.
Type "golars sql --csv 'SELECT DISTINCT symbol FROM trades' vhs/fixtures/trades.csv | head"
Sleep 400ms
Enter
Sleep 2s

Type "clear"
Enter

# --markdown for copy-into-a-PR workflows.
Type "golars sql --markdown 'SELECT symbol, COUNT(ts) AS n FROM trades GROUP BY symbol' vhs/fixtures/trades.csv"
Sleep 400ms
Enter
Sleep 2s
