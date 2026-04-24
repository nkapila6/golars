# Hero tape. The first thing a README visitor sees.
# Pitch in ~20 seconds: run a SQL query, switch the output format,
# browse the file interactively. No voice-over needed.
#
# Regenerate: `vhs vhs/hero.tape`   (outputs vhs/out/hero.gif)

Output vhs/out/hero.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 400ms

# 1. A SQL query over a CSV file. The table that pops out makes the
#    point faster than any paragraph could.
Type `golars sql 'SELECT symbol, SUM(qty) AS vol FROM trades`
Sleep 100ms
Enter
Type `            GROUP BY symbol ORDER BY vol DESC' vhs/fixtures/trades.csv`
Sleep 400ms
Enter
Sleep 2s

# 2. Same query, piped as NDJSON. Shows the -o flag in action.
Type "clear"
Enter
Type `golars sql --ndjson 'SELECT symbol, SUM(qty) AS vol FROM trades`
Sleep 100ms
Enter
Type `                   GROUP BY symbol' vhs/fixtures/trades.csv`
Sleep 400ms
Enter
Sleep 2s

# 3. Interactive TUI browser. Sort, filter, hide columns without
#    ever leaving the terminal.
Type "clear"
Enter
Type "golars head -o table vhs/fixtures/people.csv 8"
Sleep 400ms
Enter
Sleep 3s
