# "What's in this file?" Shows schema, peek, and stats.

Output vhs/out/inspect.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "# Column names + dtypes"
Sleep 250ms
Enter
Type "golars schema vhs/fixtures/people.csv"
Enter
Sleep 1800ms

Type "clear"
Enter

Type "# Schema + head + shape in one call"
Sleep 250ms
Enter
Type "golars peek vhs/fixtures/people.csv 5"
Enter
Sleep 2500ms

Type "clear"
Enter

Type "# Per-column describe() stats"
Sleep 250ms
Enter
Type "golars stats vhs/fixtures/people.csv"
Enter
Sleep 3s
