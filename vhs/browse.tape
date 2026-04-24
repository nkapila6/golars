# Interactive TUI browser demo. Shows scroll, filter, sort, column
# hide, column freeze. Uses VHS key events to drive the Bubble Tea
# program without a human in the loop.

Output vhs/out/browse.gif

Source vhs/_common.tape
Set Width 1320
Set Height 780
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 400ms

Type "golars browse vhs/fixtures/people.csv"
Sleep 400ms
Enter
Sleep 1500ms

# Scroll down a few rows.
Down
Sleep 200ms
Down
Sleep 200ms
Down
Sleep 400ms

# Jump across columns.
Right
Sleep 250ms
Right
Sleep 400ms

# Sort ascending on salary.
Type "s"
Sleep 1200ms

# Flip to descending.
Type "s"
Sleep 1200ms

# Freeze the salary column so it stays on screen while we scroll.
Type "f"
Sleep 800ms

# Filter to engineering rows.
Type "/"
Sleep 400ms
Type "eng"
Sleep 400ms
Enter
Sleep 2s

# Pop the filter back off.
Escape
Sleep 1s

# Quit.
Type "q"
Sleep 500ms
