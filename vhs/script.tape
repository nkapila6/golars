# `.glr` scripting: pipe-style commands, one per line. Shows
# the non-interactive runner.

Output vhs/out/script.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "bat --style=plain vhs/fixtures/pipeline.glr 2>/dev/null || cat vhs/fixtures/pipeline.glr"
Sleep 300ms
Enter
Sleep 2500ms

Type "clear"
Enter

Type "golars run vhs/fixtures/pipeline.glr"
Sleep 400ms
Enter
Sleep 3500ms
