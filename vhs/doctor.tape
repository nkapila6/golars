# `golars doctor`: environment diagnostic. Short but useful for
# setup-help screenshots.

Output vhs/out/doctor.gif

Source vhs/_common.tape
Set Height 620
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "golars doctor"
Sleep 300ms
Enter
Sleep 3s

Type "golars version"
Sleep 300ms
Enter
Sleep 1500ms
