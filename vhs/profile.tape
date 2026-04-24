# `golars explain`: lazy plan, optimiser trace, optimised plan.
# Shows both the indented form and the box-drawn tree.

Output vhs/out/profile.gif

Source vhs/_common.tape
Require golars

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "cat vhs/fixtures/pipeline.glr"
Sleep 300ms
Enter
Sleep 2500ms

Type "clear"
Enter

# Tree-drawn plan: box-drawing connectors make nesting visible at a
# glance. Swap --tree for the indented form seen in polars' own
# .explain().
Type "golars explain --tree vhs/fixtures/pipeline.glr"
Sleep 400ms
Enter
Sleep 4500ms

Type "clear"
Enter

# --profile collects per-node wall-time alongside the plan.
Type "golars explain --profile vhs/fixtures/pipeline.glr"
Sleep 400ms
Enter
Sleep 4500ms
