# golars-mcp: the Model Context Protocol server. Drives the tool
# over stdin with raw JSON-RPC so the gif shows the actual wire
# protocol a host like Claude Desktop would use.

Output vhs/out/mcp.gif

Source vhs/_common.tape
Require golars-mcp
Require jq

Hide
Type "export PS1='\033[38;5;141m>\033[0m '"
Enter
Type "clear"
Enter
Show

Sleep 300ms

Type "# 1. Discover the tool catalogue."
Sleep 300ms
Enter
Type "cat vhs/fixtures/mcp-list.json | golars-mcp | jq '.result.tools[] | {name, description}'"
Sleep 400ms
Enter
Sleep 4s

Type "clear"
Enter

Type "# 2. Call schema on a data file."
Sleep 300ms
Enter
Type "cat vhs/fixtures/mcp-schema.json | golars-mcp | jq -r '.result.content[0].text'"
Sleep 400ms
Enter
Sleep 3500ms
