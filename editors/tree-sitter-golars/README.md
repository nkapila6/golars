# tree-sitter-golars

Tree-sitter grammar for the golars `.glr` scripting language. See
[`docs/scripting.md`](../../docs/scripting.md) for the language itself.

Ships: `grammar.js` + `queries/highlights.scm` + a minimal
`package.json`. Generate the parser with the `tree-sitter` CLI.

## Install (Neovim via nvim-treesitter)

Add a local parser config to your nvim setup:

```lua
-- in ~/.config/nvim/lua/plugins/golars.lua (lazy.nvim) or similar
require('nvim-treesitter.parsers').get_parser_configs().golars = {
  install_info = {
    url = "/path/to/golars/editors/tree-sitter-golars",  -- local path OK
    files = { "src/parser.c" },
    branch = "main",
    generate_requires_npm = true,
    requires_generate_from_grammar = true,
  },
  filetype = "glr",
}

vim.filetype.add({
  extension = { glr = "glr" },
})
```

Then `:TSInstall golars`. Copy `queries/highlights.scm` to
`~/.config/nvim/queries/golars/highlights.scm` so Neovim finds them.

## Install (Helix)

Add to `~/.config/helix/languages.toml`:

```toml
[[language]]
name = "glr"
scope = "source.golars"
file-types = ["glr"]
comment-token = "#"
roots = []
indent = { tab-width = 2, unit = "  " }

[[grammar]]
name = "glr"
source = { path = "/path/to/golars/editors/tree-sitter-golars" }
```

Then `hx --grammar fetch && hx --grammar build`. Copy the queries to
`~/.config/helix/runtime/queries/glr/`.

## Install (VS Code)

VS Code's built-in highlighting doesn't use tree-sitter yet. For a
lightweight TextMate-based alternative, see this grammar's token
names: they map cleanly to TextMate scopes (`@keyword` →
`keyword.control.golars`, `@string` → `string.quoted.double.golars`,
etc.). A full VS Code extension isn't in this repo; contributions
welcome.

## Build + test locally

```sh
cd editors/tree-sitter-golars
npm install
npx tree-sitter generate
npx tree-sitter test                  # runs tests in corpus/ (not included yet)
npx tree-sitter parse ../../examples/script/multisource.glr
```

## Status

Grammar is complete enough to highlight every sample in
`examples/script/`. Extending for new commands is a one-line change
to the `command` rule in `grammar.js`.
