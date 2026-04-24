# nvim-golars

Neovim language support for the golars `.glr` scripting language.
Registers the filetype, default buffer options, and the
`golars-lsp` language server.

What you get:

* Inline **diagnostics** for unknown commands and missing required args
* **Completions** for commands, keywords (`as`, `on`, `asc`, `desc`, `inner`, `left`, `cross`), staged frame names (from earlier `load ... as NAME` in the same file), and filesystem paths after `load` / `save` / `join` / `source`
* **Hover** docs with signature + full description on any command token
* Proper `commentstring = "# %s"` so `gcc` / `gc{motion}` comment the right way

## Install (lazy.nvim, from a local checkout)

`~/.config/nvim/lua/plugins/golars.lua`:

```lua
return {
  {
    dir = vim.fn.expand("~/dev/golars/editors/nvim-golars"),
    name = "nvim-golars",
    ft = "glr",
    config = function()
      require("golars").setup({})
    end,
  },
}
```

If `golars-lsp` isn't on `$PATH`, pass an absolute path:

```lua
require("golars").setup({
  cmd = { vim.fn.expand("~/go/bin/golars-lsp") },
})
```

## Install the binaries

```sh
go install github.com/Gaurav-Gosain/golars/cmd/golars@latest
go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest
```

For local development (repo checkout), run from the repo root:

```sh
go install ./cmd/golars ./cmd/golars-lsp
```

## Tree-sitter highlighting

Grammar and highlight queries live at
[`../tree-sitter-golars/`](../tree-sitter-golars/). Install
separately per the README there; the LSP plugin works without it but
syntax highlighting is much nicer once you have it.

## Verify

Open any `.glr` file and check `:LspInfo`: you should see
`golars-lsp` attached. Type a bad command (e.g. `fooz bar`) and the
sign column should show a diagnostic. Hover over a known command
with `K` and the signature + doc pop up.

If nothing happens, check `:checkhealth vim.lsp` and the server's
stderr via:

```sh
tail -f ~/.local/state/nvim/lsp.log
```
