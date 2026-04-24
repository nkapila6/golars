# Zed Extension for golars

Syntax highlighting, LSP client, and editor features for the golars `.glr` scripting language.

## Features

- **Syntax Highlighting**: Full tree-sitter grammar support for the `.glr` scripting language
- **LSP Client**: Connects to `golars-lsp` for completions, diagnostics, hover, and more
- **Language Server**: Uses `stdio` transport to communicate with the `golars-lsp` binary
- **Editor Features**: Comment toggling, bracket matching, code outline

## Prerequisites

Install the language server binary:

```bash
go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest
```

## Installation

### Quick Setup (curl)

This script downloads the extension source to your machine. You then install it as a dev extension in Zed:

```bash
curl -fsSL https://raw.githubusercontent.com/Gaurav-Gosain/golars/main/editors/zed-golars/install.sh | bash
```

After the download completes, open Zed: **Extensions** → **Install Dev Extension** → select the printed directory path.

### Manual Dev Extension

1. Open Zed
2. Go to **Extensions** (`Cmd+Shift+X` / `Ctrl+Shift+X`)
3. Click **"Install Dev Extension"**
4. Navigate to and select the `editors/zed-golars/` directory from this repository

## Configuration

You can configure a custom path to the `golars-lsp` binary in your Zed settings:

```json
{
  "lsp": {
    "golars-lsp": {
      "binary": {
        "path": "/path/to/golars-lsp"
      }
    }
  }
}
```

## File Structure

```
zed-golars/
├── extension.toml          # Extension manifest
├── Cargo.toml              # Rust crate for LSP WASM extension
├── src/
│   └── lib.rs              # LSP client implementation
├── languages/
│   └── golars/
│       ├── config.toml     # Language configuration
│       ├── highlights.scm  # Syntax highlighting queries
│       ├── brackets.scm    # Bracket matching
│       ├── indents.scm     # Indentation rules
│       ├── outline.scm     # Code outline
│       └── injections.scm  # Language injection rules
└── LICENSE                 # MIT License
```

## Development

To build the extension locally:

```bash
cd editors/zed-golars
cargo build --target wasm32-wasip1 --release
```

To debug, launch Zed with verbose logging:

```bash
zed --foreground
```

## Grammar Source

The tree-sitter grammar lives at [nkapila6/tree-sitter-golars](https://github.com/nkapila6/tree-sitter-golars).

## License

MIT - see LICENSE file