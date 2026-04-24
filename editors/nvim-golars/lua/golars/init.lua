-- nvim-golars: LSP attach + filetype defaults + `# ^?` probe
-- previews for .glr scripts.
--
-- Filetype detection lives in ftdetect/glr.lua so it fires at
-- startup regardless of lazy-loading. Syntax highlighting is
-- provided by the sibling syntax/glr.vim (loaded automatically
-- when the buffer filetype resolves to glr). Table previews below
-- `# ^?` comments are rendered by the preview submodule via
-- `golars --preview` subprocess calls.
--
-- setup() is called from the plugin's `config` hook, which under
-- lazy.nvim's `ft = "glr"` fires AFTER the FileType event has
-- already been dispatched on the current buffer. So we both
-- register an autocmd for future buffers AND sweep existing ones.
local M = {}

--- Attach LSP, inlay hints, and (optionally) probe previews to
--- every `.glr` buffer.
--- @param opts table|nil
---   cmd                 override LSP binary (default: { "golars-lsp" })
---   root_dir            function(bufnr) -> string (default: buffer dir)
---   preview             enable `# ^?` table previews (default: true)
---   preview_cmd         golars binary path for previews
---                       (default: { "golars" })
---   preview_rows        row cap for preview output (default: 10)
---   preview_timeout_ms  subprocess timeout in ms (default: 2000)
---   preview_debounce_ms debounce edits before re-previewing
---                       (default: 400)
function M.setup(opts)
  opts = opts or {}
  local cmd = opts.cmd or { "golars-lsp" }
  local root_dir = opts.root_dir
    or function(bufnr)
      local name = vim.api.nvim_buf_get_name(bufnr or 0)
      if name == "" then return vim.fn.getcwd() end
      return vim.fs.dirname(name)
    end

  local preview_enabled = opts.preview
  if preview_enabled == nil then preview_enabled = true end
  local preview_opts = {
    cmd = opts.preview_cmd or { "golars" },
    rows = opts.preview_rows,
    timeout = opts.preview_timeout_ms,
    debounce = opts.preview_debounce_ms,
  }

  local notified_missing = false
  local function attach(bufnr)
    -- Buffer defaults.
    vim.bo[bufnr].commentstring = "# %s"
    vim.bo[bufnr].expandtab = true
    vim.bo[bufnr].shiftwidth = 2
    vim.bo[bufnr].tabstop = 2

    -- LSP: tolerate a missing binary by warning once.
    if vim.fn.executable(cmd[1]) == 0 then
      if not notified_missing then
        vim.notify(
          "golars-lsp: binary not found (looked for `" .. cmd[1] .. "`)\n" ..
            "install with: go install github.com/Gaurav-Gosain/golars/cmd/golars-lsp@latest",
          vim.log.levels.WARN
        )
        notified_missing = true
      end
    else
      -- vim.lsp.start deduplicates by name + root_dir, so opening
      -- multiple .glr files in the same project shares one client.
      local client_id = vim.lsp.start({
        name = "golars-lsp",
        cmd = cmd,
        root_dir = root_dir(bufnr),
        filetypes = { "glr" },
      }, { bufnr = bufnr })

      -- Enable inlay hints for this buffer. Neovim 0.10+ surfaces
      -- the textDocument/inlayHint capability automatically, but the
      -- user still has to opt in. Flip it on per-buffer so the
      -- pipeline-shape annotations and `# ^?` probes render inline.
      if client_id and vim.lsp.inlay_hint ~= nil then
        pcall(vim.lsp.inlay_hint.enable, true, { bufnr = bufnr })
      end
    end

    -- Probe previews. Runs independently of the LSP so scripts still
    -- get table previews even on hosts where the LSP binary isn't
    -- installed.
    if preview_enabled then
      local ok, preview = pcall(require, "golars.preview")
      if ok then preview.attach(bufnr, preview_opts) end
    end
  end

  -- Future .glr buffers.
  vim.api.nvim_create_autocmd("FileType", {
    pattern = "glr",
    callback = function(ev) attach(ev.buf) end,
  })

  -- Already-open .glr buffers (the one that triggered lazy-loading
  -- plus any others Neovim restored from a session).
  for _, buf in ipairs(vim.api.nvim_list_bufs()) do
    if vim.api.nvim_buf_is_loaded(buf) and vim.bo[buf].filetype == "glr" then
      attach(buf)
    end
  end
end

return M
