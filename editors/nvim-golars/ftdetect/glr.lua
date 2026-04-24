-- Filetype detection for golars .glr scripts. Runtime-loaded so it
-- fires before any plugin logic. lazy.nvim sources ftdetect/*.lua
-- automatically when the plugin's runtimepath is added.
vim.filetype.add({ extension = { glr = "glr" } })
