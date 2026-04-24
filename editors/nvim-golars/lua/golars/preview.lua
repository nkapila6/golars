-- Renders `# ^?` probe previews as virtual text below the probe
-- line. The probe reveals the focused pipeline's current table at
-- that point in the script by spawning `golars --preview` with the
-- prefix of the script up to (but not including) the probe line.
--
-- The spawn is debounced and time-bounded so a slow or runaway
-- script doesn't stall the editor. Output is rendered as multi-line
-- virtual text via extmarks. No buffer text is modified.

local M = {}

local api = vim.api
local ns = api.nvim_create_namespace("golars.preview")

-- defaults (overridable via setup_buffer(bufnr, opts))
local DEFAULT_CMD = { "golars" }
local DEFAULT_ROWS = 10
local DEFAULT_TIMEOUT_MS = 2000
local DEFAULT_DEBOUNCE_MS = 400

-- Per-buffer state. `generation` guards against stale subprocess
-- callbacks: each refresh() bumps it, and completion handlers
-- compare their captured generation against the current one before
-- touching extmarks. Without this, a fast typist can trigger
-- overlapping subprocess runs whose replies then stack as duplicate
-- virtual-text blocks.
local state = {}

-- is_probe_line returns true iff `line` is a comment ending in ^?.
local function is_probe_line(line)
  if line == nil then return false end
  local trimmed = line:match("^%s*(.*)$")
  if trimmed:sub(1, 1) ~= "#" then return false end
  local body = trimmed:sub(2):gsub("%s+$", "")
  return body:sub(-2) == "^?"
end

-- Locate every probe line in the buffer.
local function find_probes(bufnr)
  local probes = {}
  local total = api.nvim_buf_line_count(bufnr)
  for i = 0, total - 1 do
    local l = api.nvim_buf_get_lines(bufnr, i, i + 1, false)[1] or ""
    if is_probe_line(l) then
      probes[#probes + 1] = i
    end
  end
  return probes
end

-- Write a tempfile containing lines[1..upto_line-1] (0-indexed).
-- Returns the path or nil on error.
local function write_prefix_tempfile(bufnr, upto_line)
  local lines = api.nvim_buf_get_lines(bufnr, 0, upto_line, false)
  local prefix = table.concat(lines, "\n")
  local tmp = vim.fn.tempname() .. ".glr"
  local f, err = io.open(tmp, "w")
  if not f then
    return nil, err
  end
  f:write(prefix)
  f:close()
  return tmp
end

-- Render `lines` as virtual lines below `row` (0-indexed) in bufnr.
-- Previous extmarks in the namespace for this row are cleared first.
local function render_virtual_lines(bufnr, row, lines, hl)
  local virt_lines = {}
  for _, l in ipairs(lines) do
    virt_lines[#virt_lines + 1] = { { l, hl or "Comment" } }
  end
  api.nvim_buf_set_extmark(bufnr, ns, row, 0, {
    virt_lines = virt_lines,
    virt_lines_above = false,
    hl_mode = "combine",
  })
end

-- Spawn the preview binary for a single probe line. `gen` is the
-- generation number captured at call time. The callback ignores
-- its result if the generation has moved on. `done` is invoked on
-- the main thread with (ok, output, err).
local function spawn_preview(bufnr, probe_line, gen, on_done)
  local st = state[bufnr]
  if not st then return end
  local tmp, terr = write_prefix_tempfile(bufnr, probe_line)
  if not tmp then
    on_done(false, "", "tempfile: " .. tostring(terr))
    return
  end

  local cmd = {}
  for _, part in ipairs(st.cmd) do
    cmd[#cmd + 1] = part
  end
  cmd[#cmd + 1] = "--preview"
  cmd[#cmd + 1] = tmp
  cmd[#cmd + 1] = "--preview-rows"
  cmd[#cmd + 1] = tostring(st.rows)

  vim.system(cmd, { text = true, timeout = st.timeout }, function(obj)
    -- Callback runs off the main thread. Hop back via schedule.
    vim.schedule(function()
      pcall(os.remove, tmp)
      -- Stale: a newer refresh has started since we spawned.
      local cur = state[bufnr]
      if not cur or cur.generation ~= gen then
        return
      end
      if obj.signal ~= 0 and obj.signal ~= nil then
        on_done(false, "", "signal " .. tostring(obj.signal))
        return
      end
      if obj.code ~= 0 then
        local stderr = (obj.stderr or ""):gsub("%s+$", "")
        on_done(false, obj.stdout or "", stderr ~= "" and stderr or "exit " .. tostring(obj.code))
        return
      end
      on_done(true, obj.stdout or "", "")
    end)
  end)
end

-- refresh clears all probe extmarks in bufnr, bumps the generation
-- counter, then spawns a preview subprocess per probe line.
-- Completion handlers ignore stale generations so overlapping
-- refreshes can't stack duplicate virtual-text blocks.
function M.refresh(bufnr)
  local st = state[bufnr]
  if not st or not api.nvim_buf_is_loaded(bufnr) then return end

  st.generation = (st.generation or 0) + 1
  local gen = st.generation

  api.nvim_buf_clear_namespace(bufnr, ns, 0, -1)
  for _, row in ipairs(find_probes(bufnr)) do
    -- Placeholder so the user sees immediate feedback even on slow
    -- disks; it's overwritten when the subprocess completes.
    render_virtual_lines(bufnr, row, { "  … running preview" })
    spawn_preview(bufnr, row, gen, function(ok, out, err)
      if not api.nvim_buf_is_valid(bufnr) then return end
      -- Clear THIS row's prior marks before appending the result so
      -- we don't pile new virt_lines below the placeholder.
      api.nvim_buf_clear_namespace(bufnr, ns, row, row + 1)
      if not ok then
        render_virtual_lines(bufnr, row,
          { "  ! preview failed: " .. err }, "DiagnosticWarn")
        return
      end
      local lines = {}
      for line in (out or ""):gmatch("([^\n]*)\n?") do
        if line ~= "" then lines[#lines + 1] = "  " .. line end
      end
      if #lines == 0 then
        lines = { "  (no frame in focus)" }
      end
      render_virtual_lines(bufnr, row, lines, "Comment")
    end)
  end
end

-- schedule_refresh debounces the spawn so typing a probe line
-- doesn't trigger a subprocess on every keystroke.
local function schedule_refresh(bufnr)
  local st = state[bufnr]
  if not st then return end
  if st.timer then
    pcall(vim.uv.timer_stop, st.timer)
    pcall(vim.uv.close, st.timer)
    st.timer = nil
  end
  local t = vim.uv.new_timer()
  st.timer = t
  t:start(st.debounce, 0, function()
    vim.schedule(function() M.refresh(bufnr) end)
  end)
end

--- attach wires preview rendering onto a buffer. Clears any
--- previous binding so calling twice (e.g. on plugin reload) is
--- safe.
function M.attach(bufnr, opts)
  opts = opts or {}
  state[bufnr] = {
    cmd = opts.cmd or DEFAULT_CMD,
    rows = opts.rows or DEFAULT_ROWS,
    timeout = opts.timeout or DEFAULT_TIMEOUT_MS,
    debounce = opts.debounce or DEFAULT_DEBOUNCE_MS,
    timer = nil,
    generation = 0,
  }

  if vim.fn.executable(state[bufnr].cmd[1]) == 0 then
    vim.notify(
      "golars preview: `" .. state[bufnr].cmd[1] .. "` not on $PATH; probe previews disabled",
      vim.log.levels.WARN
    )
    return
  end

  -- Re-render on load and after text changes.
  local group = api.nvim_create_augroup("golars.preview." .. bufnr, { clear = true })
  api.nvim_create_autocmd({ "TextChanged", "TextChangedI", "BufWritePost" }, {
    group = group,
    buffer = bufnr,
    callback = function() schedule_refresh(bufnr) end,
  })
  api.nvim_create_autocmd("BufUnload", {
    group = group,
    buffer = bufnr,
    callback = function()
      state[bufnr] = nil
      api.nvim_buf_clear_namespace(bufnr, ns, 0, -1)
    end,
  })

  -- Initial render.
  vim.schedule(function() M.refresh(bufnr) end)
end

return M
