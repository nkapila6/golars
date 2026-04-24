package browse

import (
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
)

// keysHelp is the one-line status-bar reminder for `?` in normal mode.
const keysHelp = "j/k rows  h/l cols  gg/G top/end  / filter  : command  s sort  - hide  = unhide  f freeze  v visual  mouse: wheel/click  q quit"

// commandHelp is the `:help` cheat-sheet dumped into the status bar.
const commandHelp = ":sort COL [asc|desc]  :sort none  :filter SUB  :hide COL  :show COL|all  :freeze COL  :goto N  :export PATH  :q"

// Update is the bubbletea message dispatch. Routes key events by
// current mode (normal / visual / command / filter); mouse events go
// to the mouse handler regardless of mode, since scroll + click feel
// expected to work in all modes.
func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.MouseWheelMsg:
		return m.handleMouseWheel(msg)
	case tea.MouseClickMsg:
		return m.handleMouseClick(msg)
	case tea.KeyPressMsg:
		switch m.mode {
		case modeFilter:
			return m.handleFilterKey(msg)
		case modeCommand:
			return m.handleCommandKey(msg)
		case modeVisual:
			return m.handleVisualKey(msg)
		default:
			return m.handleNormalKey(msg)
		}
	}
	return m, nil
}

// ---- mouse --------------------------------------------------------

// wheelStep is how many rows one scroll tick moves. Matches the feel
// of most terminal pagers.
const wheelStep = 3

// handleMouseWheel scrolls the viewport. We move the cursor too so
// the selection is visible after scrolling; this matches sheets'
// behaviour. Horizontal wheel maps to column motion.
func (m *model) handleMouseWheel(msg tea.MouseWheelMsg) (tea.Model, tea.Cmd) {
	switch msg.Button {
	case tea.MouseWheelUp:
		m.move(-wheelStep)
	case tea.MouseWheelDown:
		m.move(wheelStep)
	case tea.MouseWheelLeft:
		m.moveCol(-1)
	case tea.MouseWheelRight:
		m.moveCol(1)
	}
	m.ensureVisible()
	return m, nil
}

// handleMouseClick maps an (x, y) pixel to (visRow, visCol) via the
// current layout and moves the cursor. Clicks on the column-header
// row toggle sort on that column, mirroring spreadsheet UX.
func (m *model) handleMouseClick(msg tea.MouseClickMsg) (tea.Model, tea.Cmd) {
	if msg.Button != tea.MouseLeft {
		return m, nil
	}
	lay := m.computeLayout()

	// y=1 is the column-header strip.
	if msg.Y == 1 {
		i, ok := colAtX(lay, msg.X)
		if ok {
			m.cursorCol = lay.vispos[i]
			m.toggleSort()
		}
		return m, nil
	}

	row, i, ok := cellAtPoint(lay, msg.X, msg.Y, m.topRow, m.rowCount())
	if !ok {
		return m, nil
	}
	m.cursorRow = row
	m.cursorCol = lay.vispos[i]
	if m.mode != modeVisual {
		m.commandMessage = ""
	}
	m.ensureVisible()
	return m, nil
}

// colAtX maps an x coordinate to a visible-column index in lay.vis.
// Returns (-1, false) when x lands outside any data column (row
// gutter, border, or phantom trailing cells).
func colAtX(lay layout, x int) (int, bool) {
	// Gutter occupies x < rowLabelWidth+2; first │ at rowLabelWidth+1.
	cursor := lay.rowLabelWidth + 2
	for i, w := range lay.widths {
		if x >= cursor && x < cursor+w {
			return i, true
		}
		cursor += w + 1 // cell + right │
	}
	return -1, false
}

// cellAtPoint maps (x, y) to (visRow, visCol) indices. Returns
// !ok when the click hit a border, a row past end-of-data, or fell
// outside the visible data area.
func cellAtPoint(lay layout, x, y, topRow, rowCount int) (int, int, bool) {
	// y=0 top bar, y=1 header, y=2 top border, y=3 first content row,
	// y=4 border, y=5 second content row, ... i.e. content rows live
	// at y = 3 + 2*i.
	if y < 3 {
		return 0, 0, false
	}
	rel := y - 3
	if rel%2 != 0 {
		return 0, 0, false
	}
	visRow := topRow + rel/2
	if visRow >= rowCount || visRow-topRow >= lay.visibleRows {
		return 0, 0, false
	}
	col, ok := colAtX(lay, x)
	if !ok {
		return 0, 0, false
	}
	return visRow, col, true
}

// ---- normal mode --------------------------------------------------

func (m *model) handleNormalKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	s := msg.String()

	// `g` as a motion prefix: `gg` -> top, `gN<enter>` -> goto row N.
	if m.gotoPending {
		switch {
		case s == "g":
			m.jumpTop()
			m.gotoPending, m.gotoBuffer = false, ""
			return m, nil
		case s == "esc":
			m.gotoPending, m.gotoBuffer = false, ""
			return m, nil
		case s == "enter":
			if n, err := strconv.Atoi(m.gotoBuffer); err == nil && n > 0 {
				m.cursorRow = clamp(n-1, 0, m.rowCount()-1)
			}
			m.gotoPending, m.gotoBuffer = false, ""
			m.ensureVisible()
			return m, nil
		case len(s) == 1 && s[0] >= '0' && s[0] <= '9':
			m.gotoBuffer += s
			return m, nil
		default:
			m.gotoPending, m.gotoBuffer = false, ""
		}
	}

	switch s {
	case "q", "ctrl+c":
		return m, tea.Quit

	// --- vertical motion ---
	case "j", "down":
		m.move(1)
	case "k", "up":
		m.move(-1)
	case "ctrl+d":
		m.move(m.viewportRows() / 2)
	case "ctrl+u":
		m.move(-m.viewportRows() / 2)
	case "ctrl+f", "pgdown":
		m.move(m.viewportRows())
	case "ctrl+b", "pgup":
		m.move(-m.viewportRows())
	case "G":
		m.cursorRow = m.rowCount() - 1
		m.ensureVisible()
	case "g":
		m.gotoPending, m.gotoBuffer = true, ""

	// --- horizontal motion ---
	case "h", "left":
		m.moveCol(-1)
	case "l", "right":
		m.moveCol(1)
	case "0", "home", "^":
		m.cursorCol = 0
		m.leftCol = 0
	case "$", "end":
		m.cursorCol = max(len(m.visibleCols())-1, 0)

	// --- frozen-column page ---
	case "[":
		if m.leftCol > 0 {
			m.leftCol--
		}
	case "]":
		if m.leftCol < len(m.visibleCols())-1 {
			m.leftCol++
		}

	// --- visual / selection ---
	case "v", "V":
		m.enterVisual()

	// --- search / filter ---
	case "/":
		m.mode = modeFilter
		m.filterInput.Focus()
		m.filterInput.SetValue(m.filter)
		return m, nil

	// --- command prompt ---
	case ":":
		m.mode = modeCommand
		m.commandInput.Focus()
		m.commandInput.SetValue("")
		m.commandMessage = ""
		return m, nil

	// --- sort toggle on current column ---
	case "s":
		m.toggleSort()

	// --- column visibility ---
	case "-":
		m.hideCurrentCol()
	case "=":
		for i := range m.cols {
			m.cols[i].hidden = false
		}
		m.commandMessage = "all columns visible"
	case "f":
		m.toggleFrozenCol()

	// --- reset view ---
	case "esc":
		if m.filter != "" || m.sort.idx >= 0 {
			m.filter = ""
			m.sort = sortKey{idx: -1}
			m.applyView()
			m.commandMessage = "view cleared"
		}

	// --- help ---
	case "?":
		m.commandMessage = keysHelp
	}

	m.ensureVisible()
	return m, nil
}

func (m *model) move(delta int) {
	if n := m.rowCount(); n > 0 {
		m.cursorRow = clamp(m.cursorRow+delta, 0, n-1)
	}
}

func (m *model) moveCol(delta int) {
	vis := m.visibleCols()
	if len(vis) == 0 {
		return
	}
	m.cursorCol = clamp(m.cursorCol+delta, 0, len(vis)-1)
	if m.cursorCol < m.leftCol {
		m.leftCol = m.cursorCol
	}
}

func (m *model) jumpTop() {
	m.cursorRow = 0
	m.topRow = 0
}

func (m *model) toggleSort() {
	vis := m.visibleCols()
	if m.cursorCol >= len(vis) {
		return
	}
	ci := vis[m.cursorCol]
	switch {
	case m.sort.idx != ci:
		m.sort = sortKey{idx: ci, desc: false}
	case !m.sort.desc:
		m.sort.desc = true
	default:
		m.sort = sortKey{idx: -1}
	}
	m.applyView()
}

func (m *model) hideCurrentCol() {
	vis := m.visibleCols()
	if m.cursorCol >= len(vis) {
		return
	}
	m.cols[vis[m.cursorCol]].hidden = true
	if m.cursorCol >= len(m.visibleCols()) {
		m.cursorCol = max(len(m.visibleCols())-1, 0)
	}
}

func (m *model) toggleFrozenCol() {
	vis := m.visibleCols()
	if m.cursorCol >= len(vis) {
		return
	}
	ci := vis[m.cursorCol]
	m.cols[ci].frozen = !m.cols[ci].frozen
}

// ---- visual mode --------------------------------------------------

func (m *model) enterVisual() {
	m.mode = modeVisual
	m.anchorRow = m.cursorRow
}

func (m *model) handleVisualKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc", "v", "V":
		m.mode = modeNormal
	case "q", "ctrl+c":
		return m, tea.Quit
	case "j", "down":
		m.move(1)
	case "k", "up":
		m.move(-1)
	case "G":
		m.cursorRow = m.rowCount() - 1
	case "gg":
		m.jumpTop()
	case "ctrl+d":
		m.move(m.viewportRows() / 2)
	case "ctrl+u":
		m.move(-m.viewportRows() / 2)
	case "y":
		// Yank selection range as status: users can then run :export.
		lo, hi := m.visualRange()
		m.commandMessage = fmt.Sprintf("selected rows %d-%d (%d rows)", lo+1, hi+1, hi-lo+1)
		m.mode = modeNormal
	}
	m.ensureVisible()
	return m, nil
}

// visualRange returns the inclusive (lo, hi) visual-mode selection
// indices, always with lo <= hi.
func (m *model) visualRange() (int, int) {
	a, b := m.anchorRow, m.cursorRow
	if a > b {
		a, b = b, a
	}
	return a, b
}

// ---- filter mode --------------------------------------------------

func (m *model) handleFilterKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		// Accept current filter, drop back to NORMAL.
		m.filter = strings.TrimSpace(m.filterInput.Value())
		m.mode = modeNormal
		m.applyView()
		if m.filter != "" {
			m.commandMessage = fmt.Sprintf("%d/%d rows match %q", m.rowCount(), m.df.Height(), m.filter)
		} else {
			m.commandMessage = ""
		}
		return m, nil
	case "esc":
		// Cancel: restore the previous filter.
		m.filterInput.SetValue(m.filter)
		m.mode = modeNormal
		m.applyView()
		return m, nil
	}
	// Let textinput consume the key, then re-apply the filter LIVE.
	// Filtering uses golars compute kernels (Cast -> Str.Lower ->
	// Str.Contains -> Or). At 100k rows across 10 columns this is
	// sub-millisecond, so typing stays snappy even with the view
	// updating per keystroke.
	var cmd tea.Cmd
	m.filterInput, cmd = m.filterInput.Update(msg)
	m.filter = strings.TrimSpace(m.filterInput.Value())
	m.applyView()
	return m, cmd
}

// ---- command mode -------------------------------------------------

func (m *model) handleCommandKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		cmd := strings.TrimSpace(m.commandInput.Value())
		m.mode = modeNormal
		return m, m.runCommand(cmd)
	case "esc":
		m.mode = modeNormal
		return m, nil
	}
	var cmd tea.Cmd
	m.commandInput, cmd = m.commandInput.Update(msg)
	return m, cmd
}

// runCommand parses and executes an ex-style command.
// Supported:
//
//	:q, :quit              -- exit
//	:sort col [asc|desc]   -- sort by column name
//	:sort none             -- clear sort
//	:hide col              -- hide a column
//	:show col | :show all  -- un-hide a column (or all)
//	:freeze col            -- toggle freeze on a column
//	:filter substring      -- apply filter (empty clears)
//	:goto N                -- jump to row N (1-based)
//	:export PATH           -- write current view to PATH (format from extension)
//	:help                  -- print the command reference
func (m *model) runCommand(line string) tea.Cmd {
	if line == "" {
		return nil
	}
	fields := strings.Fields(line)
	head := strings.ToLower(fields[0])
	m.commandError = false
	switch head {
	case "q", "quit":
		return tea.Quit
	case "sort":
		if len(fields) < 2 {
			m.commandMessage = "sort: need column or 'none'"
			m.commandError = true
			return nil
		}
		if strings.EqualFold(fields[1], "none") {
			m.sort = sortKey{idx: -1}
			m.applyView()
			m.commandMessage = "sort cleared"
			return nil
		}
		ci := m.findCol(fields[1])
		if ci < 0 {
			m.commandMessage = fmt.Sprintf("no such column: %s", fields[1])
			m.commandError = true
			return nil
		}
		desc := len(fields) >= 3 && strings.EqualFold(fields[2], "desc")
		m.sort = sortKey{idx: ci, desc: desc}
		m.applyView()
		m.commandMessage = fmt.Sprintf("sorted by %s", m.cols[ci].name)
	case "hide":
		if len(fields) < 2 {
			m.commandError = true
			m.commandMessage = "hide: need column"
			return nil
		}
		ci := m.findCol(fields[1])
		if ci < 0 {
			m.commandError = true
			m.commandMessage = fmt.Sprintf("no such column: %s", fields[1])
			return nil
		}
		m.cols[ci].hidden = true
		m.commandMessage = fmt.Sprintf("hid %s", m.cols[ci].name)
	case "show":
		if len(fields) >= 2 && strings.EqualFold(fields[1], "all") {
			for i := range m.cols {
				m.cols[i].hidden = false
			}
			m.commandMessage = "all columns visible"
			return nil
		}
		if len(fields) < 2 {
			m.commandError = true
			m.commandMessage = "show: need column or 'all'"
			return nil
		}
		ci := m.findCol(fields[1])
		if ci < 0 {
			m.commandError = true
			m.commandMessage = fmt.Sprintf("no such column: %s", fields[1])
			return nil
		}
		m.cols[ci].hidden = false
		m.commandMessage = fmt.Sprintf("showing %s", m.cols[ci].name)
	case "freeze":
		if len(fields) < 2 {
			m.commandError = true
			m.commandMessage = "freeze: need column"
			return nil
		}
		ci := m.findCol(fields[1])
		if ci < 0 {
			m.commandError = true
			m.commandMessage = fmt.Sprintf("no such column: %s", fields[1])
			return nil
		}
		m.cols[ci].frozen = !m.cols[ci].frozen
		if m.cols[ci].frozen {
			m.commandMessage = fmt.Sprintf("froze %s", m.cols[ci].name)
		} else {
			m.commandMessage = fmt.Sprintf("unfroze %s", m.cols[ci].name)
		}
	case "filter":
		if len(fields) >= 2 {
			m.filter = strings.Join(fields[1:], " ")
		} else {
			m.filter = ""
		}
		m.applyView()
		m.commandMessage = fmt.Sprintf("%d/%d rows match", m.rowCount(), m.df.Height())
	case "goto":
		if len(fields) < 2 {
			m.commandError = true
			m.commandMessage = "goto: need row number"
			return nil
		}
		n, err := strconv.Atoi(fields[1])
		if err != nil || n <= 0 {
			m.commandError = true
			m.commandMessage = "goto: invalid row number"
			return nil
		}
		m.cursorRow = clamp(n-1, 0, m.rowCount()-1)
		m.ensureVisible()
	case "export", "w", "write":
		if len(fields) < 2 {
			m.commandError = true
			m.commandMessage = "export: need path (format inferred from extension)"
			return nil
		}
		path := strings.Join(fields[1:], " ")
		if err := m.exportView(path); err != nil {
			m.commandError = true
			m.commandMessage = "export: " + err.Error()
			return nil
		}
		m.commandMessage = fmt.Sprintf("wrote %d rows × %d cols to %s",
			m.rowCount(), len(m.visibleCols()), path)
	case "help", "h":
		m.commandMessage = commandHelp
	default:
		m.commandError = true
		m.commandMessage = fmt.Sprintf("unknown command: %s", fields[0])
	}
	return nil
}

// findCol returns the column index whose name matches name (case-
// sensitive first, falling back to case-insensitive), or -1.
func (m *model) findCol(name string) int {
	for i := range m.cols {
		if m.cols[i].name == name {
			return i
		}
	}
	for i := range m.cols {
		if strings.EqualFold(m.cols[i].name, name) {
			return i
		}
	}
	return -1
}
