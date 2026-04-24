package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/browse"
)

// cmdInteractiveShow materialises the current REPL pipeline and opens
// it in the browse TUI. The viewer runs on the alt screen, so the
// REPL state is preserved below and restored on quit. The pipeline is
// left untouched: filter / sort / hide inside the TUI are view-only.
// Export from the viewer via `:export PATH`.
func (s *state) cmdInteractiveShow(_ []string) error {
	df, err := s.materialize()
	if err != nil {
		return err
	}
	defer df.Release()

	label := s.focused
	if label == "" {
		if s.path != "" {
			label = filepath.Base(s.path)
		} else {
			label = "<pipeline>"
		}
	}
	return browse.RunWithContext(s.ctx, df, label)
}

// newBrowseCmd opens the TUI viewer for a data file. Arrow keys scroll,
// `/` filters, `q` quits. See the browse/ package for the model.
func newBrowseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "browse FILE",
		Short:   "interactive TUI table viewer",
		Example: "golars browse data.csv",
		Args:    cobra.ExactArgs(1),
	}
	cmd.ValidArgsFunction = dataFileCompletion
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		ctx := context.Background()
		df, err := loadByExt(ctx, args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer df.Release()
		if err := browse.Run(df, args[0]); err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		return nil
	}
	return cmd
}
