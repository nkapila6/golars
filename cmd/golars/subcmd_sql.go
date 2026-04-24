package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Gaurav-Gosain/golars/sql"
)

// newSqlCmd executes a SQL query against one or more data files. Each
// file is registered as a table keyed on its filename stem: so
// `people.csv` becomes table `people`.
func newSqlCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "sql QUERY [FILE...]",
		Aliases: []string{"query"},
		Short:   "run SQL against one or more data files",
		Example: `golars sql 'SELECT symbol, SUM(qty) FROM t GROUP BY symbol' t.csv`,
		Args:    cobra.MinimumNArgs(1),
	}
	ff := bindFormatFlags(cmd)
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		format, err := ff.resolve()
		if err != nil {
			return err
		}
		ctx := context.Background()
		session := sql.NewSession()
		defer session.Close()
		for _, f := range args[1:] {
			df, err := loadByExt(ctx, f)
			if err != nil {
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
			name := strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))
			if err := session.Register(name, df); err != nil {
				df.Release()
				fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
				return errSubcommandFailed
			}
			df.Release()
		}
		out, err := session.Query(ctx, args[0])
		if err != nil {
			fmt.Fprintln(os.Stderr, errMsgStyle.Render(err.Error()))
			return errSubcommandFailed
		}
		defer out.Release()
		renderFrame(out, format)
		return nil
	}
	return cmd
}
