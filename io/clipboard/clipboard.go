// Package clipboard reads and writes DataFrames to the OS clipboard
// as CSV text. Mirrors polars' pl.read_clipboard / DataFrame.write_
// clipboard. Underlying transport is github.com/atotto/clipboard
// which is already a transitive dependency via bubbletea.
package clipboard

import (
	"bytes"
	"context"

	extclip "github.com/atotto/clipboard"

	"github.com/Gaurav-Gosain/golars/dataframe"
	iocsv "github.com/Gaurav-Gosain/golars/io/csv"
)

// Read pulls the current clipboard contents and parses it as CSV
// (header expected, comma-delimited). Opts are forwarded to the CSV
// reader so the caller can override delimiters, schema, etc.
func Read(ctx context.Context, opts ...iocsv.Option) (*dataframe.DataFrame, error) {
	text, err := extclip.ReadAll()
	if err != nil {
		return nil, err
	}
	return iocsv.Read(ctx, bytes.NewReader([]byte(text)), opts...)
}

// Write serialises df as CSV and places it on the OS clipboard. Opts
// control delimiters and header rendering via the CSV writer.
func Write(ctx context.Context, df *dataframe.DataFrame, opts ...iocsv.Option) error {
	var buf bytes.Buffer
	if err := iocsv.Write(ctx, &buf, df, opts...); err != nil {
		return err
	}
	return extclip.WriteAll(buf.String())
}
