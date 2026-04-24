package dataframe

import "github.com/Gaurav-Gosain/golars/selector"

// SelectBy projects the columns chosen by sel. Equivalent to
// df.Select(sel.Apply(df.Schema())...).
func (df *DataFrame) SelectBy(sel selector.Selector) (*DataFrame, error) {
	return df.Select(sel.Apply(df.sch)...)
}

// DropBy drops the columns chosen by sel.
func (df *DataFrame) DropBy(sel selector.Selector) *DataFrame {
	return df.Drop(sel.Apply(df.sch)...)
}
