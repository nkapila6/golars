package json_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/Gaurav-Gosain/golars/io/json"
)

func TestReadArrayOfObjects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	doc := `[
		{"name": "a", "value": 1, "score": 1.5},
		{"name": "b", "value": 2, "score": 2.5},
		{"name": "c", "value": 3, "score": 3.5}
	]`
	df, err := json.ReadString(ctx, doc)
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	defer df.Release()

	if df.Height() != 3 {
		t.Errorf("Height = %d, want 3", df.Height())
	}
	if df.Width() != 3 {
		t.Errorf("Width = %d, want 3", df.Width())
	}
	if !df.Contains("name") || !df.Contains("value") || !df.Contains("score") {
		t.Errorf("missing columns: %v", df.Schema().Names())
	}
	val, _ := df.Column("value")
	if id := val.DType().ID(); id != arrow.INT64 {
		t.Errorf("value dtype = %s, want INT64", val.DType())
	}
	sc, _ := df.Column("score")
	if id := sc.DType().ID(); id != arrow.FLOAT64 {
		t.Errorf("score dtype = %s, want FLOAT64", sc.DType())
	}
}

func TestReadNDJSON(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	doc := `{"a": 1, "b": "x"}
{"a": 2, "b": "y"}

{"a": 3, "b": "z"}
`
	df, err := json.ReadNDJSONString(ctx, doc)
	if err != nil {
		t.Fatalf("ReadNDJSONString: %v", err)
	}
	defer df.Release()
	if df.Height() != 3 {
		t.Errorf("Height = %d, want 3 (blank line should be skipped)", df.Height())
	}
}

func TestReadMixedTypesPromotesToFloat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	doc := `[{"x": 1}, {"x": 2.5}, {"x": 3}]`
	df, err := json.ReadString(ctx, doc)
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	defer df.Release()
	col, _ := df.Column("x")
	if id := col.DType().ID(); id != arrow.FLOAT64 {
		t.Errorf("x dtype = %s, want FLOAT64 (int+float should promote)", col.DType())
	}
}

func TestReadNulls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	doc := `[{"a": 1}, {"a": null}, {"a": 2}]`
	df, err := json.ReadString(ctx, doc)
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	defer df.Release()
	col, _ := df.Column("a")
	if got := col.NullCount(); got != 1 {
		t.Errorf("NullCount = %d, want 1", got)
	}
}

func TestReadColumnsObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	doc := `{"a": [1, 2, 3], "b": ["x", "y", "z"]}`
	df, err := json.ReadString(ctx, doc)
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	defer df.Release()
	if df.Height() != 3 || df.Width() != 2 {
		t.Errorf("got %dx%d, want 3x2", df.Height(), df.Width())
	}
}

func TestWriteNDJSONRoundtrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	doc := `[{"name": "a", "value": 1}, {"name": "b", "value": 2}]`
	df, err := json.ReadString(ctx, doc)
	if err != nil {
		t.Fatalf("ReadString: %v", err)
	}
	defer df.Release()

	var buf bytes.Buffer
	if err := json.WriteNDJSON(ctx, &buf, df); err != nil {
		t.Fatalf("WriteNDJSON: %v", err)
	}
	df2, err := json.ReadNDJSON(ctx, &buf)
	if err != nil {
		t.Fatalf("ReadNDJSON: %v", err)
	}
	defer df2.Release()
	if df2.Height() != 2 || df2.Width() != 2 {
		t.Errorf("roundtrip shape = %dx%d, want 2x2", df2.Height(), df2.Width())
	}
}
