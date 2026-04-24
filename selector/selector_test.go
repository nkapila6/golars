package selector_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/schema"
	"github.com/Gaurav-Gosain/golars/selector"
)

func fixture(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.New(
		schema.Field{Name: "id", DType: dtype.Int64()},
		schema.Field{Name: "age", DType: dtype.Int64()},
		schema.Field{Name: "name", DType: dtype.String()},
		schema.Field{Name: "salary_usd", DType: dtype.Float64()},
		schema.Field{Name: "salary_eur", DType: dtype.Float64()},
		schema.Field{Name: "is_active", DType: dtype.Bool()},
	)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSelectorAll(t *testing.T) {
	s := fixture(t)
	got := selector.All().Apply(s)
	if len(got) != 6 {
		t.Errorf("All len = %d, want 6", len(got))
	}
}

func TestSelectorNamedAndExclude(t *testing.T) {
	s := fixture(t)
	got := selector.Named("id", "name", "missing").Apply(s)
	if len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Errorf("Named = %v", got)
	}
	got = selector.Exclude("id", "age").Apply(s)
	if len(got) != 4 {
		t.Errorf("Exclude len = %d, want 4", len(got))
	}
}

func TestSelectorByDtype(t *testing.T) {
	s := fixture(t)
	got := selector.ByDtype(dtype.Int64()).Apply(s)
	if len(got) != 2 || got[0] != "id" || got[1] != "age" {
		t.Errorf("ByDtype(int64) = %v", got)
	}
	got = selector.Numeric().Apply(s)
	if len(got) != 4 {
		t.Errorf("Numeric len = %d, want 4", len(got))
	}
	got = selector.Integer().Apply(s)
	if len(got) != 2 {
		t.Errorf("Integer len = %d", len(got))
	}
	got = selector.Float().Apply(s)
	if len(got) != 2 {
		t.Errorf("Float len = %d", len(got))
	}
	got = selector.StringCols().Apply(s)
	if len(got) != 1 || got[0] != "name" {
		t.Errorf("StringCols = %v", got)
	}
}

func TestSelectorNamePredicates(t *testing.T) {
	s := fixture(t)
	got := selector.StartsWith("salary_").Apply(s)
	if len(got) != 2 {
		t.Errorf("StartsWith = %v", got)
	}
	got = selector.EndsWith("_usd").Apply(s)
	if len(got) != 1 || got[0] != "salary_usd" {
		t.Errorf("EndsWith = %v", got)
	}
	got = selector.Contains("sal").Apply(s)
	if len(got) != 2 {
		t.Errorf("Contains = %v", got)
	}
	got = selector.Matching("^is_").Apply(s)
	if len(got) != 1 || got[0] != "is_active" {
		t.Errorf("Matching = %v", got)
	}
}

func TestSelectorCombinators(t *testing.T) {
	s := fixture(t)
	union := selector.Union(selector.StartsWith("salary_"), selector.Named("id"))
	got := union.Apply(s)
	if len(got) != 3 {
		t.Errorf("Union len = %d, want 3 (id + 2 salary)", len(got))
	}

	inter := selector.Intersect(selector.Numeric(), selector.StartsWith("salary_"))
	got = inter.Apply(s)
	if len(got) != 2 {
		t.Errorf("Intersect len = %d, want 2", len(got))
	}

	diff := selector.Minus(selector.Numeric(), selector.StartsWith("salary_"))
	got = diff.Apply(s)
	// Numeric minus salary_* = {id, age}
	if len(got) != 2 {
		t.Errorf("Minus = %v", got)
	}
}
