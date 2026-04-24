package series_test

import (
	"testing"

	"github.com/Gaurav-Gosain/golars/dtype"
	"github.com/Gaurav-Gosain/golars/series"
)

func TestFromTimeMicroseconds(t *testing.T) {
	dt := dtype.Time(dtype.Microsecond)
	s, err := series.FromTime("t", []int64{0, 3_600_000_000, 86_399_000_000}, nil, dt)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	if s.Len() != 3 {
		t.Errorf("len=%d want 3", s.Len())
	}
	if s.DType().ID().String() != "TIME64" {
		t.Errorf("dtype id=%s want time64", s.DType().ID())
	}
}

func TestFromTimeSeconds(t *testing.T) {
	dt := dtype.Time(dtype.Second)
	s, err := series.FromTime("t", []int64{0, 60, 3600}, nil, dt)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	if s.Len() != 3 {
		t.Errorf("len=%d want 3", s.Len())
	}
	if s.DType().ID().String() != "TIME32" {
		t.Errorf("dtype id=%s want time32", s.DType().ID())
	}
}

func TestFromTimeRejectsNonTime(t *testing.T) {
	_, err := series.FromTime("t", []int64{0, 1}, nil, dtype.Int64())
	if err == nil {
		t.Error("expected error when dt is not a Time")
	}
}
