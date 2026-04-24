// Register in-memory DataFrames and run SQL against them.
// Run: go run ./examples/sql_session
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
	"github.com/Gaurav-Gosain/golars/sql"
)

func main() {
	ctx := context.Background()

	dept, _ := series.FromString("dept", []string{"eng", "eng", "sales", "ops"}, nil)
	name, _ := series.FromString("name", []string{"ada", "bob", "cleo", "dan"}, nil)
	salary, _ := series.FromInt64("salary", []int64{100, 120, 80, 70}, nil)
	employees, _ := dataframe.New(dept, name, salary)
	defer employees.Release()

	s := sql.NewSession()
	defer s.Close()
	if err := s.Register("employees", employees); err != nil {
		log.Fatal(err)
	}

	// 1. Basic filter + projection.
	out, err := s.Query(ctx, `SELECT name, salary FROM employees WHERE salary > 75 ORDER BY salary DESC`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("--- top earners ---")
	fmt.Println(out)
	out.Release()

	// 2. Aggregation.
	out, err = s.Query(ctx, `SELECT dept, SUM(salary) AS total, COUNT(name) AS headcount FROM employees GROUP BY dept ORDER BY total DESC`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("--- per-department totals ---")
	fmt.Println(out)
	out.Release()

	// 3. DISTINCT.
	out, err = s.Query(ctx, `SELECT DISTINCT dept FROM employees ORDER BY dept`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("--- distinct departments ---")
	fmt.Println(out)
	out.Release()
}
