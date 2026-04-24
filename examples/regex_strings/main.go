// Regex + string ops: Extract, ContainsRegex, SplitN.
// Run: go run ./examples/regex_strings
package main

import (
	"fmt"
	"log"

	"github.com/Gaurav-Gosain/golars/dataframe"
	"github.com/Gaurav-Gosain/golars/series"
)

func main() {
	emails, _ := series.FromString("email", []string{
		"ada@example.com",
		"brian@contoso.net",
		"carl@example.com",
		"not-an-email",
	}, nil)
	defer emails.Release()

	// Extract the domain after '@'.
	domains, err := emails.Str().Extract(`@([^.]+\.[^.]+)$`, 1)
	if err != nil {
		log.Fatal(err)
	}
	defer domains.Release()

	// Match all addresses that look like US-style handles.
	ok, _ := emails.Str().ContainsRegex(`^[a-z]+@[a-z]+\.[a-z]+$`)
	defer ok.Release()

	// First path segment of a URL-ish string.
	paths, _ := emails.Str().SplitExactNullShort("@", 0)
	defer paths.Release()

	out, _ := dataframe.New(emails.Clone(), domains.Rename("domain"),
		ok.Rename("is_standard"), paths.Rename("local"))
	defer out.Release()
	fmt.Println(out)
}
