// +build ignore

package main

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

func main() {
	testdata, err := os.Create("testdata")
	if err != nil {
		panic(err)
	}
	overlapped := "overlapped"
	for i := 0; i < 333333; i++ {
		for _, dc := range []string{"ams", "sh", "sf"} {
			// testdata.WriteString("disk.used")
			machine := uuid.New()
			if _, err := fmt.Fprintf(testdata, "disk.used,%s,%s,%s\n", dc, machine, overlapped); err != nil {
				panic(err)
			}
			uniqMetric := fmt.Sprintf("special_per_instance_counter.%s", uuid.New())
			if _, err := fmt.Fprintf(testdata, "%s,%s,%s,%s\n", uniqMetric, dc, machine, overlapped); err != nil {
				panic(err)
			}
			// if _, err := fmt.Fprintf(testdata, "%s,%s,%s\n", uniqMetric, dc, overlappedTag); err != nil {
			// 	panic(err)
			// }
		}
	}
	if err := testdata.Close(); err != nil {
		panic(err)
	}
}
