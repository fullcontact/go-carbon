package index

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/lomik/go-carbon/tags"
)

func TestJoinTagValueInodes(t *testing.T) {
	jtvis := joinTagValueInodes([][]*TagValueInode{
		{{Value: "11"}, {Value: "12"}, {Value: "13"}},
		{{Value: "21"}, {Value: "22"}, {Value: "23"}},
		{{Value: "31"}, {Value: "32"}},
	})
	want := [][]*TagValueInode{
		0:  {{Value: "11"}, {Value: "21"}, {Value: "31"}},
		1:  {{Value: "11"}, {Value: "21"}, {Value: "32"}},
		2:  {{Value: "11"}, {Value: "22"}, {Value: "31"}},
		3:  {{Value: "11"}, {Value: "22"}, {Value: "32"}},
		4:  {{Value: "11"}, {Value: "23"}, {Value: "31"}},
		5:  {{Value: "11"}, {Value: "23"}, {Value: "32"}},
		6:  {{Value: "12"}, {Value: "21"}, {Value: "31"}},
		7:  {{Value: "12"}, {Value: "21"}, {Value: "32"}},
		8:  {{Value: "12"}, {Value: "22"}, {Value: "31"}},
		9:  {{Value: "12"}, {Value: "22"}, {Value: "32"}},
		10: {{Value: "12"}, {Value: "23"}, {Value: "31"}},
		11: {{Value: "12"}, {Value: "23"}, {Value: "32"}},
		12: {{Value: "13"}, {Value: "21"}, {Value: "31"}},
		13: {{Value: "13"}, {Value: "21"}, {Value: "32"}},
		14: {{Value: "13"}, {Value: "22"}, {Value: "31"}},
		15: {{Value: "13"}, {Value: "22"}, {Value: "32"}},
		16: {{Value: "13"}, {Value: "23"}, {Value: "31"}},
		17: {{Value: "13"}, {Value: "23"}, {Value: "32"}},
	}
	if !reflect.DeepEqual(jtvis, want) {
		// t.Errorf("g = %s; want %s", jtvis, want)
		t.Errorf("differences: %s", cmp.Diff(jtvis, want, cmp.AllowUnexported(TagValueInode{})))
	}
}

func TestIndex(t *testing.T) {
	index := NewTagIndex()

	testdata, err := ioutil.ReadFile("testdata")
	if err != nil {
		start := time.Now()
		testdataf, err := os.Create("testdata")
		if err != nil {
			panic(err)
		}
		overlapped := "overlapped"
		for i := 0; i < 333333; i++ {
			for _, dc := range []string{"ams", "sh", "sf"} {
				var id = make([]byte, 18)
				rand.Read(id)
				machine := fmt.Sprintf("%x", id)
				if _, err := fmt.Fprintf(testdataf, "disk.used,%s,%s,%s\n", dc, machine, overlapped); err != nil {
					panic(err)
				}
				rand.Read(id)
				uniqMetric := fmt.Sprintf("special_per_instance_counter.%x", id)
				if _, err := fmt.Fprintf(testdataf, "%s,%s,%s,%s\n", uniqMetric, dc, machine, overlapped); err != nil {
					panic(err)
				}
			}
		}
		if err := testdataf.Close(); err != nil {
			panic(err)
		}
		testdata, err = ioutil.ReadFile("testdata")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("took %s to create testdata file", time.Now().Sub(start))
	}

	recs, err := csv.NewReader(bytes.NewReader(testdata)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for _, rec := range recs {
		metric := string(rec[0])
		dc := string(rec[1])
		machine := string(rec[2])
		overlapped := string(rec[3])
		index.Insert("dc", dc, metric, tags.FilePath("", metric, false))
		index.Insert("machine", machine, metric, tags.FilePath("", metric, false))
		index.Insert("overlapped", overlapped, metric, tags.FilePath("", metric, false))
	}
	t.Logf("index took %s", time.Now().Sub(start))

	profile, err := os.Create("cpu_profile")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(profile)

	start = time.Now()
	t.Logf(
		"list tiny (%d) took %s",
		len(index.ListMetrics(&TagValueExpr{}, []*TagValueExpr{{"dc", "ams", "="}, {"machine", "8704f178-04ce-4f13-b166-a890ad8ebc8c", "="}}, 0)),
		time.Now().Sub(start),
	)

	start = time.Now()
	t.Logf(
		"list big (%d) took %s",
		len(index.ListMetrics(&TagValueExpr{}, []*TagValueExpr{{"dc", "ams", "="}, {"overlapped", "overlapped", "="}}, 0)),
		time.Now().Sub(start),
	)

	pprof.StopCPUProfile()
	profile.Close()
}
