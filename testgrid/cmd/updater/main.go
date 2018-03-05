/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"log"
	"net/url"
	"path"
	"regexp"
	"sort"
	"time"

	"k8s.io/test-infra/testgrid/config"
	"k8s.io/test-infra/testgrid/state"
	"k8s.io/test-infra/testgrid/summary"

	"cloud.google.com/go/storage"
	"github.com/golang/protobuf/proto"
	"google.golang.org/api/iterator"

	"vbom.ml/util/sortorder"
)

type Build struct {
	Bucket  *storage.BucketHandle
	Context context.Context
	Prefix  string
	number  *int
}

type Started struct {
	Timestamp   int64             `json:"timestamp"` // epoch seconds
	RepoVersion string            `json:"repo-version"`
	Node        string            `json:"node"`
	Pull        string            `json:"pull"`
	Repos       map[string]string `json:"repos"` // {repo: branch_or_pull} map
}

type Finished struct {
	Timestamp  int64    `json:"timestamp"` // epoch seconds
	Passed     bool     `json:"passed"`
	JobVersion string   `json:"job-version"`
	Metadata   Metadata `json:"metadata"`
}

// infra-commit, repos, repo, repo-commit, others
type Metadata map[string]interface{}

func (m Metadata) String(name string) (*string, bool) {
	if v, ok := m[name]; !ok {
		return nil, false
	} else if t, good := v.(string); !good {
		return nil, true
	} else {
		return &t, true
	}
}

func (m Metadata) Meta(name string) (*Metadata, bool) {
	if v, ok := m[name]; !ok {
		return nil, true
	} else if t, good := v.(Metadata); !good {
		return nil, false
	} else {
		return &t, true
	}
}

func (m Metadata) ColumnMetadata() ColumnMetadata {
	bm := ColumnMetadata{}
	for k, v := range m {
		if s, ok := v.(string); ok {
			bm[k] = s
		}
		// TODO(fejta): handle sub items
	}
	return bm
}

type JunitSuites struct {
	XMLName xml.Name     `xml:"testsuites"`
	Suites  []JunitSuite `xml:"testsuite"`
}

type JunitSuite struct {
	XMLName  xml.Name      `xml:"testsuite"`
	Name     string        `xml:"name,attr"`
	Time     float64       `xml:"time,attr"` // Seconds
	Failures int           `xml:"failures,attr"`
	Tests    int           `xml:"tests,attr"`
	Results  []JunitResult `xml:"testcase"`
	/*
	* <properties><property name="go.version" value="go1.8.3"/></properties>
	 */
}

type JunitResult struct {
	Name      string  `xml:"name,attr"`
	Time      float64 `xml:"time,attr"`
	ClassName string  `xml:"classname,attr"`
	Failure   *string `xml:"failure"`
	Output    *string `xml:"system-out"`
	Skipped   *string `xml:"skipped"`
}

func (jr JunitResult) RowResult() state.Row_Result {
	switch {
	case jr.Failure != nil:
		return state.Row_FAIL
	case jr.Skipped != nil:
		return state.Row_PASS_WITH_SKIPS
	}
	return state.Row_PASS
}

func extractRows(buf []byte, rows map[string][]Row, meta map[string]string) error {
	var suites JunitSuites
	// Try to parse it as a <testsuites/> object
	err := xml.Unmarshal(buf, &suites)
	if err != nil {
		// Maybe it is a <testsuite/> object instead
		suites.Suites = append([]JunitSuite(nil), JunitSuite{})
		ie := xml.Unmarshal(buf, &suites.Suites[0])
		if ie != nil {
			// Nope, it just doesn't parse
			return fmt.Errorf("not valid testsuites: %v nor testsuite: %v", err, ie)
		}
	}
	for _, suite := range suites.Suites {
		for _, sr := range suite.Results {
			if sr.Skipped != nil && len(*sr.Skipped) == 0 {
				continue
			}

			n := sr.Name
			if len(suite.Name) > 0 {
				n = suite.Name + "." + n
			}
			r := Row{
				Result:  sr.RowResult(),
				Metrics: map[string]float64{},
				Metadata: map[string]string{
					"Tests name": n,
				},
			}
			if sr.Time > 0 {
				r.Metrics[elapsedKey] = sr.Time
			}
			for k, v := range meta {
				r.Metadata[k] = v
			}
			// TODO(fejta): set message from failure/skipped/system-out
			rows[n] = append(rows[n], r)
		}
	}
	return nil
}

type ColumnMetadata map[string]string

type Column struct {
	Id       string
	Started  int64
	Finished int64
	Passed   bool
	Rows     map[string][]Row
	Metadata ColumnMetadata
}

type Row struct {
	Result   state.Row_Result
	Metrics  map[string]float64
	Metadata map[string]string
}

func (br Column) Overall() state.Row_Result {
	switch {
	case br.Finished > 0:
		// Completed
		if br.Passed {
			return state.Row_PASS
		}
		return state.Row_FAIL
	case time.Now().Add(-24*time.Hour).Unix() > br.Started:
		// Timed out
		return state.Row_FAIL
	default:
		return state.Row_RUNNING
	}
}

var uniq int

func AppendMetric(metric *state.Metric, idx int32, value float64) {
	if l := int32(len(metric.Indices)); l == 0 || metric.Indices[l-2]+metric.Indices[l-1] != idx {
		// If we append V to idx 9 and metric.Indices = [3, 4] then the last filled index is 3+4-1=7
		// So that means we have holes in idx 7 and 8, so start a new group.
		metric.Indices = append(metric.Indices, idx, 1)
	} else {
		metric.Indices[l-1]++ // Expand the length of the current filled list
	}
	metric.Values = append(metric.Values, value)
}

func FindMetric(row *state.Row, name string) *state.Metric {
	for _, m := range row.Metrics {
		if m.Name == name {
			return m
		}
	}
	return nil
}

func AppendResult(row *state.Row, result state.Row_Result, count int) {
	latest := int32(result)
	n := len(row.Results)
	switch {
	case n == 0, row.Results[n-2] != latest:
		row.Results = append(row.Results, latest, int32(count))
	default:
		row.Results[n-1] += int32(count)
	}
	for i := 0; i < count; i++ {
		row.CellIds = append(row.CellIds, fmt.Sprintf("%d", uniq))
		row.Messages = append(row.Messages, fmt.Sprintf("messsage %d", uniq))
		row.Icons = append(row.Icons, string(int('A')+uniq%26))
		uniq++
	}
}

type NameConfig struct {
	format string
	parts  []string
}

func MakeNameConfig(tnc *config.TestNameConfig) NameConfig {
	if tnc == nil {
		return NameConfig{
			format: "%s",
			parts:  []string{"Tests name"},
		}
	}
	nc := NameConfig{
		format: tnc.NameFormat,
		parts:  make([]string, len(tnc.NameElements)),
	}
	for i, e := range tnc.NameElements {
		nc.parts[i] = e.TargetConfig
	}
	return nc
}

func (r Row) Format(config NameConfig, meta map[string]string) string {
	parsed := make([]interface{}, len(config.parts))
	for i, p := range config.parts {
		if v, ok := r.Metadata[p]; ok {
			parsed[i] = v
			continue
		}
		parsed[i] = meta[p] // "" if missing
	}
	return fmt.Sprintf(config.format, parsed...)
}

func AppendColumn(headers []string, format NameConfig, grid *state.Grid, rows map[string]*state.Row, build Column) {
	c := state.Column{
		Build:   build.Id,
		Started: float64(build.Started * 1000),
	}
	for _, h := range headers {
		if build.Finished == 0 {
			c.Extra = append(c.Extra, "")
			continue
		}
		trunc := 0
		if h == "Commit" { // TODO(fejta): fix
			h = "repo-commit"
			trunc = 9
		}
		v, ok := build.Metadata[h]
		if !ok {
			log.Printf("%s metadata missing %s", c.Build, h)
			v = "missing"
		}
		if trunc > 0 && trunc < len(v) {
			v = v[0:trunc]
		}
		c.Extra = append(c.Extra, v)
	}
	grid.Columns = append(grid.Columns, &c)

	missing := map[string]*state.Row{}
	for name, row := range rows {
		missing[name] = row
	}

	found := map[string]bool{}

	for target, results := range build.Rows {
		for _, br := range results {
			prefix := br.Format(format, build.Metadata)
			name := prefix
			// Ensure each name is unique
			// If we have multiple results with the same name foo
			// then append " [n]" to the name so we wind up with:
			//   foo
			//   foo [1]
			//   foo [2]
			//   etc
			for idx := 1; found[name]; idx++ {
				// found[name] exists, so try foo [n+1]
				name = fmt.Sprintf("%s [%d]", prefix, idx)
			}
			// hooray, name not in found
			found[name] = true
			delete(missing, name)
			r, ok := rows[name]
			if !ok {
				r = &state.Row{
					Name: name,
					Id:   target,
				}
				rows[name] = r
				grid.Rows = append(grid.Rows, r)
				if n := len(grid.Columns); n > 0 {
					// Add missing entries for later builds
					AppendResult(r, state.Row_NO_RESULT, n-1)
				}
			}

			AppendResult(r, br.Result, 1)
			for k, v := range br.Metrics {
				m := FindMetric(r, k)
				if m == nil {
					m = &state.Metric{Name: k}
					r.Metrics = append(r.Metrics, m)
				}
				AppendMetric(m, int32(len(r.Messages)), v)
			}
		}
	}

	for _, row := range missing {
		AppendResult(row, state.Row_NO_RESULT, 1)
	}
}

const elapsedKey = "seconds-elapsed"

// junit_CONTEXT_TIMESTAMP_THREAD.xml
var re = regexp.MustCompile(`.+/junit(_[^_]+)?(_\d+-\d+)?(_\d+)?\.xml$`)

// dropPrefix removes the _ in _CONTEXT to help keep the regexp simple
func dropPrefix(name string) string {
	if len(name) == 0 {
		return name
	}
	return name[1:]
}

func ValidateName(name string) map[string]string {
	// Expected format: junit_context_20180102-1256-07
	// Results in {
	//   "Context": "context",
	//   "Timestamp": "20180102-1256",
	//   "Thread": "07",
	// }
	mat := re.FindStringSubmatch(name)
	if mat == nil {
		return nil
	}
	return map[string]string{
		"Context":   dropPrefix(mat[1]),
		"Timestamp": dropPrefix(mat[2]),
		"Thread":    dropPrefix(mat[3]),
	}

}

func ReadBuild(build Build) (*Column, error) {
	br := Column{
		Id: path.Base(build.Prefix),
	}
	s := build.Bucket.Object(build.Prefix + "started.json")
	sr, err := s.NewReader(build.Context)
	if err != nil {
		return nil, fmt.Errorf("build has not started")
	}
	var started Started
	if err = json.NewDecoder(sr).Decode(&started); err != nil {
		return nil, fmt.Errorf("could not decode started.json: %v", err)
	}
	br.Started = started.Timestamp
	br.Rows = map[string][]Row{}

	f := build.Bucket.Object(build.Prefix + "finished.json")
	fr, err := f.NewReader(build.Context)
	if err == storage.ErrObjectNotExist {
		br.Rows["Overall"] = []Row{
			{
				Result: br.Overall(),
				Metadata: map[string]string{
					"Tests name": "Overall",
				},
			},
		}
		return &br, nil
	}

	var finished Finished
	if err = json.NewDecoder(fr).Decode(&finished); err != nil {
		return nil, fmt.Errorf("could not decode finished.json: %v", err)
	}

	br.Finished = finished.Timestamp
	br.Metadata = finished.Metadata.ColumnMetadata()
	br.Passed = finished.Passed

	br.Rows["Overall"] = []Row{
		{
			Result: br.Overall(),
			Metrics: map[string]float64{
				elapsedKey: float64(br.Finished - br.Started),
			},
			Metadata: map[string]string{
				"Tests name": "Overall",
			},
		},
	}

	ai := build.Bucket.Objects(build.Context, &storage.Query{Prefix: build.Prefix + "artifacts/"})
	artifacts := map[string]map[string]string{}
	for {
		a, err := ai.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list artifacts: %v", err)
		}

		meta := ValidateName(a.Name)
		if meta == nil {
			continue
		}
		artifacts[a.Name] = meta
	}
	for ap, meta := range artifacts {
		ar, err := build.Bucket.Object(ap).NewReader(build.Context)
		if err != nil {
			return nil, fmt.Errorf("could not read %s: %v", ap, err)
		}
		if r := ar.Remain(); r > 50e6 {
			return nil, fmt.Errorf("too large: %s is %d > 50M", ap, r)
		}
		buf, err := ioutil.ReadAll(ar)
		if err != nil {
			return nil, fmt.Errorf("failed to read all of %s: %v", ap, err)
		}

		if err = extractRows(buf, br.Rows, meta); err != nil {
			return nil, fmt.Errorf("failed to parse %s: %v", ap, err)
		}
	}
	return &br, nil
}

type Builds []Build

func (b Builds) Len() int      { return len(b) }
func (b Builds) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b Builds) Less(i, j int) bool {
	return sortorder.NaturalLess(b[i].Prefix, b[j].Prefix)
}

func ListBuilds(client *storage.Client, ctx context.Context, path string, builds chan Build) error {
	u, err := url.Parse(path)
	if err != nil {
		return fmt.Errorf("could not parse %s: %v", path, err)
	}
	if u.Scheme != "gs" {
		return fmt.Errorf("only gs:// paths supported: %s", path)
	}
	if len(u.Host) == 0 {
		return fmt.Errorf("empty host: %s", path)
	}
	if len(u.Path) < 2 {
		return fmt.Errorf("empty path: %s", path)
	}
	b := u.Host
	p := u.Path[1:]
	if p[len(p)-1] != '/' {
		p += "/"
	}
	bkt := client.Bucket(b)
	it := bkt.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    p,
	})
	fmt.Println("Looking in ", u, b, p)
	var all Builds
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}
		if len(objAttrs.Prefix) == 0 {
			continue
		}

		//fmt.Println("Found name:", objAttrs.Name, "prefix:", objAttrs.Prefix)
		all = append(all, Build{
			Bucket:  bkt,
			Context: ctx,
			Prefix:  objAttrs.Prefix,
		})
	}
	// Expect builds to be in monotonically increasing order.
	// So build9 should be followed by build10 or build888 but not build8
	sort.Sort(all)
	// Iterate backwards since the largest (and thus most recent) is at the end.
	for i := len(all) - 1; i >= 0; i-- {
		builds <- all[i]
	}
	return nil
}

func Headers(group config.TestGroup) []string {
	var extra []string
	for _, h := range group.ColumnHeader {
		extra = append(extra, h.ConfigurationValue)
	}
	return extra
}

type Rows []*state.Row

func (r Rows) Len() int      { return len(r) }
func (r Rows) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r Rows) Less(i, j int) bool {
	return sortorder.NaturalLess(r[i].Name, r[j].Name)
}

func ReadBuilds(group config.TestGroup, builds chan Build, max int, dur time.Duration) state.Grid {
	i := 0
	var stop time.Time
	if dur != 0 {
		stop = time.Now().Add(-dur)
	}
	grid := &state.Grid{}
	h := Headers(group)
	nc := MakeNameConfig(group.TestNameConfig)
	rows := map[string]*state.Row{}
	log.Printf("Reading builds after %s (%d)", stop, stop.Unix())
	for b := range builds {
		i++
		if max > 0 && i > max {
			log.Printf("Hit ceiling of %d results", max)
			break
		}
		br, err := ReadBuild(b)
		if err != nil {
			log.Printf("FAIL %s: %v", b.Prefix, err)
			continue
		}
		AppendColumn(h, nc, grid, rows, *br)
		log.Printf("found: %s pass:%t %d-%d: %d results", br.Id, br.Passed, br.Started, br.Finished, len(br.Rows))
		if br.Started < stop.Unix() {
			log.Printf("Latest result before %s", stop)
			break
		}
	}
	log.Println("Finished reading builds.")
	for range builds {
	}
	sort.Stable(Rows(grid.Rows))
	return *grid
}

func Days(d float64) time.Duration {
	return time.Duration(24*d) * time.Hour // Close enough
}

func ReadConfig(obj *storage.ObjectHandle, ctx context.Context) (*config.Configuration, error) {
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open config: %v", err)
	}
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %v", err)
	}
	var cfg config.Configuration
	if err = proto.Unmarshal(buf, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse: %v", err)
	}
	return &cfg, nil
}

func Group(cfg config.Configuration, name string) (*config.TestGroup, bool) {
	for _, g := range cfg.TestGroups {
		if g.Name == name {
			return g, true
		}
	}
	return nil, false
}

func main() {
	b := "fejternetes"
	o := "ci-kubernetes-test-go"
	o = "ci-kubernetes-node-kubelet-stable3"

	var _ summary.Summary
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}

	cfg, err := ReadConfig(client.Bucket(b).Object("config"), ctx)
	if err != nil {
		log.Fatalf("Failed to read gs://%s/config: %v", b, err)
	}
	tg, ok := Group(*cfg, o)
	if !ok {
		log.Fatalf("Failed to find %s in gs://%s/config", o, b)
	}
	log.Println(tg)

	bkt := client.Bucket("kubernetes-jenkins")
	attrs, err := bkt.Attrs(ctx)
	if err != nil {
		log.Fatalf("Failed to access bucket: %v", err)
	}
	fmt.Printf("bucket %s, attrs %v", bkt, attrs)
	g := state.Grid{}
	g.Columns = append(g.Columns, &state.Column{Build: "first", Started: 1})
	fmt.Println(g)
	builds := make(chan Build)
	uPR := "gs://kubernetes-jenkins/pr-logs/pull-ingress-gce-e2e"
	uCI := "gs://kubernetes-jenkins/logs/ci-kubernetes-test-go"
	uCIN := "gs://kubernetes-jenkins/logs/ci-kubernetes-node-kubelet-stable3"
	_ = uCI
	_ = uPR
	u := uCIN
	go func() {
		if err = ListBuilds(client, ctx, u, builds); err != nil {
			log.Fatalf("Failed to list builds: %v", err)
		}
		close(builds)
	}()
	grid := ReadBuilds(*tg, builds, 1000, Days(5))
	log.Printf("Grid: %d %s", len(grid.Columns), grid.String())
	buf, err := proto.Marshal(&grid)
	if err != nil {
		log.Fatalf("Failed to encode grid: %v", err)
	}
	var zbuf bytes.Buffer
	zw := zlib.NewWriter(&zbuf)
	if _, err = zw.Write(buf); err != nil {
		log.Fatalf("Failed to compress gs://%s/%s: %v", b, o, err)
	}
	if err = zw.Close(); err != nil {
		log.Fatalf("Failed to close zlib gs://%s/%s buffer: %v", b, o, err)
	}
	if b == "k8s-testgrid" {
		log.Fatalf("do not change prod")
	}
	w := client.Bucket(b).Object(o).NewWriter(ctx)
	w.SendCRC32C = true
	buf = zbuf.Bytes()
	// Send our CRC32 to ensure google received the same data we sent.
	// See checksum example at:
	// https://godoc.org/cloud.google.com/go/storage#Writer.Write
	w.ObjectAttrs.CRC32C = crc32.Checksum(buf, crc32.MakeTable(crc32.Castagnoli))
	w.ProgressFunc = func(bytes int64) {
		log.Printf("Uploading gs://%s/%s: %d/%d...", b, o, bytes, len(buf))
	}
	if n, err := w.Write(buf); err != nil {
		log.Fatalf("Failed to write gs://%s/%s: %v", b, o, err)
	} else if n != len(buf) {
		log.Fatalf("Partial gs://%s/%s write: %d < %d", b, o, n, len(buf))
	}
	if err = w.Close(); err != nil {
		log.Fatalf("Failed to close write to gs://%s/%s: %v", b, o, err)
	}
	log.Print("Success!")
}
