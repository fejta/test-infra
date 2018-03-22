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
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/test-infra/testgrid/config"
	"k8s.io/test-infra/testgrid/state"

	"cloud.google.com/go/storage"
	"github.com/golang/protobuf/proto"
	"google.golang.org/api/iterator"

	"vbom.ml/util/sortorder"
)

// options configures the updater
type options struct {
	config           gcsPath // gs://path/to/config/proto
	creds            string  // TODO(fejta): implement
	confirm          bool    // TODO(fejta): implement
	group            string
	groupConcurrency int
	buildConcurrency int
}

// validate ensures sane options
func (o *options) validate() error {
	if o.config.String() == "" {
		return errors.New("empty --config")
	}
	if o.config.bucket() == "k8s-testgrid" { // TODO(fejta): remove
		return fmt.Errorf("--config=%s cannot start with gs://k8s-testgrid", o.config)
	}
	if o.groupConcurrency == 0 {
		o.groupConcurrency = 4 * runtime.NumCPU()
	}
	if o.buildConcurrency == 0 {
		o.buildConcurrency = 4 * runtime.NumCPU()
	}

	return nil
}

// gatherOptions reads options from flags
func gatherOptions() options {
	o := options{}
	flag.Var(&o.config, "config", "gs://path/to/config.pb")
	flag.StringVar(&o.creds, "gcp-service-account", "", "/path/to/gcp/creds (use local creds if empty")
	flag.BoolVar(&o.confirm, "confirm", false, "Upload data if set")
	flag.StringVar(&o.group, "test-group", "", "Only update named group if set")
	flag.IntVar(&o.groupConcurrency, "group-concurrency", 0, "Manually define the number of groups to concurrently update if non-zero")
	flag.IntVar(&o.buildConcurrency, "build-concurrency", 0, "Manually define the number of builds to concurrently read if non-zero")
	flag.Parse()
	return o
}

// gcsPath parses gs://bucket/obj urls
type gcsPath struct {
	url url.URL
}

// String() returns the gs://bucket/obj url
func (g gcsPath) String() string {
	return g.url.String()
}

// Set() updates value from a gs://bucket/obj string, validating errors.
func (g *gcsPath) Set(v string) error {
	u, err := url.Parse(v)
	switch {
	case err != nil:
		return fmt.Errorf("invalid gs:// url %s: %v", v, err)
	case u.Scheme != "gs":
		return fmt.Errorf("must use a gs:// url: %s", v)
	case strings.Contains(u.Host, ":"):
		return fmt.Errorf("gs://bucket may not contain a port: %s", v)
	case u.Opaque != "":
		return fmt.Errorf("url must start with gs://: %s", v)
	case u.User != nil:
		return fmt.Errorf("gs://bucket may not contain an user@ prefix: %s", v)
	case u.RawQuery != "":
		return fmt.Errorf("gs:// url may not contain a ?query suffix: %s", v)
	case u.Fragment != "":
		return fmt.Errorf("gs:// url may not contain a #fragment suffix: %s", v)
	}
	g.url = *u
	return nil
}

// bucket() returns bucket in gs://bucket/obj
func (g gcsPath) bucket() string {
	return g.url.Host
}

// object() returns path/to/something in gs://bucket/path/to/something
func (g gcsPath) object() string {
	if g.url.Path == "" {
		return g.url.Path
	}
	return g.url.Path[1:]
}

// testGroup() returns the path to a test_group proto given this proto
func (g gcsPath) testGroup(name string) gcsPath {
	newG := g
	newG.url.Path = path.Join(path.Dir(g.url.Path), name)
	return newG
}

type Build struct {
	Bucket  *storage.BucketHandle
	Context context.Context
	Prefix  string
	number  *int
}

func (b Build) String() string {
	return b.Prefix
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
	running    bool
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
	Failure   *string `xml:"failure,omitempty"`
	Output    *string `xml:"system-out,omitempty"`
	Error     *string `xml:"system-err,omitempty"`
	Skipped   *string `xml:"skipped,omitempty"`
}

func (jr JunitResult) Message() string {
	const max = 140
	var msg string
	switch {
	case jr.Failure != nil && *jr.Failure != "":
		msg = *jr.Failure
	case jr.Skipped != nil && *jr.Skipped != "":
		msg = *jr.Skipped
	case jr.Output != nil && *jr.Output != "":
		msg = *jr.Output
	}
	if l := len(msg); max == 0 || l <= max {
		return msg
	} else {
		h := max / 2
		return msg[:h] + "..." + msg[l-h-1:]
	}
}

func (jr JunitResult) Row(suite string) (string, Row) {
	n := jr.Name
	if suite != "" {
		n = suite + "." + n
	}
	r := Row{
		Metrics: map[string]float64{},
		Metadata: map[string]string{
			"Tests name": n,
		},
	}
	if jr.Time > 0 {
		r.Metrics[elapsedKey] = jr.Time
	}
	if msg := jr.Message(); msg != "" {
		r.Message = msg
	}
	switch {
	case jr.Failure != nil:
		r.Result = state.Row_FAIL
		if r.Message != "" {
			r.Icon = "F"
		}
	case jr.Skipped != nil:
		r.Result = state.Row_PASS_WITH_SKIPS
		if r.Message != "" {
			r.Icon = "S"
		}
	default:
		r.Result = state.Row_PASS
	}
	return n, r
}

func unmarshalXML(buf []byte, i interface{}) error {
	reader := bytes.NewReader(buf)
	dec := xml.NewDecoder(reader)
	dec.CharsetReader = func(charset string, input io.Reader) (io.Reader, error) {
		switch charset {
		case "UTF-8", "utf8", "":
			// utf8 is not recognized by golang, but our coalesce.py writes a utf8 doc, which python accepts.
			return input, nil
		default:
			return nil, fmt.Errorf("unknown charset: %s", charset)
		}
	}
	return dec.Decode(i)
}

func extractRows(buf []byte, meta map[string]string) (map[string][]Row, error) {
	var suites JunitSuites
	// Try to parse it as a <testsuites/> object
	err := unmarshalXML(buf, &suites)
	if err != nil {
		// Maybe it is a <testsuite/> object instead
		suites.Suites = append([]JunitSuite(nil), JunitSuite{})
		ie := unmarshalXML(buf, &suites.Suites[0])
		if ie != nil {
			// Nope, it just doesn't parse
			return nil, fmt.Errorf("not valid testsuites: %v nor testsuite: %v", err, ie)
		}
	}
	rows := map[string][]Row{}
	for _, suite := range suites.Suites {
		for _, sr := range suite.Results {
			if sr.Skipped != nil && len(*sr.Skipped) == 0 {
				continue
			}

			n, r := sr.Row(suite.Name)
			for k, v := range meta {
				r.Metadata[k] = v
			}
			rows[n] = append(rows[n], r)
		}
	}
	return rows, nil
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
	Message  string
	Icon     string
}

func (br Column) Overall() Row {
	r := Row{
		Metadata: map[string]string{"Tests name": "Overall"},
	}
	switch {
	case br.Finished > 0:
		// Completed, did we pass?
		if br.Passed {
			r.Result = state.Row_PASS // Yep
		} else {
			r.Result = state.Row_FAIL
		}
		r.Metrics = map[string]float64{
			elapsedKey: float64(br.Finished - br.Started),
		}
	case time.Now().Add(-24*time.Hour).Unix() > br.Started:
		// Timed out
		r.Result = state.Row_FAIL
		r.Message = "Testing did not complete within 24 hours"
		r.Icon = "T"
	default:
		r.Result = state.Row_RUNNING
		r.Message = "Still running; has not finished..."
		r.Icon = "R"
	}
	return r
}

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

var noResult = Row{Result: state.Row_NO_RESULT}

func AppendResult(row *state.Row, rowResult Row, count int) {
	latest := int32(rowResult.Result)
	n := len(row.Results)
	switch {
	case n == 0, row.Results[n-2] != latest:
		row.Results = append(row.Results, latest, int32(count))
	default:
		row.Results[n-1] += int32(count)
	}

	for i := 0; i < count; i++ { // TODO(fejta): update server to allow empty cellids
		row.CellIds = append(row.CellIds, "")
	}

	// Javascript client expects no result cells to skip icons/messages
	// TODO(fejta): reconsider this
	if rowResult.Result != state.Row_NO_RESULT {
		for i := 0; i < count; i++ {
			row.Messages = append(row.Messages, rowResult.Message)
			row.Icons = append(row.Icons, rowResult.Icon)
		}
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
		var ah string
		if h == "Commit" { // TODO(fejta): fix, jobs use explicit key, support truncation
			h = "repo-commit"
			trunc = 9
			ah = "job-version"
		}
		v, ok := build.Metadata[h]
		if !ok {
			// TODO(fejta): fix, make jobs use one or the other
			if ah == "" {
				log.Printf("  %s metadata missing %s", c.Build, h)
				v = "missing"
			} else {
				if av, ok := build.Metadata[ah]; ok {
					parts := strings.SplitN(av, "+", 2)
					v = parts[len(parts)-1]
				} else {
					log.Printf("  %s metadata missing both keys %s and alternate %s", c.Build, h, ah)
				}
			}
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

			// Does this row already exist?
			r, ok := rows[name]
			if !ok { // New row
				r = &state.Row{
					Name: name,
					Id:   target,
				}
				rows[name] = r
				grid.Rows = append(grid.Rows, r)
				if n := len(grid.Columns); n > 1 {
					// Add missing entries for more recent builds (aka earlier columns)
					AppendResult(r, noResult, n-1)
				}
			}

			AppendResult(r, br, 1)
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
		AppendResult(row, noResult, 1)
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
	var wg sync.WaitGroup                                             // Each subtask does wg.Add(1), then we wg.Wait() for them to finish
	ctx, cancel := context.WithTimeout(build.Context, 30*time.Second) // Allows aborting after first error
	ec := make(chan error)                                            // Receives errors from anyone

	// Download started.json, send to sc
	wg.Add(1)
	sc := make(chan Started) // Receives started.json result
	go func() {
		defer wg.Done()
		started, err := func() (Started, error) {
			var started Started
			s := build.Bucket.Object(build.Prefix + "started.json")
			sr, err := s.NewReader(ctx)
			if err != nil {
				return started, fmt.Errorf("build has not started")
			}
			if err = json.NewDecoder(sr).Decode(&started); err != nil {
				return started, fmt.Errorf("could not decode started.json: %v", err)
			}
			return started, nil
		}()
		if err != nil {
			select {
			case <-ctx.Done():
			case ec <- err:
			}
			return
		}
		select {
		case <-ctx.Done():
		case sc <- started:
		}
	}()

	// Download finished.json, send to fc
	wg.Add(1)
	fc := make(chan Finished) // Receives finished.json result
	go func() {
		defer wg.Done()
		finished, err := func() (Finished, error) {
			f := build.Bucket.Object(build.Prefix + "finished.json")
			fr, err := f.NewReader(ctx)
			var finished Finished
			if err == storage.ErrObjectNotExist { // Job has not (yet) completed
				finished.running = true
				return finished, nil
			} else if err != nil {
				return finished, fmt.Errorf("could not open %s: %v", f, err)
			}
			if err = json.NewDecoder(fr).Decode(&finished); err != nil {
				return finished, fmt.Errorf("could not decode finished.json: %v", err)
			}
			return finished, nil
		}()
		if err != nil {
			select {
			case <-ctx.Done():
			case ec <- err:
			}
			return
		}
		select {
		case <-ctx.Done():
		case fc <- finished:
		}
	}()

	// List artifacts, send to ac channel
	wg.Add(1)
	ac := make(chan string) // Receives names of arifacts
	go func() {
		defer wg.Done()
		defer close(ac) // No more artifacts
		err := func() error {
			pref := build.Prefix + "artifacts/"
			ai := build.Bucket.Objects(ctx, &storage.Query{Prefix: pref})
			for {
				a, err := ai.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to list %s: %v", pref, err)
				}
				select {
				case <-ctx.Done():
					return fmt.Errorf("interrupted listing %s", pref)
				case ac <- a.Name: // Added
				}
			}
			return nil
		}()
		if err != nil {
			select {
			case <-ctx.Done():
			case ec <- err:
			}
		}
	}()

	// Download each artifact, send row map to rc
	// With parallelism: 60s without: 220s
	wg.Add(1)
	rc := make(chan map[string][]Row)
	go func() {
		defer wg.Done()
		defer close(rc) // No more rows
		var awg sync.WaitGroup
		for a := range ac {
			select { // Should we stop?
			case <-ctx.Done(): // Yes
				return
			default: // No, keep going
			}
			meta := ValidateName(a)
			if meta == nil { // Not junit
				continue
			}
			awg.Add(1)
			// Read each artifact in a new thread
			go func(ap string, meta map[string]string) {
				defer awg.Done()
				err := func() error {
					ar, err := build.Bucket.Object(ap).NewReader(ctx)
					if err != nil {
						return fmt.Errorf("could not read %s: %v", ap, err)
					}
					if r := ar.Remain(); r > 50e6 {
						return fmt.Errorf("too large: %s is %d > 50M", ap, r)
					}
					buf, err := ioutil.ReadAll(ar)
					if err != nil {
						return fmt.Errorf("partial read of %s: %v", ap, err)
					}

					select { // Keep going?
					case <-ctx.Done(): // No, cancelled
						return errors.New("aborted artifact read")
					default: // Yes, acquire lock
						// TODO(fejta): consider sync.Map
						if rows, err := extractRows(buf, meta); err != nil {
							return fmt.Errorf("failed to parse %s: %v", ap, err)
						} else {
							rc <- rows
						}
					}
					return nil
				}()
				if err == nil {
					return
				}
				select {
				case <-ctx.Done():
				case ec <- err:
				}
			}(a, meta)
		}
		awg.Wait()
	}()

	// Append each row into the column
	rows := map[string][]Row{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range rc {
			select { // Should we continue
			case <-ctx.Done(): // No, aborted
				return
			default: // Yes
			}
			for t, rs := range r {
				rows[t] = append(rows[t], rs...)
			}
		}
	}()

	// Wait for everyone to complete their work
	go func() {
		wg.Wait()
		select {
		case <-ctx.Done():
			return
		case ec <- nil:
		}
	}()
	var finished *Finished
	var started *Started
	for { // Wait until we receive started and finished and/or an error
		select {
		case err := <-ec:
			if err != nil {
				cancel()
				return nil, fmt.Errorf("failed to read %s: %v", build, err)
			}
			break
		case s := <-sc:
			started = &s
		case f := <-fc:
			finished = &f
		}
		if started != nil && finished != nil {
			break
		}
	}
	br := Column{
		Id:      path.Base(build.Prefix),
		Started: started.Timestamp,
	}
	// Has the build finished?
	if finished.running { // No
		cancel()
		br.Rows = map[string][]Row{
			"Overall": {br.Overall()},
		}
		return &br, nil
	}
	br.Finished = finished.Timestamp
	br.Metadata = finished.Metadata.ColumnMetadata()
	br.Passed = finished.Passed
	or := br.Overall()
	br.Rows = map[string][]Row{
		"Overall": {or},
	}
	select {
	case <-ctx.Done():
		cancel()
		return nil, fmt.Errorf("interrupted reading %s", build)
	case err := <-ec:
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to read %s: %v", build, err)
		}
	}

	for t, rs := range rows {
		br.Rows[t] = append(br.Rows[t], rs...)
	}
	if or.Result == state.Row_FAIL { // Ensure failing build has a failing row
		ft := false
		for n, rs := range br.Rows {
			if n == "Overall" {
				continue
			}
			for _, r := range rs {
				if r.Result == state.Row_FAIL {
					ft = true // Failing test, huzzah!
					break
				}
			}
			if ft {
				break
			}
		}
		if !ft { // Nope, add the F icon and an explanatory message
			br.Rows["Overall"][0].Icon = "F"
			br.Rows["Overall"][0].Message = "Build failed outside of test results"
		}
	}

	cancel()
	return &br, nil
}

type Builds []Build

func (b Builds) Len() int      { return len(b) }
func (b Builds) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b Builds) Less(i, j int) bool {
	return sortorder.NaturalLess(b[i].Prefix, b[j].Prefix)
}

// listBuilds lists and sorts builds under path, sending them to the builds channel.
func listBuilds(client *storage.Client, ctx context.Context, path gcsPath) (Builds, error) {
	log.Printf("LIST: %s", path)
	p := path.object()
	if p[len(p)-1] != '/' {
		p += "/"
	}
	bkt := client.Bucket(path.bucket())
	it := bkt.Objects(ctx, &storage.Query{
		Delimiter: "/",
		Prefix:    p,
	})
	var all Builds
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %v", err)
		}
		if len(objAttrs.Prefix) == 0 {
			continue
		}

		all = append(all, Build{
			Bucket:  bkt,
			Context: ctx,
			Prefix:  objAttrs.Prefix,
		})
	}
	// Expect builds to be in monotonically increasing order.
	// So build9 should be followed by build10 or build888 but not build8
	sort.Sort(sort.Reverse(all))
	return all, nil
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

func ReadBuilds(parent context.Context, group config.TestGroup, builds Builds, max int, dur time.Duration, concurrency int) (*state.Grid, error) {
	// Spawn build readers
	if concurrency == 0 {
		return nil, fmt.Errorf("zero readers for %s", group.Name)
	}
	ctx, cancel := context.WithCancel(parent)
	var stop time.Time
	if dur != 0 {
		stop = time.Now().Add(-dur)
	}
	lb := len(builds)
	if lb > max {
		log.Printf("  Truncating %d %s results to %d", lb, group.Name, max)
		lb = max
	}
	cols := make([]*Column, lb)
	log.Printf("UPDATE: %s since %s (%d)", group.Name, stop, stop.Unix())
	ec := make(chan error)
	old := make(chan int)
	var wg sync.WaitGroup

	// Send build indices to readers
	indices := make(chan int)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(indices)
		for i := range builds[:lb] {
			select {
			case <-ctx.Done():
				return
			case <-old:
				return
			case indices <- i:
			}
		}
	}()

	// Concurrently receive indicies and read builds
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case i, open := <-indices:
					if !open {
						return
					}
					b := builds[i]
					c, err := ReadBuild(b)
					if err != nil {
						ec <- err
						return
					}
					cols[i] = c
					if c.Started < stop.Unix() {
						select {
						case <-ctx.Done():
						case old <- i:
							log.Printf("STOP: %d %s started at %d < %d", i, b.Prefix, c.Started, stop.Unix())
						default: // Someone else may have already reported an old result
						}
					}
				}
			}
		}()
	}

	// Wait for everyone to finish
	go func() {
		wg.Wait()
		select {
		case <-ctx.Done():
		case ec <- nil: // No error
		}
	}()

	// Determine if we got an error
	select {
	case <-ctx.Done():
		cancel()
		return nil, fmt.Errorf("interrupted reading %s", group.Name)
	case err := <-ec:
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error reading %s: %v", group.Name, err)
		}
	}

	// Add the columns into a grid message
	grid := &state.Grid{}
	rows := map[string]*state.Row{} // For fast target => row lookup
	h := Headers(group)
	nc := MakeNameConfig(group.TestNameConfig)
	for _, c := range cols {
		select {
		case <-ctx.Done():
			cancel()
			return nil, fmt.Errorf("interrupted appending columns to %s", group.Name)
		default:
		}
		if c == nil {
			continue
		}
		AppendColumn(h, nc, grid, rows, *c)
		if c.Started < stop.Unix() { // There may be concurrency results < stop.Unix()
			log.Printf("  %s#%s before %s, stopping...", group.Name, c.Id, stop)
			break // Just process the first result < stop.Unix()
		}
	}
	sort.Stable(Rows(grid.Rows))
	cancel()
	return grid, nil
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

type outageConfig struct {
	staleHours int
	failureToOpen int
	passesToClose int
}

func makeOutageConfig(tab config.DashboardTab, group config.TestGroup) outageConfig {
	// Are outages configured by the tab?
	if opt := tab.AlertOptions; opt != nil {
		return outageConfig{
			staleHours: opt.AlertStaleResultsHours,
			failuresToOpen: opt.NumFailuresToAlert,
			passesToClose: opt.NumPassesToDisableAlert,
		}
	}
	// No they are set by the test group
	return outageConfig{
		staleHours: group.AlertStaleResultsHours,
		failuresToOpen: group.NumFailuresToAlert,
		passesToClose: group.NumPassesToDisableAlert,
	}
}

type resultGroup struct {
	result state.Row_Result
	count int
}

func groupResults(results []int) []resultGroup {
	var rg []resultGroup
	var latest *resultGroup
	for i := 0; i + 1 < len(results); i += 2 {
		r := normalize(results[i])
		n := results[i+1]
		if r == 
		if latest == nil || latest.result != r {
			if latest != nil {
				rg = append(rg, latest)
			}
			latest = &resultGroup{
				result: r,
				count: n,
			}
		} else {
			latest.count += n
		}
	}
	return rg
}

func normalize(result int) state.Row_Result {
	switch state.Row_Result(result) {
	case state.Row_NO_RESULT, state.Row_RUNNING:
		return state.Row_NO_RESULT
	case state.Row_PASS, state.Row_PASS_WITH_ERRORS, state.Row_PASS_WITH_SKIPS, state.Row_FLAKY:
		return state.Row_PASS
	default:
		return state.Row_FAIL
	}
}

func classifyRows(grid state.Grid, cfg outageConfig) map[summary.TabSummary_TabStatus][]state.Row {
	classes := map[summary.TabSummary_TabStatus][]state.Row{}
	red := cfg.failsToOpen
	green := cfg.passesToClose
	for _, r := range grid.Rows {
		rgs := groupResults(r.Results)
		sgs := groupStatuses(rgs)
		for _, sg := range sgs {
			switch {
			case sg.status == summary.TabSummary_PASS && sg.count > green:
				classes[sg.status] = append(classes[sg.status], r)
			case sg.status == summary.TabSummary_FAIL && sg.count > red:
				classes[sg.status] = append(classes[sg.status], r)
			}
		}
	}
}

func summarizeTab(tab config.DashboardTab, group config.TestGroup, grid state.Grid) summary.TabSummary {


	s := summary.TabSummary{
		Name: tab.Name,
		Updated: foo,
		StatusMessage: foo,
		Status: summary.TabSummary_FAIL, // _FLAKY, etc
		LatestGreen: foo,
		LatestRun: foo,
	}
	return s
}

func main() {
	for dashboard := range config.Dashboards {
		for tab := range dashboard.DashboardTab {
			group := tab.TestGroupName
			// TODO(fejta): are these used by summary?
			opt := tab.AlertOptions
			staleHours := opt.AlertStaleResultsHourts
			failuresToAlert := opt.NumFailuresToAlert

			// TODO(fejta): used by anything??
			recentCols := tab.NumColumnsRecent

			alertStaleResultsHours := tg.AlertStaleResultsHours
			failuresToOpen := tg.NumFailuresToAlert
			passesToClose := tg.NumPassesToDisableAlert
		}
	}
}

type finder interface {
	findGroup(name string) config.TestGroup
	findDashboard(name string) config.Dashboard
	findTab(name string) config.DashboardTab
	findGrid(tab string) state.Grid
}

type Configuration struct {
	cfg config.Configuration
}

func (c *Configuration) findGroup(name string) *config.TestGroup {
	for _, tg := range c.cfg.TestGroups {
		if tg.Name == name {
			return tg
		}
	}
	return nil
}

func (c *Configuration) findDashboard(name string) *config.Dashboard {
	if g, ok := c.groups[name]; !ok {
		for _, tg := range c.cfg.TestGroups {
			if tg.Name == name {
				c.groups[name] = &tg
				return &tg
			}
		}
		c.groups[name] = nil
		return nil
	}
	return c.groups[name]



func main() {
	opt := gatherOptions()
	if err := opt.validate(); err != nil {
		log.Fatalf("Invalid flags: %v", err)
	}
	if opt.creds != "" {
		log.Fatalf("Service accounts are not yet supported")
	}
	// opt.confirm

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}

	cfg, err := ReadConfig(client.Bucket(opt.config.bucket()).Object(opt.config.object()), ctx)
	if err != nil {
		log.Fatalf("Failed to read %s: %v", opt.config, err)
	}
	log.Printf("Found %d groups", len(cfg.TestGroups))

	dash := "google-gce"
	tab = "gci-gce"

	for _, d := range cfg.Dashboards {
		if d.Name != dash {
			continue
		}
		summarizeDashboard(d)
	}
}

func summarizeDashboard(dashboard config.Dashboard) {
	for _, tab := range dashboard.DashboardTab {
		if tab.Name != "gci-gce" {
			continue
		}
		summarizeTab(tab)
	}
}

func summarizeTab(tab config.DashboardTab) {
	grp := tab.TestGroupName

}

