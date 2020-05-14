/*
Copyright 2020 The Kubernetes Authors.

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

package adapter

import (
	"testing"
	"time"

	"github.com/andygrunwald/go-gerrit"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/test-infra/prow/apis/prowjobs/v1"
	"k8s.io/test-infra/prow/config"
	reporter "k8s.io/test-infra/prow/crier/reporters/gerrit"
)

func TestPresubmitContexts(t *testing.T) {
	cases := []struct {
		name       string
		presubmits []config.Presubmit
		reports    map[string]reporter.JobReport
		failed     sets.String
		all        sets.String
	}{
		{
			name: "basically works",
		},
		{
			name: "simple case works",
			presubmits: []config.Presubmit{
				{
					JobBase: config.JobBase{
						Name: "hello-fail",
					},
				},
				{
					JobBase: config.JobBase{
						Name: "world",
					},
				},
			},
			reports: map[string]reporter.JobReport{
				"whatever": {
					Jobs: []reporter.Job{
						{
							Name:  "hello-fail",
							State: string(v1.FailureState),
						},
						{
							Name:  "world",
							State: "something odd",
						},
					},
				},
			},
			failed: sets.NewString("hello-fail"),
			all:    sets.NewString("hello-fail", "world"),
		},
		{
			name: "aggregate contexts from multiple reports",
			presubmits: []config.Presubmit{
				{
					JobBase: config.JobBase{
						Name: "verified-fail",
					},
				},
				{
					JobBase: config.JobBase{
						Name: "inform-fail",
					},
				},
				{
					JobBase: config.JobBase{
						Name: "pass-verified",
					},
				},
				{
					JobBase: config.JobBase{
						Name: "pass-inform",
					},
				},
			},
			reports: map[string]reporter.JobReport{
				"Verified": {
					Jobs: []reporter.Job{
						{
							Name:  "verified-fail",
							State: string(v1.FailureState),
						},
						{
							Name:  "pass-verified",
							State: "green",
						},
					},
				},
				"Informative": {
					Jobs: []reporter.Job{
						{
							Name:  "inform-fail",
							State: string(v1.ErrorState),
						},
						{
							Name: "pass-inform",
						},
					},
				},
			},
			failed: sets.NewString("verified-fail", "inform-fail"),
			all:    sets.NewString("verified-fail", "inform-fail", "pass-inform", "pass-verified"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotFailed, gotAll := presubmitContexts(tc.presubmits, tc.reports, logrus.WithField("case", tc.name))
			if !equality.Semantic.DeepEqual(tc.failed, gotFailed) {
				t.Errorf("wrong failures:%s", diff.ObjectReflectDiff(tc.failed, gotFailed))
			}
			if !equality.Semantic.DeepEqual(tc.all, gotAll) {
				t.Errorf("wrong all contexts:%s", diff.ObjectReflectDiff(tc.all, gotAll))
			}
		})
	}
}

func stamp(t time.Time) gerrit.Timestamp {
	return gerrit.Timestamp{Time: t}
}

func TestCurrentMessages(t *testing.T) {
	now := time.Now()
	before := now.Add(-time.Minute)
	after := now.Add(time.Hour)
	later := after.Add(time.Hour)
	cases := []struct {
		name   string
		change gerrit.ChangeInfo
		since  time.Time
		want   []string
	}{
		{
			name: "basically works",
		},
		{
			name:  "simple case",
			since: before,
			change: gerrit.ChangeInfo{
				Revisions: map[string]gerrit.RevisionInfo{
					"3": {
						Number: 3,
					},
				},
				CurrentRevision: "3",
				Messages: []gerrit.ChangeMessageInfo{
					{
						RevisionNumber: 3,
						Date:           stamp(now),
						Message:        "now",
					},
					{
						RevisionNumber: 3,
						Date:           stamp(after),
						Message:        "after",
					},
					{
						RevisionNumber: 3,
						Date:           stamp(later),
						Message:        "later",
					},
				},
			},
			want: []string{"now", "after", "later"},
		},
		{
			name:  "reject old messages",
			since: now,
			change: gerrit.ChangeInfo{
				Revisions: map[string]gerrit.RevisionInfo{
					"3": {
						Number: 3,
					},
				},
				CurrentRevision: "3",
				Messages: []gerrit.ChangeMessageInfo{
					{
						RevisionNumber: 3,
						Date:           stamp(now),
						Message:        "now",
					},
					{
						RevisionNumber: 3,
						Date:           stamp(after),
						Message:        "after",
					},
					{
						RevisionNumber: 3,
						Date:           stamp(later),
						Message:        "later",
					},
				},
			},
			want: []string{"after", "later"},
		},
		{
			name:  "reject message from other revisions",
			since: before,
			change: gerrit.ChangeInfo{
				Revisions: map[string]gerrit.RevisionInfo{
					"3": {
						Number: 3,
					},
				},
				CurrentRevision: "3",
				Messages: []gerrit.ChangeMessageInfo{
					{
						RevisionNumber: 3,
						Date:           stamp(now),
						Message:        "3-now",
					},
					{
						RevisionNumber: 4,
						Date:           stamp(after),
						Message:        "4-after",
					},
					{
						RevisionNumber: 3,
						Date:           stamp(later),
						Message:        "3-later",
					},
				},
			},
			want: []string{"3-now", "3-later"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := currentMessages(tc.change, tc.since)
			if !equality.Semantic.DeepEqual(got, tc.want) {
				t.Errorf("wrong messages:%s", diff.ObjectReflectDiff(got, tc.want))
			}
		})
	}
}

func TestMessageFilter(t *testing.T) {
	job := func(name string, patch func(j *config.Presubmit)) config.Presubmit {
		var presubmit config.Presubmit
		presubmit.Name = name
		presubmit.Context = name
		presubmit.Trigger = config.DefaultTriggerFor(name)
		presubmit.RerunCommand = config.DefaultRerunCommandFor(name)
		presubmit.AlwaysRun = true
		if patch != nil {
			patch(&presubmit)
		}
		return presubmit
	}
	type check struct {
		job             config.Presubmit
		shouldRun       bool
		forcedToRun     bool
		defaultBehavior bool
	}
	cases := []struct {
		name     string
		messages []string
		failed   sets.String
		all      sets.String
		checks   []check
	}{
		{
			name: "basically works",
		},
		{
			name:     "/test foo works",
			messages: []string{"/test foo", "/test bar"},
			all:      sets.NewString("foo", "bar", "ignored"),
			checks: []check{
				{
					job:             job("foo", nil),
					shouldRun:       true,
					forcedToRun:     true,
					defaultBehavior: true,
				},
				{
					job:             job("bar", nil),
					shouldRun:       true,
					forcedToRun:     true,
					defaultBehavior: true,
				},
				{
					job:             job("ignored", nil),
					shouldRun:       false,
					forcedToRun:     false,
					defaultBehavior: false,
				},
			},
		},
		{
			name:     "/test all triggers multiple",
			messages: []string{"/test all"},
			all:      sets.NewString("foo", "bar"),
			checks: []check{
				{
					job:             job("foo", nil),
					shouldRun:       true,
					forcedToRun:     false,
					defaultBehavior: false,
				},
				{
					job:             job("bar", nil),
					shouldRun:       true,
					forcedToRun:     false,
					defaultBehavior: false,
				},
			},
		},
		{
			name:     "/retest triggers failures",
			messages: []string{"/retest"},
			failed:   sets.NewString("failed"),
			all:      sets.NewString("foo", "bar", "failed"),
			checks: []check{
				{
					job:             job("foo", nil),
					shouldRun:       false,
					forcedToRun:     false,
					defaultBehavior: false,
				},
				{
					job:             job("failed", nil),
					shouldRun:       true,
					forcedToRun:     false,
					defaultBehavior: true,
				},
				{
					job:             job("bar", nil),
					shouldRun:       false,
					forcedToRun:     false,
					defaultBehavior: false,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, check := range tc.checks {
				t.Run(check.job.Name, func(t *testing.T) {
					fixed := []config.Presubmit{check.job}
					config.SetPresubmitRegexes(fixed)
					check.job = fixed[0]
					logger := logrus.WithField("case", tc.name).WithField("job", check.job.Name)
					filt := messageFilter(tc.messages, tc.failed, tc.all, logger)
					shouldRun, forcedToRun, defaultBehavior := filt(check.job)
					if got, want := shouldRun, check.shouldRun; got != want {
						t.Errorf("shouldRun: got %t, want %t", got, want)
					}
					if got, want := forcedToRun, check.forcedToRun; got != want {
						t.Errorf("forcedToRun: got %t, want %t", got, want)
					}
					if got, want := defaultBehavior, check.defaultBehavior; got != want {
						t.Errorf("defaultBehavior: got %t, want %t", got, want)
					}
				})
			}
		})
	}
}
