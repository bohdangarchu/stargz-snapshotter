/*
   Copyright The containerd Authors.

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

package fs

import (
	"context"
	"testing"
	"time"

	"github.com/containerd/stargz-snapshotter/estargz"
	pb "github.com/containerd/stargz-snapshotter/fs/pb"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func makeDigest(seed string) digest.Digest {
	return digest.FromString(seed)
}

func TestDiffLayers_NoChange(t *testing.T) {
	d1, d2 := makeDigest("a"), makeDigest("b")
	old := []pb.SubscriptionLayer{
		{Digest: d1, MediaType: "x"},
		{Digest: d2, MediaType: "x"},
	}
	newDescs := []ocispec.Descriptor{
		{Digest: d1, MediaType: "x"},
		{Digest: d2, MediaType: "x"},
	}
	pairs, updated, err := diffLayers(old, newDescs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 0 {
		t.Fatalf("expected no pairs, got %d", len(pairs))
	}
	if len(updated) != 2 {
		t.Fatalf("expected 2 updated layers, got %d", len(updated))
	}
}

func TestDiffLayers_OneChanged(t *testing.T) {
	dOldA, dNewA := makeDigest("oldA"), makeDigest("newA")
	dB := makeDigest("b")
	tocNewA := makeDigest("tocNewA")

	old := []pb.SubscriptionLayer{
		{Digest: dOldA, MediaType: "x"},
		{Digest: dB, MediaType: "x"},
	}
	newDescs := []ocispec.Descriptor{
		{Digest: dNewA, MediaType: "x", Annotations: map[string]string{
			estargz.TOCJSONDigestAnnotation: tocNewA.String(),
		}},
		{Digest: dB, MediaType: "x"},
	}
	pairs, updated, err := diffLayers(old, newDescs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
	pair := pairs[0]
	if pair.OldDigest != dOldA || pair.NewDigest != dNewA {
		t.Errorf("wrong pair digests: %+v", pair)
	}
	if pair.NewTOCDigest != tocNewA {
		t.Errorf("missing toc digest: %+v", pair)
	}
	if updated[0].Digest != dNewA || updated[0].TOCDigest != tocNewA {
		t.Errorf("updated[0] wrong: %+v", updated[0])
	}
	if updated[1].Digest != dB {
		t.Errorf("updated[1] wrong: %+v", updated[1])
	}
}

func TestDiffLayers_StructureMismatch(t *testing.T) {
	old := []pb.SubscriptionLayer{{Digest: makeDigest("a")}}
	newDescs := []ocispec.Descriptor{{Digest: makeDigest("a")}, {Digest: makeDigest("b")}}
	if _, _, err := diffLayers(old, newDescs); err == nil {
		t.Fatal("expected error on layer count mismatch")
	}
}

func TestDiffLayers_MediaTypeMismatch(t *testing.T) {
	d := makeDigest("a")
	old := []pb.SubscriptionLayer{{Digest: d, MediaType: "x"}}
	newDescs := []ocispec.Descriptor{{Digest: d, MediaType: "y"}}
	if _, _, err := diffLayers(old, newDescs); err == nil {
		t.Fatal("expected error on media type mismatch")
	}
}

// TestWatchUnwatch verifies that Watch registers an entry and Unwatch removes
// it. The poll loop is started but its first tick is far in the future, so
// nothing else fires during this test.
func TestWatchUnwatch(t *testing.T) {
	fs := &filesystem{subs: make(map[string]*subscription)}

	sub := pb.WatchSubscription{
		Ref:      "registry.example/img:tag",
		Layers:   []pb.SubscriptionLayer{{Digest: makeDigest("a")}},
		Interval: time.Hour,
	}
	if err := fs.Watch(context.Background(), sub); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if got := len(fs.subs); got != 1 {
		t.Fatalf("expected 1 subscription, got %d", got)
	}

	if err := fs.Unwatch(sub.Ref); err != nil {
		t.Fatalf("Unwatch: %v", err)
	}
	if got := len(fs.subs); got != 0 {
		t.Fatalf("expected 0 subscriptions after Unwatch, got %d", got)
	}

	// Idempotent.
	if err := fs.Unwatch(sub.Ref); err != nil {
		t.Fatalf("Unwatch (second call) should be idempotent, got %v", err)
	}
}

func TestWatchReplacesExisting(t *testing.T) {
	fs := &filesystem{subs: make(map[string]*subscription)}
	sub := pb.WatchSubscription{
		Ref:      "registry.example/img:tag",
		Layers:   []pb.SubscriptionLayer{{Digest: makeDigest("a")}},
		Interval: time.Hour,
	}
	if err := fs.Watch(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	first := fs.subs[sub.Ref]

	if err := fs.Watch(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	second := fs.subs[sub.Ref]
	if first == second {
		t.Fatal("expected second Watch to replace first subscription instance")
	}
	if len(fs.subs) != 1 {
		t.Fatalf("expected exactly 1 entry, got %d", len(fs.subs))
	}
	_ = fs.Unwatch(sub.Ref)
}

func TestWatchListSnapshot(t *testing.T) {
	fs := &filesystem{subs: make(map[string]*subscription)}
	for _, ref := range []string{"a:1", "b:1"} {
		if err := fs.Watch(context.Background(), pb.WatchSubscription{
			Ref:      ref,
			Layers:   []pb.SubscriptionLayer{{Digest: makeDigest(ref)}},
			Interval: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
	}
	infos := fs.WatchList()
	if len(infos) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(infos))
	}
	for _, ref := range []string{"a:1", "b:1"} {
		_ = fs.Unwatch(ref)
	}
}

// TestUnmountDropsSubscription verifies that releasing a layer cancels any
// subscription whose layer set referenced it.
func TestUnmountDropsSubscription(t *testing.T) {
	fs := &filesystem{subs: make(map[string]*subscription)}
	layerDigest := makeDigest("a")
	if err := fs.Watch(context.Background(), pb.WatchSubscription{
		Ref:      "img:tag",
		Layers:   []pb.SubscriptionLayer{{Digest: layerDigest}},
		Interval: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	fs.dropSubscriptionsForLayer(layerDigest)
	if got := len(fs.subs); got != 0 {
		t.Fatalf("expected subscription dropped, %d remain", got)
	}
}

func TestWatchValidation(t *testing.T) {
	fs := &filesystem{subs: make(map[string]*subscription)}
	if err := fs.Watch(context.Background(), pb.WatchSubscription{}); err == nil {
		t.Fatal("expected error on empty ref")
	}
	if err := fs.Watch(context.Background(), pb.WatchSubscription{Ref: "x"}); err == nil {
		t.Fatal("expected error on empty layers")
	}
	if err := fs.Unwatch(""); err == nil {
		t.Fatal("expected error on empty ref")
	}
}
