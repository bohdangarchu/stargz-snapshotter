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
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/log"
	"github.com/containerd/platforms"
	"github.com/containerd/stargz-snapshotter/estargz"
	pb "github.com/containerd/stargz-snapshotter/fs/pb"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	defaultWatchInterval     = 30 * time.Second
	maxConsecutiveWatchFails = 10
)

// subscription is the daemon-side state for a single Watch.
type subscription struct {
	ref                 string
	layers              []pb.SubscriptionLayer
	labels              map[string]string
	interval            time.Duration
	withBackgroundFetch bool
	mu                  sync.Mutex
	lastDigest          digest.Digest
	lastManifestDigest  digest.Digest
	lastUpdate          time.Time
	consecFailures      int
	cancel              context.CancelFunc
}

func (fs *filesystem) Watch(ctx context.Context, sub pb.WatchSubscription) error {
	if sub.Ref == "" {
		return fmt.Errorf("ref must be non-empty")
	}
	if len(sub.Layers) == 0 {
		return fmt.Errorf("at least one layer required")
	}
	interval := sub.Interval
	if interval <= 0 {
		interval = defaultWatchInterval
	}

	fs.subsMu.Lock()
	replaced := false
	if existing, ok := fs.subs[sub.Ref]; ok {
		existing.cancel()
		delete(fs.subs, sub.Ref)
		replaced = true
	}
	pollCtx, cancel := context.WithCancel(context.Background())
	entry := &subscription{
		ref:                 sub.Ref,
		layers:              sub.Layers,
		labels:              sub.Labels,
		interval:            interval,
		withBackgroundFetch: sub.WithBackgroundFetch,
		cancel:              cancel,
	}
	fs.subs[sub.Ref] = entry
	fs.subsMu.Unlock()

	logger := log.G(ctx).WithFields(log.Fields{
		"watch_ref":             sub.Ref,
		"layers":                len(sub.Layers),
		"interval":              interval,
		"with_background_fetch": sub.WithBackgroundFetch,
	})
	if replaced {
		logger.Info("watch replaced existing subscription")
	} else {
		logger.Info("watch registered")
	}

	go fs.runWatch(pollCtx, entry)
	return nil
}

func (fs *filesystem) Unwatch(ref string) error {
	if ref == "" {
		return fmt.Errorf("ref must be non-empty")
	}
	fs.subsMu.Lock()
	entry, ok := fs.subs[ref]
	if ok {
		delete(fs.subs, ref)
	}
	fs.subsMu.Unlock()
	if !ok {
		log.L.WithField("watch_ref", ref).Debug("unwatch: no such subscription")
		return nil
	}
	entry.cancel()
	log.L.WithField("watch_ref", ref).Info("watch unregistered")
	return nil
}

func (fs *filesystem) WatchList() []pb.WatchInfo {
	fs.subsMu.Lock()
	defer fs.subsMu.Unlock()
	out := make([]pb.WatchInfo, 0, len(fs.subs))
	for _, entry := range fs.subs {
		entry.mu.Lock()
		out = append(out, pb.WatchInfo{
			Ref:                 entry.ref,
			LastManifestDigest:  entry.lastManifestDigest,
			LastUpdate:          entry.lastUpdate,
			ConsecutiveFailures: entry.consecFailures,
			Interval:            entry.interval,
		})
		entry.mu.Unlock()
	}
	return out
}

// dropSubscription removes the entry from the map and cancels its context.
func (fs *filesystem) dropSubscription(entry *subscription) {
	fs.subsMu.Lock()
	if cur, ok := fs.subs[entry.ref]; ok && cur == entry {
		delete(fs.subs, entry.ref)
	}
	fs.subsMu.Unlock()
	entry.cancel()
}

// dropSubscriptionsForLayer cancels every subscription whose layer set
// references the given layer digest.
func (fs *filesystem) dropSubscriptionsForLayer(layerDigest digest.Digest) {
	fs.subsMu.Lock()
	defer fs.subsMu.Unlock()
	for ref, entry := range fs.subs {
		entry.mu.Lock()
		for _, l := range entry.layers {
			if l.Digest == layerDigest {
				delete(fs.subs, ref)
				entry.cancel()
				break
			}
		}
		entry.mu.Unlock()
	}
}

func (fs *filesystem) runWatch(ctx context.Context, entry *subscription) {
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("watch_ref", entry.ref))
	ticker := time.NewTicker(entry.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		err := fs.pollOnce(ctx, entry)
		entry.mu.Lock()
		if err != nil {
			entry.consecFailures++
			fails := entry.consecFailures
			entry.mu.Unlock()
			log.G(ctx).WithError(err).Warnf("watch poll failed (%d/%d)", fails, maxConsecutiveWatchFails)
			if fails >= maxConsecutiveWatchFails {
				log.G(ctx).Warn("dropping watch subscription after consecutive failures")
				fs.dropSubscription(entry)
				return
			}
			continue
		}
		entry.consecFailures = 0
		entry.mu.Unlock()
	}
}

func (fs *filesystem) pollOnce(ctx context.Context, entry *subscription) error {
	log.G(ctx).Debug("polling registry")

	resolver, err := fs.resolverForLabels(entry.labels)
	if err != nil {
		return fmt.Errorf("build resolver: %w", err)
	}

	desc, fetcher, err := resolveRef(ctx, resolver, entry.ref)
	if err != nil {
		return fmt.Errorf("resolve manifest: %w", err)
	}

	entry.mu.Lock()
	if desc.Digest == entry.lastDigest {
		entry.mu.Unlock()
		log.G(ctx).WithField("digest", desc.Digest).Debug("poll: digest unchanged")
		return nil
	}
	entry.mu.Unlock()

	body, manifestDesc, err := fetchPlatformManifestBody(ctx, fetcher, desc)
	if err != nil {
		return fmt.Errorf("fetch manifest: %w", err)
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return fmt.Errorf("unmarshal manifest %s: %w", manifestDesc.Digest, err)
	}

	entry.mu.Lock()
	defer entry.mu.Unlock()

	// First poll establishes a baseline
	if entry.lastDigest == "" {
		entry.lastDigest = desc.Digest
		entry.lastManifestDigest = manifestDesc.Digest
		log.G(ctx).WithField("digest", desc.Digest).Debug("poll: baseline established")
		return nil
	}

	pairs, newLayers, err := diffLayers(entry.layers, manifest.Layers)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		log.G(ctx).WithFields(log.Fields{
			"old_digest": entry.lastDigest,
			"new_digest": desc.Digest,
		}).Debug("poll: digest changed but layers identical")
		entry.lastDigest = desc.Digest
		entry.lastManifestDigest = manifestDesc.Digest
		entry.layers = newLayers
		entry.lastUpdate = time.Now()
		return nil
	}

	log.G(ctx).WithFields(log.Fields{
		"old_digest":        entry.lastDigest,
		"new_digest":        desc.Digest,
		"layers_to_refresh": len(pairs),
		"layers_unchanged":  len(manifest.Layers) - len(pairs),
	}).Info("manifest changed, dispatching refresh")

	results, err := fs.RefreshImage(ctx, pairs, entry.withBackgroundFetch)
	if err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	failed := 0
	for _, refreshed := range results {
		if refreshed.Error != "" {
			failed++
		}
	}
	if failed > 0 {
		log.G(ctx).Warnf("watch refresh: %d/%d layers failed", failed, len(results))
	}

	entry.lastDigest = desc.Digest
	entry.lastManifestDigest = manifestDesc.Digest
	entry.layers = newLayers
	entry.lastUpdate = time.Now()
	return nil
}

// resolverForLabels builds a registry resolver bound to the registry hosts
// derived from the given source labels.
func (fs *filesystem) resolverForLabels(labels map[string]string) (remotes.Resolver, error) {
	sources, err := fs.getSources(labels)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no sources resolved from labels")
	}
	src := sources[0]
	hostFn := func(_ string) ([]docker.RegistryHost, error) {
		return src.Hosts(src.Name)
	}
	return docker.NewResolver(docker.ResolverOptions{Hosts: hostFn}), nil
}

func resolveRef(ctx context.Context, resolver remotes.Resolver, ref string) (ocispec.Descriptor, remotes.Fetcher, error) {
	name, desc, err := resolver.Resolve(ctx, ref)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("resolve %q: %w", ref, err)
	}
	fetcher, err := resolver.Fetcher(ctx, name)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("fetcher for %q: %w", name, err)
	}
	return desc, fetcher, nil
}

func fetchPlatformManifestBody(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor) ([]byte, ocispec.Descriptor, error) {
	body, err := fetchBlob(ctx, fetcher, desc)
	if err != nil {
		return nil, ocispec.Descriptor{}, err
	}
	if !images.IsIndexType(desc.MediaType) {
		if !images.IsManifestType(desc.MediaType) {
			return nil, ocispec.Descriptor{}, fmt.Errorf("unexpected media type %q for %s", desc.MediaType, desc.Digest)
		}
		return body, desc, nil
	}

	var idx ocispec.Index
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, ocispec.Descriptor{}, fmt.Errorf("unmarshal index %s: %w", desc.Digest, err)
	}
	matcher := platforms.Default()
	for _, child := range idx.Manifests {
		if child.Platform == nil || matcher.Match(*child.Platform) {
			childBody, err := fetchBlob(ctx, fetcher, child)
			if err != nil {
				return nil, ocispec.Descriptor{}, err
			}
			return childBody, child, nil
		}
	}
	return nil, ocispec.Descriptor{}, fmt.Errorf("no platform match in index %s", desc.Digest)
}

func fetchBlob(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor) ([]byte, error) {
	reader, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", desc.Digest, err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// diffLayers compares the stored per-subscription layer list against a new
// manifest's layers.
func diffLayers(old []pb.SubscriptionLayer, new []ocispec.Descriptor) ([]pb.ImageLayerPair, []pb.SubscriptionLayer, error) {
	if len(old) != len(new) {
		return nil, nil, fmt.Errorf("layer count differs: old=%d new=%d", len(old), len(new))
	}
	pairs := make([]pb.ImageLayerPair, 0, len(old))
	updated := make([]pb.SubscriptionLayer, len(new))
	for i := range old {
		if old[i].MediaType != "" && new[i].MediaType != "" && old[i].MediaType != new[i].MediaType {
			return nil, nil, fmt.Errorf("layer %d media type differs: old=%q new=%q", i, old[i].MediaType, new[i].MediaType)
		}
		var newTOC digest.Digest
		if tocStr := new[i].Annotations[estargz.TOCJSONDigestAnnotation]; tocStr != "" {
			parsed, err := digest.Parse(tocStr)
			if err != nil {
				return nil, nil, fmt.Errorf("layer %d invalid new toc digest %q: %w", i, tocStr, err)
			}
			newTOC = parsed
		}
		updated[i] = pb.SubscriptionLayer{
			Digest:    new[i].Digest,
			TOCDigest: newTOC,
			MediaType: new[i].MediaType,
		}
		if old[i].Digest == new[i].Digest {
			continue
		}
		pairs = append(pairs, pb.ImageLayerPair{
			OldDigest:    old[i].Digest,
			NewDigest:    new[i].Digest,
			NewTOCDigest: newTOC,
		})
	}
	return pairs, updated, nil
}
