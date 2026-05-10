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

package pb

import (
	"context"
	"fmt"
	"time"

	digest "github.com/opencontainers/go-digest"
)

// LayerRefresher is the interface that the filesystem must implement for layer refresh.
type LayerRefresher interface {
	// RefreshImage refreshes a list of (old, new) layer digest pairs as a single
	// best-effort batch. The implementation must populate every returned
	// LayerResult with the corresponding old/new digest strings; the Error field
	// is empty on success and populated otherwise. Pairs that fail individually
	// do not abort the rest of the batch.
	RefreshImage(ctx context.Context, pairs []ImageLayerPair, withBackgroundFetch bool) ([]*LayerResult, error)

	// Watch registers a subscription that polls the registry for the given image
	// ref and triggers an internal RefreshImage when the manifest changes.
	Watch(ctx context.Context, sub WatchSubscription) error

	// Unwatch removes a subscription registered with Watch.
	Unwatch(ref string) error

	// WatchList returns a snapshot of all active subscriptions.
	WatchList() []WatchInfo
}

// ImageLayerPair is the daemon-side input form of a single layer refresh pair.
type ImageLayerPair struct {
	OldDigest    digest.Digest
	NewDigest    digest.Digest
	NewTOCDigest digest.Digest
}

type WatchSubscription struct {
	Ref                 string
	Layers              []SubscriptionLayer
	Labels              map[string]string
	Interval            time.Duration
	WithBackgroundFetch bool
}

type SubscriptionLayer struct {
	Digest    digest.Digest
	TOCDigest digest.Digest
	MediaType string
}

type WatchInfo struct {
	Ref                 string
	LastManifestDigest  digest.Digest
	LastPoll            time.Time
	ConsecutiveFailures int
	Interval            time.Duration
}

type controlServer struct {
	UnimplementedStargzControlServer
	refresher LayerRefresher
}

// NewControlServer creates a new StargzControlServer backed by the given LayerRefresher.
func NewControlServer(refresher LayerRefresher) StargzControlServer {
	return &controlServer{refresher: refresher}
}

func (s *controlServer) RefreshImage(ctx context.Context, req *RefreshImageRequest) (*RefreshImageResponse, error) {
	pairs := make([]ImageLayerPair, 0, len(req.Pairs))
	for i, p := range req.Pairs {
		if p == nil {
			return nil, fmt.Errorf("pair %d is nil", i)
		}
		oldDgst, err := digest.Parse(p.OldDigest)
		if err != nil {
			return nil, fmt.Errorf("pair %d invalid old digest %q: %w", i, p.OldDigest, err)
		}
		newDgst, err := digest.Parse(p.NewDigest)
		if err != nil {
			return nil, fmt.Errorf("pair %d invalid new digest %q: %w", i, p.NewDigest, err)
		}
		var newTOCDgst digest.Digest
		if p.NewTocDigest != "" {
			newTOCDgst, err = digest.Parse(p.NewTocDigest)
			if err != nil {
				return nil, fmt.Errorf("pair %d invalid new toc digest %q: %w", i, p.NewTocDigest, err)
			}
		}
		pairs = append(pairs, ImageLayerPair{
			OldDigest:    oldDgst,
			NewDigest:    newDgst,
			NewTOCDigest: newTOCDgst,
		})
	}
	results, err := s.refresher.RefreshImage(ctx, pairs, req.WithBackgroundFetch)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh image: %w", err)
	}
	return &RefreshImageResponse{Results: results}, nil
}

func (s *controlServer) Watch(ctx context.Context, req *WatchRequest) (*WatchResponse, error) {
	if req.Ref == "" {
		return nil, fmt.Errorf("ref must be non-empty")
	}
	if len(req.Layers) == 0 {
		return nil, fmt.Errorf("at least one layer is required")
	}
	layers := make([]SubscriptionLayer, 0, len(req.Layers))
	for i, l := range req.Layers {
		if l == nil {
			return nil, fmt.Errorf("layer %d is nil", i)
		}
		dgst, err := digest.Parse(l.Digest)
		if err != nil {
			return nil, fmt.Errorf("layer %d invalid digest %q: %w", i, l.Digest, err)
		}
		var tocDgst digest.Digest
		if l.TocDigest != "" {
			tocDgst, err = digest.Parse(l.TocDigest)
			if err != nil {
				return nil, fmt.Errorf("layer %d invalid toc digest %q: %w", i, l.TocDigest, err)
			}
		}
		layers = append(layers, SubscriptionLayer{
			Digest:    dgst,
			TOCDigest: tocDgst,
			MediaType: l.MediaType,
		})
	}
	sub := WatchSubscription{
		Ref:                 req.Ref,
		Layers:              layers,
		Labels:              req.Labels,
		Interval:            time.Duration(req.IntervalSeconds) * time.Second,
		WithBackgroundFetch: req.WithBackgroundFetch,
	}
	if err := s.refresher.Watch(ctx, sub); err != nil {
		return &WatchResponse{Error: err.Error()}, nil
	}
	return &WatchResponse{}, nil
}

func (s *controlServer) Unwatch(_ context.Context, req *UnwatchRequest) (*UnwatchResponse, error) {
	if req.Ref == "" {
		return nil, fmt.Errorf("ref must be non-empty")
	}
	if err := s.refresher.Unwatch(req.Ref); err != nil {
		return &UnwatchResponse{Error: err.Error()}, nil
	}
	return &UnwatchResponse{}, nil
}

func (s *controlServer) WatchList(_ context.Context, _ *WatchListRequest) (*WatchListResponse, error) {
	infos := s.refresher.WatchList()
	entries := make([]*WatchEntry, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, &WatchEntry{
			Ref:                 info.Ref,
			LastManifestDigest:  info.LastManifestDigest.String(),
			LastPollUnix:        info.LastPoll.Unix(),
			ConsecutiveFailures: int32(info.ConsecutiveFailures),
			IntervalSeconds:     int64(info.Interval / time.Second),
		})
	}
	return &WatchListResponse{Entries: entries}, nil
}
