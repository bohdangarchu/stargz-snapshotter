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
}

// ImageLayerPair is the daemon-side input form of a single layer refresh pair.
type ImageLayerPair struct {
	OldDigest    digest.Digest
	NewDigest    digest.Digest
	NewTOCDigest digest.Digest
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
