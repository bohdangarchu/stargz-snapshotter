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
	RefreshLayer(ctx context.Context, oldDigest, newDigest digest.Digest) error
}

type controlServer struct {
	UnimplementedStargzControlServer
	refresher LayerRefresher
}

// NewControlServer creates a new StargzControlServer backed by the given LayerRefresher.
func NewControlServer(refresher LayerRefresher) StargzControlServer {
	return &controlServer{refresher: refresher}
}

func (s *controlServer) RefreshLayer(ctx context.Context, req *RefreshLayerRequest) (*RefreshLayerResponse, error) {
	oldDigest, err := digest.Parse(req.OldDigest)
	if err != nil {
		return nil, fmt.Errorf("invalid old digest %q: %w", req.OldDigest, err)
	}
	newDigest, err := digest.Parse(req.NewDigest)
	if err != nil {
		return nil, fmt.Errorf("invalid new digest %q: %w", req.NewDigest, err)
	}
	if err := s.refresher.RefreshLayer(ctx, oldDigest, newDigest); err != nil {
		return nil, fmt.Errorf("failed to refresh layer: %w", err)
	}
	return &RefreshLayerResponse{}, nil
}
