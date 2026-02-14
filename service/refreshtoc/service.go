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

package refreshtoc

import (
	"context"
	"fmt"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/log"
	"github.com/containerd/stargz-snapshotter/snapshot"
)

const (
	// Label keys used to identify the image reference in snapshot labels.
	defaultRefLabel = "containerd.io/snapshot/remote/stargz.reference"
	criRefLabel     = "containerd.io/snapshot/cri.image-ref"

	// remoteLabel identifies remote snapshots.
	remoteLabel = "containerd.io/snapshot/remote"
)

// Service implements RefreshTOCServiceServer.
type Service struct {
	UnimplementedRefreshTOCServiceServer
	fs          snapshot.FileSystem
	snapshotter snapshots.Snapshotter
}

// NewService creates a new RefreshTOC service.
func NewService(fs snapshot.FileSystem, sn snapshots.Snapshotter) *Service {
	return &Service{
		fs:          fs,
		snapshotter: sn,
	}
}

// Refresh handles the RefreshTOC gRPC request. It walks all snapshots,
// finds those matching the given image reference, and calls RefreshTOC
// on each matching layer.
func (s *Service) Refresh(ctx context.Context, req *RefreshTOCRequest) (*RefreshTOCResponse, error) {
	imageRef := req.GetImageRef()
	if imageRef == "" {
		return nil, fmt.Errorf("image_ref is required")
	}

	log.G(ctx).Infof("refreshing TOC for image %q", imageRef)

	type snapshotInfo struct {
		mountpoint string
		labels     map[string]string
	}

	var targets []snapshotInfo

	// Walk all snapshots to find ones matching the image reference.
	if err := s.snapshotter.Walk(ctx, func(ctx context.Context, info snapshots.Info) error {
		if _, ok := info.Labels[remoteLabel]; !ok {
			return nil // skip non-remote snapshots
		}

		ref := info.Labels[defaultRefLabel]
		if ref == "" {
			ref = info.Labels[criRefLabel]
		}
		if ref != imageRef {
			return nil
		}

		// Get the snapshot's ID to compute the mountpoint.
		// The mountpoint follows the pattern: <root>/snapshots/<id>/fs
		// We can get the mountpoint by calling Mounts on the snapshot.
		mounts, err := s.snapshotter.Mounts(ctx, info.Name)
		if err != nil {
			log.G(ctx).WithError(err).Warnf("failed to get mounts for snapshot %q", info.Name)
			return nil
		}

		// The source of the first mount is the mountpoint directory.
		if len(mounts) > 0 {
			targets = append(targets, snapshotInfo{
				mountpoint: mounts[0].Source,
				labels:     info.Labels,
			})
		}

		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to walk snapshots: %w", err)
	}

	if len(targets) == 0 {
		return &RefreshTOCResponse{
			LayersRefreshed: 0,
			Message:         fmt.Sprintf("no remote layers found for image %q", imageRef),
		}, nil
	}

	var refreshed int32
	var lastErr error
	for _, t := range targets {
		log.G(ctx).Infof("refreshing TOC for mountpoint %q", t.mountpoint)
		if err := s.fs.RefreshTOC(ctx, t.mountpoint, t.labels); err != nil {
			log.G(ctx).WithError(err).Errorf("failed to refresh TOC for mountpoint %q", t.mountpoint)
			lastErr = err
			continue
		}
		refreshed++
	}

	resp := &RefreshTOCResponse{
		LayersRefreshed: refreshed,
		Message:         fmt.Sprintf("refreshed %d/%d layers for image %q", refreshed, len(targets), imageRef),
	}

	if lastErr != nil && refreshed == 0 {
		return nil, fmt.Errorf("failed to refresh all layers for image %q: %w", imageRef, lastErr)
	}

	return resp, nil
}
