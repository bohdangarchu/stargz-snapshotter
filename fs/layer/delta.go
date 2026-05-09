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

package layer

import (
	"fmt"
	"os"

	"github.com/containerd/stargz-snapshotter/metadata"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
)

// ChunkRef identifies a single chunk inside a layer by (file id, logical offset, size).
type ChunkRef struct {
	FileID uint32
	Offset int64
	Size   int64
}

// DeltaResult is the outcome of a TOC diff between two readers of the same layer.
//
// Fallback=true means the old and new TOCs cannot be aligned chunk-for-chunk —
// entry count differs, a metadata id appears on only one side, walk order
// shifted, any chunk lacks a ChunkDigest,
// or the walk itself errored — and the caller should treat the
// refresh as a whole-layer change.
type DeltaResult struct {
	Fallback       bool
	FallbackReason string
	ChangedChunks  []ChunkRef
	AddedChunks    []ChunkRef
}

// deltaChunkInfo is a single chunk's metadata, indexed by file id and chunk offset.
type deltaChunkInfo struct {
	Size   int64
	Digest string
}

// entrySignature captures the structural identity of a TOC entry: which directory
// it lives in and what it's called. Used to detect renames/moves at the same id.
type entrySignature struct {
	parentID uint32
	name     string
}

// tocSnapshot is a flattened view of a TOC sufficient for diffing.
type tocSnapshot struct {
	chunks  map[uint32]map[int64]deltaChunkInfo
	entries map[uint32]entrySignature
}

func snapshotTOC(meta metadata.Reader) (*tocSnapshot, error) {
	snap := &tocSnapshot{
		chunks:  make(map[uint32]map[int64]deltaChunkInfo),
		entries: make(map[uint32]entrySignature),
	}
	rootID := meta.RootID()
	snap.entries[rootID] = entrySignature{
		parentID: 0,
		name:     "",
	}
	if err := walkTOC(meta, rootID, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

func walkTOC(meta metadata.Reader, dirID uint32, snap *tocSnapshot) error {
	var walkErr error
	err := meta.ForeachChild(dirID, func(name string, id uint32, mode os.FileMode) bool {
		snap.entries[id] = entrySignature{
			parentID: dirID,
			name:     name,
		}
		if mode.IsDir() {
			if err := walkTOC(meta, id, snap); err != nil {
				walkErr = err
				return false
			}
			return true
		}
		if !mode.IsRegular() {
			return true
		}
		attr, err := meta.GetAttr(id)
		if err != nil {
			walkErr = fmt.Errorf("get attr id=%d: %w", id, err)
			return false
		}
		fr, err := meta.OpenFile(id)
		if err != nil {
			walkErr = fmt.Errorf("open file id=%d: %w", id, err)
			return false
		}
		chunks := make(map[int64]deltaChunkInfo)
		var nr int64
		for nr < attr.Size {
			cOff, cSize, dgst, ok := fr.ChunkEntryForOffset(nr)
			if !ok {
				break
			}
			chunks[cOff] = deltaChunkInfo{Size: cSize, Digest: dgst}
			if cSize <= 0 {
				break
			}
			nr += cSize
		}
		snap.chunks[id] = chunks
		return true
	})
	if err != nil {
		return fmt.Errorf("foreach child of dirID=%d: %w", dirID, err)
	}
	return walkErr
}

// diffTOCs compares two TOC snapshots and returns the chunks that differ.
//
// The fast path requires the two TOCs to be walk-order-stable: every metadata id
// must point to the same (parent, name) pair on both sides. Any structural
// difference (id only on one side, id renamed, id moved) trips Fallback=true,
// and the caller should fall back to whole-layer invalidation. Missing
// ChunkDigest on any chunk also trips fallback.
func diffTOCs(oldMeta, newMeta metadata.Reader) (*DeltaResult, error) {
	oldSnap, err := snapshotTOC(oldMeta)
	if err != nil {
		return nil, fmt.Errorf("snapshot old TOC: %w", err)
	}
	newSnap, err := snapshotTOC(newMeta)
	if err != nil {
		return nil, fmt.Errorf("snapshot new TOC: %w", err)
	}
	return diffSnapshots(oldSnap, newSnap), nil
}

func diffSnapshots(oldSnap, newSnap *tocSnapshot) *DeltaResult {
	if len(oldSnap.entries) != len(newSnap.entries) {
		return &DeltaResult{
			Fallback:       true,
			FallbackReason: fmt.Sprintf("entry count differs: old=%d new=%d", len(oldSnap.entries), len(newSnap.entries)),
		}
	}
	for id, oe := range oldSnap.entries {
		ne, ok := newSnap.entries[id]
		if !ok {
			return &DeltaResult{
				Fallback:       true,
				FallbackReason: fmt.Sprintf("id %d missing in new TOC", id),
			}
		}
		if oe.parentID != ne.parentID || oe.name != ne.name {
			return &DeltaResult{
				Fallback:       true,
				FallbackReason: fmt.Sprintf("id %d moved or renamed", id),
			}
		}
	}

	res := &DeltaResult{}
	for id, newChunks := range newSnap.chunks {
		oldChunks := oldSnap.chunks[id]
		for off, ni := range newChunks {
			if ni.Digest == "" {
				return &DeltaResult{
					Fallback:       true,
					FallbackReason: fmt.Sprintf("missing chunk digest in new TOC at id=%d off=%d", id, off),
				}
			}
			oi, exists := oldChunks[off]
			if !exists {
				res.AddedChunks = append(res.AddedChunks, ChunkRef{FileID: id, Offset: off, Size: ni.Size})
				continue
			}
			if oi.Digest == "" {
				return &DeltaResult{
					Fallback:       true,
					FallbackReason: fmt.Sprintf("missing chunk digest in old TOC at id=%d off=%d", id, off),
				}
			}
			if oi.Digest != ni.Digest || oi.Size != ni.Size {
				res.ChangedChunks = append(res.ChangedChunks, ChunkRef{FileID: id, Offset: off, Size: ni.Size})
			}
		}
	}
	return res
}

// buildInodeIndex walks the live FUSE inode tree and returns a map from metadata
// file id to its inode. Only inodes that have been instantiated by FUSE (i.e.
// accessed at least once) are present; non-instantiated inodes have no kernel
// page cache to invalidate.
func buildInodeIndex(root *fusefs.Inode) map[uint32]*fusefs.Inode {
	if root == nil {
		return nil
	}
	out := make(map[uint32]*fusefs.Inode)
	var walk func(*fusefs.Inode)
	walk = func(in *fusefs.Inode) {
		if in == nil {
			return
		}
		if n, ok := in.Operations().(*node); ok {
			out[n.id] = in
		}
		for _, c := range in.Children() {
			walk(c)
		}
	}
	walk(root)
	return out
}

// invalidateChangedChunks issues NotifyContent(off, size) for every changed chunk
func invalidateChangedChunks(root *fusefs.Inode, changed []ChunkRef) {
	if len(changed) == 0 || root == nil {
		return
	}
	idx := buildInodeIndex(root)
	for _, c := range changed {
		if in, ok := idx[c.FileID]; ok {
			in.NotifyContent(c.Offset, c.Size)
		}
	}
}
