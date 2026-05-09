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
	"sort"
	"testing"
)

// snap builds a tocSnapshot from a list of file entries. Each file has an id, a
// parent id, a name, and a list of chunks (offset, size, digest). All entries
// are recorded in `entries`; regular files (chunks != nil) also appear in
// `chunks`.
type fileSpec struct {
	id     uint32
	parent uint32
	name   string
	chunks []chunkSpec
}

type chunkSpec struct {
	off    int64
	size   int64
	digest string
}

func snap(specs ...fileSpec) *tocSnapshot {
	s := &tocSnapshot{
		chunks:  map[uint32]map[int64]deltaChunkInfo{},
		entries: map[uint32]entrySignature{},
	}
	for _, fs := range specs {
		s.entries[fs.id] = entrySignature{
			parentID: fs.parent,
			name:     fs.name,
		}
		if fs.chunks == nil {
			continue
		}
		chunks := map[int64]deltaChunkInfo{}
		for _, chunk := range fs.chunks {
			chunks[chunk.off] = deltaChunkInfo{Size: chunk.size, Digest: chunk.digest}
		}
		s.chunks[fs.id] = chunks
	}
	return s
}

// chunkRefSet sorts chunks for stable comparison.
func sortChunks(refs []ChunkRef) []ChunkRef {
	out := append([]ChunkRef{}, refs...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].FileID != out[j].FileID {
			return out[i].FileID < out[j].FileID
		}
		return out[i].Offset < out[j].Offset
	})
	return out
}

func TestDiffSnapshots(t *testing.T) {
	// Three "regular files": id 2 has one chunk, id 3 has two chunks, id 4 is a
	// non-regular entry (no chunks). Root is id 1.
	base := []fileSpec{
		{id: 1, parent: 0, name: ""},
		{id: 2, parent: 1, name: "a", chunks: []chunkSpec{{0, 100, "da"}}},
		{id: 3, parent: 1, name: "b", chunks: []chunkSpec{{0, 100, "db0"}, {100, 100, "db1"}}},
		{id: 4, parent: 1, name: "link"},
	}

	tests := []struct {
		name         string
		oldSpecs     []fileSpec
		newSpecs     []fileSpec
		wantFallback bool
		wantChanged  []ChunkRef
		wantAdded    []ChunkRef
	}{
		{
			name:     "identical",
			oldSpecs: base,
			newSpecs: base,
		},
		{
			name:     "single chunk digest changed",
			oldSpecs: base,
			newSpecs: []fileSpec{
				{id: 1, parent: 0, name: ""},
				{id: 2, parent: 1, name: "a", chunks: []chunkSpec{{0, 100, "da"}}},
				{id: 3, parent: 1, name: "b", chunks: []chunkSpec{{0, 100, "db0"}, {100, 100, "db1-NEW"}}},
				{id: 4, parent: 1, name: "link"},
			},
			wantChanged: []ChunkRef{{FileID: 3, Offset: 100, Size: 100}},
		},
		{
			name:     "chunk added (file grew)",
			oldSpecs: base,
			newSpecs: []fileSpec{
				{id: 1, parent: 0, name: ""},
				{id: 2, parent: 1, name: "a", chunks: []chunkSpec{{0, 100, "da"}, {100, 50, "da-extra"}}},
				{id: 3, parent: 1, name: "b", chunks: []chunkSpec{{0, 100, "db0"}, {100, 100, "db1"}}},
				{id: 4, parent: 1, name: "link"},
			},
			wantAdded: []ChunkRef{{FileID: 2, Offset: 100, Size: 50}},
		},
		{
			name:         "file added → fallback",
			oldSpecs:     base,
			newSpecs:     append(append([]fileSpec{}, base...), fileSpec{id: 5, parent: 1, name: "c", chunks: []chunkSpec{{0, 10, "dc"}}}),
			wantFallback: true,
		},
		{
			name:         "file removed → fallback",
			oldSpecs:     base,
			newSpecs:     base[:3],
			wantFallback: true,
		},
		{
			name:     "rename at same id → fallback",
			oldSpecs: base,
			newSpecs: []fileSpec{
				{id: 1, parent: 0, name: ""},
				{id: 2, parent: 1, name: "a-renamed", chunks: []chunkSpec{{0, 100, "da"}}},
				{id: 3, parent: 1, name: "b", chunks: []chunkSpec{{0, 100, "db0"}, {100, 100, "db1"}}},
				{id: 4, parent: 1, name: "link"},
			},
			wantFallback: true,
		},
		{
			name:     "missing chunk digest in new → fallback",
			oldSpecs: base,
			newSpecs: []fileSpec{
				{id: 1, parent: 0, name: ""},
				{id: 2, parent: 1, name: "a", chunks: []chunkSpec{{0, 100, ""}}},
				{id: 3, parent: 1, name: "b", chunks: []chunkSpec{{0, 100, "db0"}, {100, 100, "db1"}}},
				{id: 4, parent: 1, name: "link"},
			},
			wantFallback: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := diffSnapshots(snap(tc.oldSpecs...), snap(tc.newSpecs...))
			if got.Fallback != tc.wantFallback {
				t.Fatalf("fallback: got %v (reason=%q), want %v", got.Fallback, got.FallbackReason, tc.wantFallback)
			}
			if tc.wantFallback {
				return
			}
			gotChanged := sortChunks(got.ChangedChunks)
			wantChanged := sortChunks(tc.wantChanged)
			if !equalChunks(gotChanged, wantChanged) {
				t.Errorf("changed: got %+v, want %+v", gotChanged, wantChanged)
			}
			gotAdded := sortChunks(got.AddedChunks)
			wantAdded := sortChunks(tc.wantAdded)
			if !equalChunks(gotAdded, wantAdded) {
				t.Errorf("added: got %+v, want %+v", gotAdded, wantAdded)
			}
		})
	}
}

func equalChunks(a, b []ChunkRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
