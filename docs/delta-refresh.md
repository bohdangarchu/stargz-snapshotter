# Delta refresh

Refresh a running image to a newer version of itself by fetching only the chunks that actually changed, instead of re-fetching every layer end-to-end.

## Surface

`ctr-remote refresh <ref>` resolves the ref, finds the locally mounted version of the same image, and runs a chunk-level delta refresh on each layer whose descriptor differs. Layers with identical digests are skipped.

`ctr-remote refresh <old-ref> <new-ref>` is the explicit form.

`ctr-remote refresh-layer <old-digest> <new-digest>` (older, per-layer) still exists.

`--with-background-fetch` re-pulls the changed/added chunks of every refreshed layer in the background after the swap.

## Per-layer flow

1. **Diff TOCs by `ChunkDigest`.** estargz writes a `ChunkDigest` per chunk for every blob this codebase produces (default 4 MiB chunks). The set diff over `(parent_id, name, chunk_digest)` gives `unchanged`, `changed`, `added`, `removed`.
2. **Scope FUSE invalidation.** Instead of `NotifyContent(0,0)` over every inode, invalidate only the offset/size ranges that changed. Untouched files in the workload stay warm.
3. **Refresh attrs on changed files.** Changed-file ids are surfaced as `DeltaResult.ChangedFiles`. After the reader swap, each affected node's `Attr` is reloaded from the new metadata reader and `NotifyEntry(name)` is sent on the parent so the kernel drops its cached dentry+attrs. Without this, `stat()` returns the v0 `Size` and the kernel clamps reads to the old `i_size`, silently truncating when v1 is larger.
4. **Scope background fetch.** When `--with-background-fetch` is set, fetch only the changed/added chunk ranges, not the whole new blob.

The reader-swap and lock structure are the same as the per-layer command. The on-disk cache format is unchanged.

`node.attr` is an `atomic.Pointer[metadata.Attr]` so the post-swap attr update is lock-free against concurrent FUSE handlers.

## Same layer structure required

Refresh is in-place. The container's rootfs is an overlayfs mount with a fixed list of lower dirs set at mount time; the kernel can't accept a new lower without unmount. So the new manifest must have the same layer count and order as the old. The daemon errors out if structure changed — that case requires a regular pull plus container restart.

## Cache-hit behaviour

The fscache (`BlobCache`) is carried across refresh — the layer holds it on `l.fsCache` at resolve time and reuses it when building the new reader, instead of allocating a fresh tmp dir per swap. Stale entries are evicted explicitly via `DeltaResult.StaleCacheKeys`: per chunk that changed, the old-size per-chunk key `GenID(fileID, off, oldSize)`; per affected file, the old-total per-file key `GenID(fileID, 0, oldTotal)`. Per-chunk keys cover the chunked read path; per-file keys cover the passthrough open path (`GetPassthroughFd` caches the whole decompressed file under one key). Both flavors can coexist for the same file, so both are emitted.

The reader's on-disk cache is keyed by `(inodeID, offset, size)`. inodeIDs are assigned in TOC walk order, so whether existing cache files survive depends on the producer's tar ordering.

- **Sorted/deterministic tar (docker, buildkit, `ctr-remote optimize`).** Content-only changes leave walk order intact, inodeIDs are stable, unchanged chunks serve from disk.
- **Walk order shifted (file added/removed earlier in the tree).** inodeIDs after the shift point change. Unchanged chunks are still on disk under their old keys but unreachable from the new reader, so they re-fetch. No correctness issue; cost is wasted bandwidth and orphan files in `fscache/` until unmount.

For the sorted case to actually hold, the walk itself has to be deterministic. `assignIDs` in `metadata/memory/reader.go` originally recursed via `TOCEntry.ForeachChild`, which ranges over a Go map — siblings visited in random order, identical TOCs produced different id maps, the cache missed even when nothing changed. The fix collects children, sorts by name, then recurses; ids are now a pure function of the tree structure.

## Metadata-only drift is not detected

The TOC diff compares chunk digests (file content) and entry structure (parent id + name at the same metadata id). It does **not** compare per-entry mode, uid/gid, xattrs, symlink target, or file type *when the file's bytes are unchanged*. If a rebuild changes only those and leaves the bytes identical, FUSE keeps serving the old `stat` / `readlink` / `getxattr` results until the layer is unmounted.

Files whose **content** changed do get their full `Attr` reloaded (size, mtime, mode, xattrs, link target) — that path is driven by `ChangedFiles`, not by hashing attrs. So the only undetected case is "metadata changed but bytes did not."

The cost avoided is always-fallback semantics: hashing every attr and tripping whole-layer fallback on any drift collapses to "always fall back" when the producer doesn't pin mtimes.

If needed later, the extension is additive: include an attr fingerprint in `entrySignature`, surface mismatches as a new `MetadataOnlyChanges []uint32` on `DeltaResult`, run the existing `refreshAttrs` over them.

## Drift from containerd's image record

Refresh updates only the daemon's in-memory layer state (`l.desc` is overwritten). Containerd's image manifest is not touched, so after a refresh `ctr images ls` still shows the original digests. Same drift as the per-layer command. Reconciling means writing back to containerd's image store — content-store handling, image-service API, error recovery if write-back fails mid-flight. Out of scope.

## Out of scope

- Cross-blob/global chunk store keyed by digest.
- Refcounting or mark-and-sweep GC over the cache.
- Adding or removing layers in a running image.
- Changes to registry, manifest, or estargz on-disk format.
- Rename pass to recover cache hits in the shifted-walk case (additive, can be added later if measurement shows it matters).
