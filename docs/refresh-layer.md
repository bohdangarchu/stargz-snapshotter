# Live Layer Refresh for stargz-snapshotter

## Problem

When a registry updates a blob, containers using that layer via stargz lazy loading continue fetching file data from the old blob. There is no way to switch a running container to use the new blob without stopping it, removing the image, re-pulling, and restarting.

## Solution

A new `ctr-remote refresh-layer` command that hot-swaps the underlying blob of a mounted layer. The container keeps running, and subsequent file reads fetch data from the new blob with correct offsets.

```
ctr-remote refresh-layer <old_layer_digest> <new_blob_digest>
```

Only the new blob's footer (~51 bytes) and TOC (table of contents) are fetched during refresh -- not the entire blob. File data is fetched on demand via range requests, same as normal stargz operation.

## Architecture

```
CLI (ctr-remote)
  |  gRPC over Unix socket
  v
Daemon (containerd-stargz-grpc)
  |  finds layer by digest in mounted layer map
  v
Filesystem (fs/fs.go)
  |  delegates to layer
  v
Layer (fs/layer/layer.go)
  |  resolves new blob, reads TOC, swaps reader, invalidates caches
  v
FUSE nodes (fs/layer/node.go)
  |  serve file reads from swapped reader
  v
Kernel page cache invalidated via go-fuse NotifyContent
```

The gRPC service (`StargzControl`) is registered on the same Unix socket that the snapshotter already uses for the containerd snapshots API. The CLI connects to this socket and sends a `RefreshLayer` RPC with the old and new digests.

The daemon locates the mounted layer by iterating over the filesystem's layer map (keyed by mountpoint) and matching the layer descriptor digest. Once found, it delegates to `layer.RefreshBlob()` which performs the actual swap.

## How the TOC is replaced

eStargz blobs have a well-defined structure: compressed file data, followed by a TOC (JSON index of all files with their offsets and sizes), followed by a footer. The footer is always the last ~51 bytes and contains the TOC offset encoded in a gzip Extra header field.

During refresh, the new TOC is obtained through the same code path used during initial layer resolve:

1. **Footer read**: A range request fetches the last ~51 bytes of the new blob (`GET /v2/<repo>/blobs/<new_digest>` with `Range: bytes=-51`). The footer is parsed to extract the TOC offset.

2. **TOC fetch**: A second range request fetches from the TOC offset to the end of the blob. This contains the compressed TOC JSON.

3. **TOC parse**: The TOC JSON is decompressed and deserialized into a `metadata.Reader`. This is an in-memory structure (when using the default memory metadata store) that maps file IDs to `TOCEntry` structs. Each `TOCEntry` contains the file's compressed offset and size within the blob, its chunk digest, file attributes (mode, uid, gid, timestamps), and directory structure.

4. **ID assignment**: The metadata reader walks the TOC entry tree and assigns integer IDs to each file/directory. These IDs are what FUSE nodes use to look up file attributes, list directory children, and open files.

The old metadata reader is replaced entirely. There is no merging or diffing -- the new TOC completely defines the file layout of the new blob.

## How caching is handled

There are three distinct cache layers that the refresh must address:

### 1. FS cache (decompressed file chunks)

The `reader.Reader` holds a reference to a `cache.BlobCache` -- an LRU cache of decompressed file chunks keyed by `(file_id, chunk_offset, chunk_size)`. When a FUSE read occurs:

- The reader checks the FS cache first (`cache.Get(id)`)
- On cache miss, it issues a range request to the registry blob, decompresses the chunk, stores it in the cache, and returns the data

During refresh, **a brand new empty FS cache is created** via `newCache()`. The new reader is wired to this new cache. This means:

- All subsequent reads go to the new cache, which starts empty
- Every file access after refresh triggers a fresh range request to the new blob
- The old cache (containing chunks from the old blob with old offsets) is not reused, since the same file ID at the same chunk offset now refers to different data in the new blob

The old cache is not explicitly closed (see "Resource lifecycle" below).

### 2. Kernel page cache (FUSE)

The Linux kernel caches pages returned by FUSE reads. The stargz FUSE nodes return `FOPEN_KEEP_CACHE` on file open, which tells the kernel to preserve cached pages across open/close cycles. This means that even after the reader is swapped, the kernel serves stale data from its page cache without ever asking FUSE.

To handle this, after the reader swap, `RefreshBlob` calls `invalidateInodeCache()`. This function recursively walks the FUSE inode tree starting from the root using `Inode.Children()` and calls `Inode.NotifyContent(0, 0)` on each inode. `NotifyContent` is a go-fuse API that tells the kernel to drop its cached page data for that inode. The next read on any file results in a kernel cache miss, which goes through FUSE, which hits the new reader, which fetches from the new blob.

The root inode reference is stored on the `layer` struct when `RootNode()` is called during mount. The recursive walk only visits inodes that the kernel has already cached (children that were never accessed don't appear in the go-fuse inode tree).

### 3. Blob cache (resolver-level)

The `Resolver` maintains a TTL-based LRU cache of `remote.Blob` objects, keyed by `refspec + "/" + digest`. Each blob represents an HTTP connection to a specific registry blob. During refresh, `resolveBlob()` is called with the new digest, which creates a new blob cache entry. The old blob's cache entry is not removed -- it remains in the resolver's blob cache until TTL expiry or LRU eviction.

## The atomic reader swap

The core challenge is swapping the reader while FUSE nodes are actively serving file reads. This is solved by `readerRef`, a `sync.RWMutex`-protected wrapper:

```go
type readerRef struct {
    mu sync.RWMutex
    r  reader.Reader
}
```

Both the `layer` struct and the shared FUSE `fs` struct hold a pointer to the same `*readerRef`. Every FUSE operation (directory listing, file lookup, file open) acquires a read lock via `rr.get()` to obtain the current reader. `RefreshBlob` acquires a write lock via `rr.swap()` to replace it.

This means:
- Multiple concurrent FUSE reads proceed in parallel (RLock doesn't block RLock)
- A swap blocks until all in-flight `get()` calls release their read locks
- After swap returns, all subsequent `get()` calls return the new reader
- An in-flight FUSE read that obtained the old reader before the swap continues using it safely -- the old reader and its resources remain valid

### What each FUSE operation does after the change

The FUSE `fs` struct (shared by all nodes in a layer) holds `rr *readerRef`. Every metadata and file access goes through it:

- **Directory listing** (`readdir`): calls `rr.get().Metadata().ForeachChild(id, ...)` to enumerate children
- **File/dir lookup** (`Lookup`): calls `rr.get().Metadata().GetChild(parentID, name)` to resolve a name to an ID and attributes
- **File open** (`Open`): calls `rr.get().OpenFile(id)` which returns an `io.ReaderAt`. This ReaderAt is bound to the reader (and its FS cache and blob connection) that was current at open time.
- **File read** (`Read`): uses the `io.ReaderAt` from Open. On cache miss, issues a range request to the blob. The blob digest used is whichever was current when the file was opened.

## Resource lifecycle

Old resources (previous blob connection, reader, metadata, FS cache) are **not closed** during refresh. This is because in-flight FUSE reads may still hold references:

```
goroutine A (FUSE read):              RefreshBlob:
  r := rr.get()      // gets old reader
  ra := r.OpenFile()  // ReaderAt uses old cache + old blob
                                        rr.swap(newReader)
  ra.ReadAt(buf, off) // range request to old blob -- still works
```

If the old blob/cache were closed at swap time, goroutine A would fail.

The consequence is that each refresh leaks one set of resources:
- One HTTP blob connection (stays in resolver blob cache until TTL/LRU eviction)
- One FS cache instance (decompressed chunks on disk, never `Close()`d)
- One metadata reader (in-memory TOC structures)

These accumulate with each successive refresh and are only cleaned up when the layer is unmounted (container stop triggers `layer.close()`).

## Constraints

- Both the old and new blobs must be in the **same registry repository**. The refresh reuses the original `hosts` and `refspec` from the initial pull, only changing the blob digest in range requests.
- Verification is **skipped** for the new blob. The caller is responsible for ensuring the new blob is trustworthy.
- The snapshotter must be using **lazy loading** (stargz FUSE mounts). If layers fell back to regular overlayfs (e.g., due to missing TOC annotations or `disable_verification` not set), there are no mounted layers to refresh.
- Both images must share the same directory and file structure. The FUSE inode tree is not rebuilt during refresh -- only the underlying reader (which maps file IDs to blob offsets) is swapped. If the new blob has different files, behavior is undefined.

## Data flow

```
ctr-remote refresh-layer sha256:aaa sha256:bbb
    |
    |  gRPC: RefreshLayer(old="sha256:aaa", new="sha256:bbb")
    v
containerd-stargz-grpc daemon
    |
    |  filesystem.RefreshLayer()
    |  iterates fs.layer map, finds layer where Info().Digest == sha256:aaa
    v
layer.RefreshBlob()
    |
    |  1. resolveBlob() -- establish connection to sha256:bbb in same repo
    |  2. Range request: last 51 bytes of sha256:bbb (footer)
    |  3. Parse footer -> extract TOC offset
    |  4. Range request: TOC offset to end of sha256:bbb
    |  5. Decompress + parse TOC JSON -> new metadata.Reader (in-memory)
    |  6. newCache() -> fresh empty FS cache
    |  7. reader.NewReader(meta, cache, digest) -> new reader
    |  8. SkipVerify() -> unwrap to usable reader
    |  9. rr.swap(newReader) -- atomic, all FUSE nodes see new reader
    | 10. invalidateInodeCache() -- walk FUSE inode tree, NotifyContent on each
    v
Next file read:
    kernel cache miss
      -> FUSE Open: rr.get().OpenFile(id) -> ReaderAt bound to new reader
      -> FUSE Read: ReaderAt.ReadAt(buf, off)
        -> new FS cache miss
        -> range request: GET /v2/<repo>/blobs/sha256:bbb (Range: chunk offset)
        -> decompress, cache in new FS cache, return to kernel
```
