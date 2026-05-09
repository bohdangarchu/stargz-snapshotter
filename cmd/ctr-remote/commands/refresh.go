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

package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/cmd/ctr/commands"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/remotes"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	fspb "github.com/containerd/stargz-snapshotter/fs/pb"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const snapshotterAddress = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"

// RefreshCommand is a subcommand to refresh a running image to a new version of
// itself by performing a chunk-level delta refresh per layer.
//
// Two forms:
//
//	ctr-remote refresh <ref>
//	    Resolves <ref> against the registry and refreshes the locally
//	    mounted image (which must already exist under <ref>) to a new
//	    manifest.
//
//	ctr-remote refresh <old-ref> <new-ref>
//	    <old-ref> must already exist in the local containerd image store.
//	    <new-ref> is resolved locally first and falls back to the registry
//	    if not present.
//
// In both forms the client enforces that the two manifests have the same
// layer structure (count + media-type/order). Layers whose digests are
// identical between the two versions are skipped entirely.
var RefreshCommand = &cli.Command{
	Name:      "refresh",
	Usage:     "refresh a running image to a new version via chunk-level delta refresh",
	ArgsUsage: "<ref> | <old-ref> <new-ref>",
	Flags: append([]cli.Flag{
		&cli.BoolFlag{
			Name:  "with-background-fetch",
			Usage: "after refresh, fetch the changed/added chunks of every refreshed layer in the background",
		},
	}, commands.RegistryFlags...),
	Action: func(clicontext *cli.Context) error {
		nargs := clicontext.NArg()
		if nargs != 1 && nargs != 2 {
			return errors.New("expected <ref> or <old-ref> <new-ref>")
		}

		client, ctx, cancel, err := commands.NewClient(clicontext)
		if err != nil {
			return err
		}
		defer cancel()

		platformMatcher := platforms.Default()

		resolver, err := commands.GetResolver(ctx, clicontext)
		if err != nil {
			return fmt.Errorf("build registry resolver: %w", err)
		}

		var (
			oldRef, newRef   string
			oldDesc, newDesc ocispec.Descriptor
			oldManifest      *ocispec.Manifest
			newManifest      *ocispec.Manifest
		)
		switch nargs {
		case 1:
			oldRef = clicontext.Args().Get(0)
			newRef = oldRef
			oldDesc, oldManifest, err = manifestFromLocal(ctx, client, oldRef, platformMatcher)
			if err != nil {
				return fmt.Errorf("resolve local manifest %q: %w", oldRef, err)
			}
			newDesc, newManifest, err = manifestFromRegistry(ctx, resolver, oldRef, platformMatcher)
			if err != nil {
				return fmt.Errorf("resolve registry manifest %q: %w", oldRef, err)
			}
		case 2:
			oldRef = clicontext.Args().Get(0)
			newRef = clicontext.Args().Get(1)
			oldDesc, oldManifest, err = manifestFromLocal(ctx, client, oldRef, platformMatcher)
			if err != nil {
				return fmt.Errorf("resolve local manifest %q: %w", oldRef, err)
			}
			newDesc, newManifest, err = manifestFromLocal(ctx, client, newRef, platformMatcher)
			if errdefs.IsNotFound(err) {
				newDesc, newManifest, err = manifestFromRegistry(ctx, resolver, newRef, platformMatcher)
				if err != nil {
					return fmt.Errorf("resolve %q (not in local image store, registry fallback failed): %w", newRef, err)
				}
			} else if err != nil {
				return fmt.Errorf("resolve local manifest %q: %w", newRef, err)
			}
		}

		if oldDesc.Digest == newDesc.Digest {
			fmt.Printf("Manifests are identical (%s); nothing to refresh\n", oldDesc.Digest)
			return nil
		}

		if err := checkLayerStructure(oldManifest, newManifest); err != nil {
			return err
		}

		pairs := make([]*fspb.LayerPair, 0, len(oldManifest.Layers))
		var skipped int
		for i := range oldManifest.Layers {
			oldDigest := oldManifest.Layers[i].Digest
			newDigest := newManifest.Layers[i].Digest
			if oldDigest == newDigest {
				skipped++
				continue
			}
			pairs = append(pairs, &fspb.LayerPair{
				OldDigest: oldDigest.String(),
				NewDigest: newDigest.String(),
			})
		}
		fmt.Printf("Refreshing %s -> %s: %d layer(s) to refresh, %d unchanged\n", oldRef, newRef, len(pairs), skipped)
		if len(pairs) == 0 {
			return nil
		}

		conn, err := grpc.NewClient(
			"unix://"+snapshotterAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("connect to snapshotter at %s: %w", snapshotterAddress, err)
		}
		defer conn.Close()

		grpcClient := fspb.NewStargzControlClient(conn)
		start := time.Now()
		resp, err := grpcClient.RefreshImage(clicontext.Context, &fspb.RefreshImageRequest{
			Pairs:               pairs,
			WithBackgroundFetch: clicontext.Bool("with-background-fetch"),
		})
		elapsed := time.Since(start)
		if err != nil {
			return fmt.Errorf("RefreshImage RPC: %w", err)
		}

		var failed int
		for _, result := range resp.Results {
			status := "ok"
			if result.Error != "" {
				status = "ERR: " + result.Error
				failed++
			} else if result.Fallback {
				status = fmt.Sprintf("ok (fallback, changed=%d added=%d)", result.ChangedChunks, result.AddedChunks)
			} else {
				status = fmt.Sprintf("ok (changed=%d added=%d)", result.ChangedChunks, result.AddedChunks)
			}
			fmt.Printf("  %s -> %s: %s\n", shortDigest(result.OldDigest), shortDigest(result.NewDigest), status)
		}
		fmt.Printf("Done in %s (%d ok, %d failed)\n", elapsed, len(resp.Results)-failed, failed)
		if failed > 0 {
			return fmt.Errorf("%d layer(s) failed to refresh", failed)
		}
		return nil
	},
}

// manifestFromLocal looks up an image in the local containerd image store and
// returns the platform-specific manifest descriptor and the parsed manifest.
func manifestFromLocal(ctx context.Context, client *containerd.Client, ref string, platform platforms.MatchComparer) (ocispec.Descriptor, *ocispec.Manifest, error) {
	img, err := client.GetImage(ctx, ref)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("get image: %w", err)
	}
	store := client.ContentStore()
	return resolveManifest(func(d ocispec.Descriptor) ([]byte, error) {
		return content.ReadBlob(ctx, store, d)
	}, img.Target(), platform)
}

// manifestFromRegistry resolves a ref against the registry and returns the
// platform-specific manifest descriptor and the parsed manifest. No bytes are
// written to local storage.
func manifestFromRegistry(ctx context.Context, resolver remotes.Resolver, ref string, platform platforms.MatchComparer) (ocispec.Descriptor, *ocispec.Manifest, error) {
	name, desc, err := resolver.Resolve(ctx, ref)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("resolve %q: %w", ref, err)
	}
	fetcher, err := resolver.Fetcher(ctx, name)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("fetcher for %q: %w", name, err)
	}
	return resolveManifest(func(d ocispec.Descriptor) ([]byte, error) {
		return fetchBytes(ctx, fetcher, d)
	}, desc, platform)
}

// resolveManifest walks a descriptor (which may be an Index) down to the
// platform-matching manifest, using the supplied byte-fetch callback. It is
// recursive only across an Index → Manifest hop, not deeper.
func resolveManifest(get func(ocispec.Descriptor) ([]byte, error), desc ocispec.Descriptor, platform platforms.MatchComparer) (ocispec.Descriptor, *ocispec.Manifest, error) {
	body, err := get(desc)
	if err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("read %s: %w", desc.Digest, err)
	}
	if images.IsIndexType(desc.MediaType) {
		var idx ocispec.Index
		if err := json.Unmarshal(body, &idx); err != nil {
			return ocispec.Descriptor{}, nil, fmt.Errorf("unmarshal index %s: %w", desc.Digest, err)
		}
		for _, child := range idx.Manifests {
			if child.Platform == nil || platform.Match(*child.Platform) {
				return resolveManifest(get, child, platform)
			}
		}
		return ocispec.Descriptor{}, nil, fmt.Errorf("no platform match in index %s", desc.Digest)
	}
	if !images.IsManifestType(desc.MediaType) {
		return ocispec.Descriptor{}, nil, fmt.Errorf("unexpected media type %q for %s", desc.MediaType, desc.Digest)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return ocispec.Descriptor{}, nil, fmt.Errorf("unmarshal manifest %s: %w", desc.Digest, err)
	}
	return desc, &manifest, nil
}

func fetchBytes(ctx context.Context, fetcher remotes.Fetcher, desc ocispec.Descriptor) ([]byte, error) {
	reader, err := fetcher.Fetch(ctx, desc)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", desc.Digest, err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func checkLayerStructure(oldManifest, newManifest *ocispec.Manifest) error {
	if len(oldManifest.Layers) != len(newManifest.Layers) {
		return fmt.Errorf("layer count differs: old=%d new=%d (refresh requires same structure; pull and restart instead)", len(oldManifest.Layers), len(newManifest.Layers))
	}
	for i := range oldManifest.Layers {
		if oldManifest.Layers[i].MediaType != newManifest.Layers[i].MediaType {
			return fmt.Errorf("layer %d media type differs: old=%q new=%q", i, oldManifest.Layers[i].MediaType, newManifest.Layers[i].MediaType)
		}
	}
	return nil
}

func shortDigest(d string) string {
	if len(d) > 19 {
		return d[:19]
	}
	return d
}
