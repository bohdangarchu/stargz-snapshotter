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
	"errors"
	"fmt"
	"time"

	"github.com/containerd/containerd/v2/cmd/ctr/commands"
	"github.com/containerd/platforms"
	"github.com/containerd/stargz-snapshotter/estargz"
	fspb "github.com/containerd/stargz-snapshotter/fs/pb"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Source labels recognized by the daemon's source.FromDefaultLabels. Kept in
// sync with the unexported constants in fs/source/source.go.
const (
	stargzReferenceLabel = "containerd.io/snapshot/remote/stargz.reference"
	stargzDigestLabel    = "containerd.io/snapshot/remote/stargz.digest"
)

var WatchCommand = &cli.Command{
	Name:      "watch",
	Usage:     "subscribe to auto-refresh an image when its content changes",
	ArgsUsage: "<ref>",
	Flags: []cli.Flag{
		&cli.DurationFlag{
			Name:  "interval",
			Value: 30 * time.Second,
			Usage: "polling interval",
		},
		&cli.BoolFlag{
			Name:  "with-background-fetch",
			Usage: "after each refresh, fetch the changed chunks of every refreshed layer in the background",
		},
	},
	Action: func(clicontext *cli.Context) error {
		if clicontext.NArg() != 1 {
			return errors.New("expected exactly one <ref>")
		}
		ref := clicontext.Args().Get(0)

		client, ctx, cancel, err := commands.NewClient(clicontext)
		if err != nil {
			return err
		}
		defer cancel()

		_, manifest, err := manifestFromLocal(ctx, client, ref, platforms.Default())
		if err != nil {
			return fmt.Errorf("resolve local manifest %q (image must be pulled first): %w", ref, err)
		}
		if len(manifest.Layers) == 0 {
			return fmt.Errorf("manifest %q has no layers", ref)
		}

		layers := make([]*fspb.WatchLayer, 0, len(manifest.Layers))
		for _, l := range manifest.Layers {
			layers = append(layers, &fspb.WatchLayer{
				Digest:    l.Digest.String(),
				TocDigest: l.Annotations[estargz.TOCJSONDigestAnnotation],
				MediaType: l.MediaType,
			})
		}

		labels := map[string]string{
			stargzReferenceLabel: ref,
			stargzDigestLabel:    manifest.Layers[0].Digest.String(),
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
		resp, err := grpcClient.Watch(clicontext.Context, &fspb.WatchRequest{
			Ref:                 ref,
			Layers:              layers,
			Labels:              labels,
			IntervalSeconds:     int64(clicontext.Duration("interval") / time.Second),
			WithBackgroundFetch: clicontext.Bool("with-background-fetch"),
		})
		if err != nil {
			return fmt.Errorf("Watch RPC: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("daemon refused watch: %s", resp.Error)
		}
		fmt.Printf("Watching %s (interval %s, %d layers)\n", ref, clicontext.Duration("interval"), len(layers))
		return nil
	},
}
