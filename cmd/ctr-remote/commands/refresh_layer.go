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

	fspb "github.com/containerd/stargz-snapshotter/fs/pb"
	digest "github.com/opencontainers/go-digest"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultSnapshotterAddress = "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock"

// RefreshLayerCommand refreshes a mounted layer to use a new blob digest.
var RefreshLayerCommand = &cli.Command{
	Name:      "refresh-layer",
	Usage:     "refresh a mounted layer to use a new blob digest",
	ArgsUsage: "<old_layer_digest> <new_blob_digest>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "snapshotter-address",
			Usage: "address of the stargz snapshotter gRPC socket",
			Value: defaultSnapshotterAddress,
		},
	},
	Action: func(clicontext *cli.Context) error {
		oldDigestStr := clicontext.Args().Get(0)
		newDigestStr := clicontext.Args().Get(1)
		if oldDigestStr == "" || newDigestStr == "" {
			return errors.New("both <old_layer_digest> and <new_blob_digest> must be specified")
		}

		// Validate digests.
		if _, err := digest.Parse(oldDigestStr); err != nil {
			return fmt.Errorf("invalid old layer digest %q: %w", oldDigestStr, err)
		}
		if _, err := digest.Parse(newDigestStr); err != nil {
			return fmt.Errorf("invalid new blob digest %q: %w", newDigestStr, err)
		}

		addr := clicontext.String("snapshotter-address")
		conn, err := grpc.NewClient(
			"unix://"+addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to snapshotter at %s: %w", addr, err)
		}
		defer conn.Close()

		client := fspb.NewStargzControlClient(conn)
		_, err = client.RefreshLayer(clicontext.Context, &fspb.RefreshLayerRequest{
			OldDigest: oldDigestStr,
			NewDigest: newDigestStr,
		})
		if err != nil {
			return fmt.Errorf("failed to refresh layer: %w", err)
		}

		fmt.Printf("Successfully refreshed layer %s -> %s\n", oldDigestStr, newDigestStr)
		return nil
	},
}
