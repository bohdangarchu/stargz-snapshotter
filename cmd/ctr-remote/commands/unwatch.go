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
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var UnwatchCommand = &cli.Command{
	Name:      "unwatch",
	Usage:     "remove a watch subscription previously registered with `watch`",
	ArgsUsage: "<ref>",
	Action: func(clicontext *cli.Context) error {
		if clicontext.NArg() != 1 {
			return errors.New("expected exactly one <ref>")
		}
		ref := clicontext.Args().Get(0)

		conn, err := grpc.NewClient(
			"unix://"+snapshotterAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("connect to snapshotter at %s: %w", snapshotterAddress, err)
		}
		defer conn.Close()

		grpcClient := fspb.NewStargzControlClient(conn)
		resp, err := grpcClient.Unwatch(clicontext.Context, &fspb.UnwatchRequest{Ref: ref})
		if err != nil {
			return fmt.Errorf("Unwatch RPC: %w", err)
		}
		if resp.Error != "" {
			return fmt.Errorf("daemon: %s", resp.Error)
		}
		fmt.Printf("Unwatched %s\n", ref)
		return nil
	},
}
