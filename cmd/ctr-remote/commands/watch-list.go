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
	"fmt"
	"text/tabwriter"
	"time"

	fspb "github.com/containerd/stargz-snapshotter/fs/pb"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var WatchListCommand = &cli.Command{
	Name:  "watch-list",
	Usage: "list active watch subscriptions on the daemon",
	Action: func(clicontext *cli.Context) error {
		conn, err := grpc.NewClient(
			"unix://"+snapshotterAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("connect to snapshotter at %s: %w", snapshotterAddress, err)
		}
		defer conn.Close()

		grpcClient := fspb.NewStargzControlClient(conn)
		resp, err := grpcClient.WatchList(clicontext.Context, &fspb.WatchListRequest{})
		if err != nil {
			return fmt.Errorf("WatchList RPC: %w", err)
		}
		if len(resp.Entries) == 0 {
			fmt.Println("No active watch subscriptions")
			return nil
		}
		writer := tabwriter.NewWriter(clicontext.App.Writer, 0, 4, 2, ' ', 0)
		fmt.Fprintln(writer, "REF\tLAST_MANIFEST\tLAST_POLL\tINTERVAL\tFAILURES")
		for _, entry := range resp.Entries {
			lastPoll := "never"
			if entry.LastPollUnix > 0 {
				lastPoll = time.Unix(entry.LastPollUnix, 0).Format(time.RFC3339)
			}
			fmt.Fprintf(writer, "%s\t%s\t%s\t%ds\t%d\n",
				entry.Ref, shortDigest(entry.LastManifestDigest), lastPoll,
				entry.IntervalSeconds, entry.ConsecutiveFailures)
		}
		return writer.Flush()
	},
}
