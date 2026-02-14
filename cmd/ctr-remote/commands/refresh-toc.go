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

	"github.com/containerd/stargz-snapshotter/service/refreshtoc"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RefreshTOCCommand refreshes the external TOC for running layers of an image.
var RefreshTOCCommand = &cli.Command{
	Name:      "refresh-toc",
	Usage:     "refresh the external TOC of running layers for an image",
	ArgsUsage: "<image_ref>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "snapshotter-address",
			Usage: "address of the stargz-snapshotter gRPC socket",
			Value: "/run/containerd-stargz-grpc/containerd-stargz-grpc.sock",
		},
	},
	Action: func(clicontext *cli.Context) error {
		imageRef := clicontext.Args().Get(0)
		if imageRef == "" {
			return errors.New("image reference must be specified")
		}

		addr := clicontext.String("snapshotter-address")
		conn, err := grpc.NewClient(
			"unix://"+addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("failed to connect to snapshotter at %q: %w", addr, err)
		}
		defer conn.Close()

		client := refreshtoc.NewRefreshTOCServiceClient(conn)
		resp, err := client.Refresh(clicontext.Context, &refreshtoc.RefreshTOCRequest{
			ImageRef: imageRef,
		})
		if err != nil {
			return fmt.Errorf("failed to refresh TOC: %w", err)
		}

		fmt.Println(resp.GetMessage())
		return nil
	},
}
