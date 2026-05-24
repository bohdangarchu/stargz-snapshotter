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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/containerd/stargz-snapshotter/estargz"
	"github.com/containerd/stargz-snapshotter/estargz/zstdchunked"
	"github.com/urfave/cli/v2"
)

// FetchTOCCommand fetches the TOC JSON of a stargz layer directly from a
// registry over plain HTTP, without going through containerd's content store
// or resolver. Intended for debugging.
var FetchTOCCommand = &cli.Command{
	Name:      "fetch-toc",
	Usage:     "fetch TOC JSON of a stargz layer directly from a registry (no auth, debug)",
	ArgsUsage: "<registry>/<repo>@<layer-digest>  |  <registry>/<repo> <layer-digest>",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "zstdchunked",
			Usage: "parse layer as zstd:chunked",
		},
		&cli.BoolFlag{
			Name:  "https",
			Usage: "use https instead of plain http",
		},
	},
	Action: func(clicontext *cli.Context) error {
		repo, layerDigest, err := parseFetchTOCArgs(clicontext.Args().Slice())
		if err != nil {
			return err
		}

		scheme := "http"
		if clicontext.Bool("https") {
			scheme = "https"
		}

		host, path, ok := strings.Cut(repo, "/")
		if !ok {
			return fmt.Errorf("expected <registry>/<repo>, got %q", repo)
		}
		url := fmt.Sprintf("%s://%s/v2/%s/blobs/%s", scheme, host, path, layerDigest)

		blob, err := newHTTPReaderAt(url)
		if err != nil {
			return fmt.Errorf("open blob: %w", err)
		}

		footerSize := int64(estargz.FooterSize)
		var decompressor estargz.Decompressor = new(estargz.GzipDecompressor)
		if clicontext.Bool("zstdchunked") {
			footerSize = int64(zstdchunked.FooterSize)
			decompressor = new(zstdchunked.Decompressor)
		}

		footer := make([]byte, footerSize)
		if _, err := blob.ReadAt(footer, blob.size-footerSize); err != nil {
			return fmt.Errorf("read footer: %w", err)
		}

		_, tocOff, tocSize, err := decompressor.ParseFooter(footer)
		if err != nil {
			return fmt.Errorf("parse footer: %w", err)
		}
		if tocSize <= 0 {
			tocSize = blob.size - tocOff - footerSize
		}

		toc, _, err := decompressor.ParseTOC(io.NewSectionReader(blob, tocOff, tocSize))
		if err != nil {
			return fmt.Errorf("parse TOC: %w", err)
		}

		out, err := json.MarshalIndent(toc, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal TOC: %w", err)
		}
		fmt.Println(string(out))
		return nil
	},
}

func parseFetchTOCArgs(args []string) (repo, digest string, err error) {
	switch len(args) {
	case 1:
		repo, digest, ok := strings.Cut(args[0], "@")
		if !ok {
			return "", "", errors.New("single-arg form must be <registry>/<repo>@<layer-digest>")
		}
		return repo, digest, nil
	case 2:
		return args[0], args[1], nil
	default:
		return "", "", errors.New("expected 1 or 2 arguments")
	}
}

type httpReaderAt struct {
	url    string
	size   int64
	client *http.Client
}

func newHTTPReaderAt(url string) (*httpReaderAt, error) {
	resp, err := http.Head(url)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HEAD %s: %s", url, resp.Status)
	}
	if resp.ContentLength <= 0 {
		return nil, fmt.Errorf("HEAD %s: missing or zero Content-Length", url)
	}
	return &httpReaderAt{url: url, size: resp.ContentLength, client: http.DefaultClient}, nil
}

func (r *httpReaderAt) Size() int64 { return r.size }

func (r *httpReaderAt) ReadAt(buf []byte, off int64) (int, error) {
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(buf))-1))
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET %s [%s]: %s", r.url, req.Header.Get("Range"), resp.Status)
	}
	return io.ReadFull(resp.Body, buf)
}
