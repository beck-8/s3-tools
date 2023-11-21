package main

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v2"
)

var download = &cli.Command{
	Name:  "download",
	Usage: "from http[s] download to s3",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "dst_endpoint",
			EnvVars:  []string{"dst_endpoint"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst_ak",
			EnvVars:  []string{"dst_ak"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst_sk",
			EnvVars:  []string{"dst_sk"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst_bucket",
			EnvVars:  []string{"dst_bucket"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "dst_region",
			EnvVars:  []string{"dst_region"},
			Required: false,
			Hidden:   true,
		},
		&cli.StringFlag{
			Name:    "dst_prefix",
			EnvVars: []string{"dst_prefix"},
		},
		&cli.StringFlag{
			Name:    "filelist",
			EnvVars: []string{"filelist"},
			Usage:   "specify the list to be downloaded, one url per line",
		},
		&cli.StringFlag{
			Name:    "PartSize",
			EnvVars: []string{"PartSize"},
			Value:   "16MiB",
		},
		&cli.UintFlag{
			Name:    "NumThreads",
			EnvVars: []string{"NumThreads"},
			Value:   4,
		},
		&cli.BoolFlag{
			Name:    "EnableMemCache",
			EnvVars: []string{"EnableMemCache"},
			Usage:   "after turning it on, it will obviously occupy memory. PartSize*NumThreads",
		},
		&cli.BoolFlag{
			Name:    "DisableMultipart",
			EnvVars: []string{"DisableMultipart"},
			Value:   true,
		},
		&cli.BoolFlag{
			Name:    "DisableContentSha256",
			EnvVars: []string{"DisableContentSha256"},
			Value:   true,
		},
		&cli.IntFlag{
			Name:    "concurrent",
			EnvVars: []string{"concurrent"},
			Value:   10,
		},
	},
	Action: downloadAction,
}

func downloadAction(cctx *cli.Context) error {

	dst_bucket := cctx.String("dst_bucket")
	dst_region := cctx.String("dst_region")
	dst_prefix := cctx.String("dst_prefix")

	PartSize, err := humanize.ParseBytes(cctx.String("PartSize"))
	if err != nil {
		return err
	}
	NumThreads := cctx.Uint("NumThreads")
	ConcurrentStreamParts := cctx.Bool("EnableMemCache")
	DisableMultipart := cctx.Bool("DisableMultipart")
	DisableContentSha256 := cctx.Bool("DisableContentSha256")
	disableLookupDomain = cctx.Bool("disable_lookup")

	parsedDst, err := url.Parse(cctx.String("dst_endpoint"))
	if err != nil {
		return err
	}
	dst_endpoint := parsedDst.Host
	dst_ssl := parsedDst.Scheme == "https"
	dstOptions := &minio.Options{
		Creds:     credentials.NewStaticV4(cctx.String("dst_ak"), cctx.String("dst_sk"), ""),
		Secure:    dst_ssl,
		Region:    dst_region,
		Transport: transport,
	}

	ctx := context.Background()
	// A wait group to manage the number of active goroutines.
	var wg sync.WaitGroup
	// Create a buffered channel to manage the number of workers.
	workerCh := make(chan struct{}, cctx.Int("concurrent"))

	content, err := os.ReadFile(cctx.String("filelist"))
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	for _, key := range lines {
		// Start a new worker.
		wg.Add(1)
		workerCh <- struct{}{} // Add to the worker queue.
		go func(key string) {
			defer wg.Done()
			defer func() {
				<-workerCh // Remove from the worker queue.
			}()

			// 解析 URL
			parsedURL, err := url.Parse(key)
			if err != nil {
				log.Println(err)
				return
			}
			// 提取路径的最后一部分
			objectName := path.Base(parsedURL.Path)

			dst, err := minio.New(dst_endpoint, dstOptions)
			if err != nil {
				log.Println(err)
				return
			}

			// Check if object already exists in the destination bucket.
			log.Printf("start StatObject %s in bucket %s\n", path.Join(dst_prefix, objectName), dst_bucket)
			_, err = dst.StatObject(ctx, dst_bucket, path.Join(dst_prefix, objectName), minio.StatObjectOptions{})
			if err == nil {
				log.Printf("object %s already exists in destination bucket %s\n", objectName, dst_bucket)
				return
			} else if !strings.Contains(err.Error(), "The specified key does not exist.") {
				log.Println("StatObject error:", err)
				return
			}

			log.Printf("start fetch %s\n", key)
			response, err := http.Get(key)
			if err != nil {
				log.Println("http Get Error:", err)
				return
			}
			defer response.Body.Close()

			log.Printf("start upload %s to bucket %s\n", path.Join(dst_prefix, objectName), dst_bucket)
			_, err = dst.PutObject(ctx, dst_bucket, path.Join(dst_prefix, objectName), response.Body, response.ContentLength, minio.PutObjectOptions{NumThreads: NumThreads, PartSize: PartSize, ConcurrentStreamParts: ConcurrentStreamParts, DisableMultipart: DisableMultipart, DisableContentSha256: DisableContentSha256})
			if err != nil {
				log.Println("PutObject error:", err)
				return
			}
			log.Printf("object %s download to destination bucket %s\n", path.Join(dst_prefix, objectName), dst_bucket)

		}(key)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return nil
}
