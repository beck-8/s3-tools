package main

import (
	"context"
	"fmt"
	"log"
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

var upload = &cli.Command{
	Name:  "upload",
	Usage: "upload local file to s3",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "dir",
			EnvVars: []string{"dir"},
		},
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
			Name:    "dst_bucket_lookup",
			EnvVars: []string{"dst_bucket_lookup"},
			Value:   "auto",
			Usage:   "bucket lookup type: dns, path, auto",
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
	Action: uploadAction,
}

func uploadAction(cctx *cli.Context) error {

	dst_bucket := cctx.String("dst_bucket")
	dst_region := cctx.String("dst_region")
	dst_prefix := cctx.String("dst_prefix")
	if cctx.IsSet("dir") && cctx.IsSet("filelist") {
		return fmt.Errorf("only be specified dir or filelist")
	}

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

	// Set bucket lookup type based on the flag
	bucketLookup := cctx.String("dst_bucket_lookup")
	switch bucketLookup {
	case "dns":
		dstOptions.BucketLookup = minio.BucketLookupDNS
	case "path":
		dstOptions.BucketLookup = minio.BucketLookupPath
	case "auto":
		dstOptions.BucketLookup = minio.BucketLookupAuto
	default:
		return fmt.Errorf("invalid bucket_lookup value: %s, must be one of: dns, path, auto", bucketLookup)
	}

	ctx := context.Background()
	// A wait group to manage the number of active goroutines.
	var wg sync.WaitGroup
	// Create a buffered channel to manage the number of workers.
	workerCh := make(chan struct{}, cctx.Int("concurrent"))

	var lines []string
	if cctx.IsSet("dir") {
		lines, err = listFiles(cctx.String("dir"))
		if err != nil {
			return err
		}
	} else if cctx.IsSet("filelist") {
		content, err := os.ReadFile(cctx.String("filelist"))
		if err != nil {
			log.Fatal(err)
		}
		lines = strings.Split(strings.TrimSpace(string(content)), "\n")
	}

	for _, key := range lines {
		// Start a new worker.
		wg.Add(1)
		workerCh <- struct{}{} // Add to the worker queue.
		go func(key string) {
			defer wg.Done()
			defer func() {
				<-workerCh // Remove from the worker queue.
			}()

			var objectName string
			if string(key[0]) == "/" {
				objectName = path.Join(dst_prefix, key[1:])
			} else {
				objectName = path.Join(dst_prefix, key)
			}

			dst, err := minio.New(dst_endpoint, dstOptions)
			if err != nil {
				log.Println(err)
				return
			}

			// Check if object already exists in the destination bucket.
			log.Printf("start StatObject %s in bucket %s\n", objectName, dst_bucket)
			_, err = dst.StatObject(ctx, dst_bucket, objectName, minio.StatObjectOptions{})
			if err == nil {
				log.Printf("object %s already exists in destination bucket %s\n", key, dst_bucket)
				return
			} else if !strings.Contains(err.Error(), "The specified key does not exist.") {
				log.Println("StatObject error:", err)
				return
			}

			log.Printf("start upload %s to bucket %s\n", key, dst_bucket)
			_, err = dst.FPutObject(ctx, dst_bucket, objectName, key, minio.PutObjectOptions{NumThreads: NumThreads, PartSize: PartSize, ConcurrentStreamParts: ConcurrentStreamParts, DisableMultipart: DisableMultipart, DisableContentSha256: DisableContentSha256})
			if err != nil {
				log.Println("FPutObject error:", err)
				return
			}
			log.Printf("object %s upload to destination bucket %s\n", key, dst_bucket)

		}(key)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return nil
}
