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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v2"
)

var migrate = &cli.Command{
	Name:  "migrate",
	Usage: "s3 to s3 migrate",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "src_endpoint",
			EnvVars:  []string{"src_endpoint"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "src_ak",
			EnvVars:  []string{"src_ak"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "src_sk",
			EnvVars:  []string{"src_sk"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "src_bucket",
			EnvVars:  []string{"src_bucket"},
			Required: true,
		},
		&cli.StringFlag{
			Name:     "src_region",
			EnvVars:  []string{"src_region"},
			Required: false,
			Hidden:   true,
		},
		&cli.StringFlag{
			Name:    "src_prefix",
			EnvVars: []string{"src_prefix"},
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
			Name:    "filelist",
			EnvVars: []string{"filelist"},
			Usage:   "specify the list to be migrated, one object per line",
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
		&cli.BoolFlag{
			Name:    "watch",
			EnvVars: []string{"watch"},
			Usage:   "loop to check if there is new data",
		},
		&cli.BoolFlag{
			Name:    "remove",
			EnvVars: []string{"remove"},
			Usage:   "delete after completion",
		},
		&cli.StringFlag{
			Name:    "src_uuid",
			EnvVars: []string{"src_uuid"},
			Usage:   "src storage uuid",
		},
		&cli.StringFlag{
			Name:    "dst_uuid",
			EnvVars: []string{"dst_uuid"},
			Usage:   "dst storage uuid",
		},
		&cli.StringFlag{
			Name:    "rpc",
			EnvVars: []string{"rpc"},
			Usage:   "miner rpc, http://localhost:2345/rpc/v0",
		},
		&cli.StringFlag{
			Name:    "token",
			EnvVars: []string{"token"},
			Usage:   "miner admin token",
		},
	},
	UsageText: `
src_endpoint and dst_endpoint must use type scheme://domain[:port], example http://example.com[:80]
`,
	Action: migrateAction,
}

func migrateAction(cctx *cli.Context) error {

	src_bucket := cctx.String("src_bucket")
	src_region := cctx.String("src_region")
	src_prefix := cctx.String("src_prefix")
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
	remove := cctx.Bool("remove")

	if cctx.IsSet("src_uuid") || cctx.IsSet("dst_uuid") || cctx.IsSet("rpc") || cctx.IsSet("token") {
		srcUuid = cctx.String("src_uuid")
		dstUuid = cctx.String("dst_uuid")
		rpc = cctx.String("rpc")
		token = cctx.String("token")
		if srcUuid == "" || dstUuid == "" || rpc == "" || token == "" {
			return fmt.Errorf("must srcUuid,dstUuid,rpc,token all set")
		}
	}

	// url parse
	parsedSrc, err := url.Parse(cctx.String("src_endpoint"))
	if err != nil {
		return err
	}
	src_endpoint := parsedSrc.Host
	src_ssl := parsedSrc.Scheme == "https"
	srcOptions := &minio.Options{
		Creds:     credentials.NewStaticV4(cctx.String("src_ak"), cctx.String("src_sk"), ""),
		Secure:    src_ssl,
		Region:    src_region,
		Transport: transport,
	}

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

	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		alreadyJobs := make(map[string]time.Time)
		for {
			if cctx.IsSet("filelist") {
				content, err := os.ReadFile(cctx.String("filelist"))
				if err != nil {
					log.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(string(content)), "\n")
				for _, key := range lines {
					objectsCh <- minio.ObjectInfo{Key: key}
				}
				return
			}

			s3SrcClient, err := minio.New(src_endpoint, srcOptions)
			if err != nil {
				log.Fatal(err)
				return
			}
			// 每60分钟列出object，48小时内已经派发的任务不会重复派，48小时之前已经派发的任务还会重新派（如果文件已经在目标位置存在不会重新传输）
			tmpCh := s3SrcClient.ListObjects(ctx, src_bucket, minio.ListObjectsOptions{
				Prefix:    src_prefix,
				Recursive: true,
			})
			for obj := range tmpCh {
				if _, ok := alreadyJobs[obj.Key]; ok {
					continue
				}
				objectsCh <- obj
				alreadyJobs[obj.Key] = time.Now()
			}
			if !cctx.Bool("watch") {
				return
			}
			time.Sleep(60 * time.Minute)

			deleteOldEntries(alreadyJobs, 48)
		}

	}()

	// A wait group to manage the number of active goroutines.
	var wg sync.WaitGroup
	// Create a buffered channel to manage the number of workers.
	workerCh := make(chan struct{}, cctx.Int("concurrent"))

	for object := range objectsCh {
		if object.Err != nil {
			log.Println("ListObjects error:", object.Err)
			continue
		}

		// Start a new worker.
		wg.Add(1)
		workerCh <- struct{}{} // Add to the worker queue.
		go func(object minio.ObjectInfo) {
			defer wg.Done()
			defer func() {
				<-workerCh // Remove from the worker queue.
			}()

			src, err := minio.New(src_endpoint, srcOptions)
			if err != nil {
				log.Println(err)
				return
			}

			dst, err := minio.New(dst_endpoint, dstOptions)
			if err != nil {
				log.Println(err)
				return
			}

			// Check if object already exists in the destination bucket.
			log.Printf("start StatObject %s in bucket %s\n", path.Join(dst_prefix, object.Key), dst_bucket)
			_, err = dst.StatObject(ctx, dst_bucket, path.Join(dst_prefix, object.Key), minio.StatObjectOptions{})
			if err == nil {
				log.Printf("object %s already exists in destination bucket %s\n", object.Key, dst_bucket)
				return
			} else if !strings.Contains(err.Error(), "The specified key does not exist.") {
				log.Println("StatObject error:", err)
				return
			}

			log.Printf("start GetObject %s in bucket %s\n", object.Key, src_bucket)
			reader, err := src.GetObject(ctx, src_bucket, object.Key, minio.GetObjectOptions{})
			if err != nil {
				log.Println("GetObject error:", err)
				return
			}
			defer reader.Close()

			info, err := reader.Stat()
			if err != nil {
				log.Println("Stat error:", err)
			}
			object.Size = info.Size

			log.Printf("start upload %s to bucket %s\n", path.Join(dst_prefix, object.Key), dst_bucket)
			_, err = dst.PutObject(ctx, dst_bucket, path.Join(dst_prefix, object.Key), reader, object.Size, minio.PutObjectOptions{NumThreads: NumThreads, PartSize: PartSize, ConcurrentStreamParts: ConcurrentStreamParts, DisableMultipart: DisableMultipart, DisableContentSha256: DisableContentSha256})
			if err != nil {
				log.Println("PutObject error:", err)
				return
			}
			log.Printf("object %s copied to destination bucket %s\n", object.Key, dst_bucket)

			if srcUuid != "" {
				err := changeStorage(object.Key, srcUuid, dstUuid)
				if err != nil {
					log.Println("changeStorage error:", err)
					return
				}
			}
			if remove {
				err = src.RemoveObject(ctx, src_bucket, object.Key, minio.RemoveObjectOptions{})
				if err != nil {
					log.Println("RemoveObject error:", err)
					return
				}
				log.Printf("remove %s success\n", object.Key)
			}

		}(object)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return nil
}
