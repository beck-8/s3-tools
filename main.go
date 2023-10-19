package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:     "s3-migrate",
		Usage:    "s3 to s3 tools",
		Version:  UserVersion(),
		Commands: []*cli.Command{},
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
				Name:     "src_prefix",
				EnvVars:  []string{"src_prefix"},
				Required: true,
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
				Name:    "dst_prefix",
				EnvVars: []string{"dst_prefix"},
			},
			&cli.StringFlag{
				Name:    "filelist",
				EnvVars: []string{"filelist"},
			},
			&cli.IntFlag{
				Name:    "concurrent",
				EnvVars: []string{"concurrent"},
				Value:   10,
			},
			&cli.BoolFlag{
				Name:    "watch",
				EnvVars: []string{"watch"},
			},
			&cli.BoolFlag{
				Name:    "remove",
				EnvVars: []string{"remove"},
				Usage:   "delete after completion",
			},
		},
		UsageText: `
src_endpoint and dst_endpoint must use type scheme://domain:port, example http://example.com:80
`,
		Action: action,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("ERROR: %+v\n", err)
		os.Exit(1)
		return
	}
}

func action(cctx *cli.Context) error {
	// url parse
	parsedSrc, err := url.Parse(cctx.String("src_endpoint"))
	if err != nil {
		return err
	}
	src_endpoint := parsedSrc.Host
	src_ssl := parsedSrc.Scheme == "https"
	srcOptions := &minio.Options{
		Creds:  credentials.NewStaticV4(cctx.String("src_ak"), cctx.String("src_sk"), ""),
		Secure: src_ssl,
	}

	parsedDst, err := url.Parse(cctx.String("dst_endpoint"))
	if err != nil {
		return err
	}
	dst_endpoint := parsedDst.Host
	dst_ssl := parsedDst.Scheme == "https"
	dstOptions := &minio.Options{
		Creds:  credentials.NewStaticV4(cctx.String("dst_ak"), cctx.String("dst_sk"), ""),
		Secure: dst_ssl,
	}

	src_bucket := cctx.String("src_bucket")
	src_prefix := cctx.String("src_prefix")
	dst_bucket := cctx.String("dst_bucket")
	dst_prefix := cctx.String("dst_prefix")
	fmt.Println(cctx.String("dst_prefix"))
	remove := cctx.Bool("remove")

	ctx := context.Background()
	s3SrcClient, err := minio.New(nslookupShuf(src_endpoint), srcOptions)
	if err != nil {
		return err
	}
	s3DstClient, err := minio.New(nslookupShuf(dst_endpoint), dstOptions)
	if err != nil {
		return err
	}

	objectsCh := make(chan minio.ObjectInfo)
	go func() {
		defer close(objectsCh)
		for {
			if cctx.IsSet("filelist") {
				content, err := os.ReadFile(cctx.String("filelist"))
				if err != nil {
					log.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(string(content)), "\n")
				for _, key := range lines {
					fmt.Println(key)
					objectsCh <- minio.ObjectInfo{Key: key}
				}
				return
			}

			tmpCh := s3SrcClient.ListObjects(ctx, src_bucket, minio.ListObjectsOptions{
				Prefix:    src_prefix,
				Recursive: true,
			})
			for obj := range tmpCh {
				objectsCh <- obj
			}
			if !cctx.Bool("watch") {
				return
			}
			time.Sleep(100 * time.Second)
		}

	}()

	// A wait group to manage the number of active goroutines.
	var wg sync.WaitGroup
	// Create a buffered channel to manage the number of workers.
	workerCh := make(chan struct{}, cctx.Int("concurrent"))

	for object := range objectsCh {
		log.Println(object.Key)
		if object.Err != nil {
			log.Println(object.Err)
			return object.Err
		}

		// Check if object already exists in the destination bucket.
		_, err := s3DstClient.StatObject(ctx, dst_bucket, object.Key, minio.StatObjectOptions{})
		if err == nil {
			log.Printf("object %s already exists in destination bucket %s\n", object.Key, dst_bucket)
			continue
		} else {
			log.Println(err)
		}

		// Start a new worker.
		wg.Add(1)
		workerCh <- struct{}{} // Add to the worker queue.
		go func(object minio.ObjectInfo) {
			defer wg.Done()
			defer func() {
				<-workerCh // Remove from the worker queue.
			}()

			src, err := minio.New(nslookupShuf(src_endpoint), srcOptions)
			if err != nil {
				log.Println(err)
				return
			}

			dst, err := minio.New(nslookupShuf(dst_endpoint), dstOptions)
			if err != nil {
				log.Println(err)
				return
			}

			reader, err := src.GetObject(ctx, src_bucket, object.Key, minio.GetObjectOptions{})
			if err != nil {
				log.Println(err)
				return
			}
			defer reader.Close()

			log.Printf("start upload %s to %s\n", object.Key, dst_bucket)
			fmt.Println(path.Join(dst_prefix, object.Key))
			_, err = dst.PutObject(ctx, dst_bucket, path.Join(dst_prefix, object.Key), reader, object.Size, minio.PutObjectOptions{})
			if err != nil {
				log.Println(err)
				return
			}

			log.Printf("object %s copied to destination bucket %s\n", object.Key, dst_bucket)

			if remove {
				err = src.RemoveObject(ctx, src_bucket, object.Key, minio.RemoveObjectOptions{})
				if err != nil {
					log.Println(err)
				}
				log.Printf("remove %s success\n", object.Key)
			}

		}(object)
	}

	// Wait for all workers to finish.
	wg.Wait()
	return nil
}

func nslookupShuf(input string) string {
	host, port, err := net.SplitHostPort(input)
	if err != nil {
		log.Fatalln(err)
	}
	ips, err := net.LookupHost(host)
	if err != nil {
		log.Fatalln(err)
	}

	// 设置随机数种子
	rand.Seed(time.Now().UnixNano())
	// 从 IP 列表中随机选择一个 IP
	randomIndex := rand.Intn(len(ips))
	randomIP := ips[randomIndex]
	return fmt.Sprintf("%s:%s", randomIP, port)

}
