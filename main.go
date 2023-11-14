package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:    "s3-tools",
		Usage:   "s3 tools",
		Version: UserVersion(),
		Commands: []*cli.Command{
			migrate,
			download,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("ERROR: %+v\n", err)
		os.Exit(1)
		return
	}
}
