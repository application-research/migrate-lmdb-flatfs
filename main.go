package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cheggaaa/pb/v3"
	lmdb "github.com/filecoin-project/go-bs-lmdb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Usage = "[lmdb blockstore path]"
	app.Description = "Migrates lmdb blockstore to flatfs"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "sharding-function",
			Value: "/repo/flatfs/shard/v1/next-to-last/3",
		},
		&cli.UintFlag{
			Name:  "buf-len",
			Usage: "How many blocks to write at a time using PutMany",
			Value: 10000,
		},
		&cli.UintFlag{
			Name:  "num-workers",
			Usage: "How many concurrent write workers to use",
			Value: 3,
		},
		&cli.BoolFlag{
			Name:  "print-blocks",
			Usage: "whether to print the CID of each migrated block",
			Value: false,
		},
	}
	app.Action = func(ctx *cli.Context) error {
		maybeRelativePath := ctx.Args().Get(0)
		absolutePath, err := filepath.Abs(maybeRelativePath)
		if err != nil {
			return fmt.Errorf("invalid blockstore path %s: %v", maybeRelativePath, err)
		}

		fmt.Printf("Attempting to migrate %s\n", absolutePath)

		// Attempt to open the lmdb blockstore to be migrated
		lmdbBlockstoreSize, err := dirSize(absolutePath)
		if err != nil {
			return fmt.Errorf("could not stat blockstore: %v", err)
		}
		lmdbBlockstore, err := openOldLMDBBlockstore(absolutePath)
		if err != nil {
			return err
		}

		// Create the flatfs blockstore to write into
		flatfsBlockstorePath := absolutePath + "-migrated"
		flatfsBlockstore, flatfsBlockstoreCloser, err := createFlatfsBlockstore(
			flatfsBlockstorePath,
			ctx.String("sharding-function"),
		)
		if err != nil {
			return err
		}

		// Move all the blocks from lmdb blockstore to flatfs blockstore
		fmt.Printf("Using buf len: %v\n", ctx.Uint("buf-len"))
		fmt.Printf("Writing blocks...\n")
		if err := transferBlocks(
			ctx.Context,
			lmdbBlockstore,
			flatfsBlockstore,
			lmdbBlockstoreSize,
			ctx.Uint("buf-len"),
			ctx.Uint("num-workers"),
			ctx.Bool("print-blocks"),
		); err != nil {
			return err
		}
		fmt.Printf("Done\n")

		if err := flatfsBlockstoreCloser(); err != nil {
			return fmt.Errorf("failed to close flatfs blockstore: %v", err)
		}

		fmt.Printf("Finished\n")

		return nil
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Printf("%v\n", err)
	}
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func transferBlocks(
	ctx context.Context,
	from blockstore.Blockstore,
	to blockstore.Blockstore,
	size int64,
	bufLen uint,
	numWorkers uint,
	printBlocks bool,
) error {

	allLMDBKeys, err := from.AllKeysChan(ctx)
	if err != nil {
		return fmt.Errorf("could not get all lmdb keys channel: %v", err)
	}

	var buffer []blocks.Block

	bar := pb.New64(size).Set(pb.Bytes, true).Set(pb.CleanOnFinish, true)
	bar.Start()
	defer bar.Finish()

	writeQueue := make(chan []blocks.Block, numWorkers)

	var wg sync.WaitGroup

	for i := 0; i < int(numWorkers); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blocks := range writeQueue {
				if err := to.PutMany(ctx, blocks); err != nil {
					fmt.Printf("could not write blocks to flatfs blockstore: %v", err)
					os.Exit(1)
				}
			}
		}()
	}

	for cid := range allLMDBKeys {
		block, err := from.Get(ctx, cid)
		if err != nil {
			return fmt.Errorf("could not get expected block '%s' from lmdb blockstore: %v", cid, err)
		}

		buffer = append(buffer, block)
		if len(buffer) == int(bufLen) {
			writeQueue <- buffer
			buffer = nil
		}

		if printBlocks {
			fmt.Println("=> %s\n", cid)
		}

		bar.Add(len(block.RawData()))
	}

	close(writeQueue)

	wg.Wait()

	return nil
}

func openOldLMDBBlockstore(blockstorePath string) (blockstore.Blockstore, error) {
	lmdbBlockstore, err := lmdb.Open(&lmdb.Options{
		Path:   blockstorePath,
		NoSync: true,
	})
	if err != nil {
		return nil, fmt.Errorf("could not open the target directory as an lmdb blockstore: %v", err)
	}

	return lmdbBlockstore, nil
}

func createFlatfsBlockstore(blockstorePath string, shardingFunctionString string) (blockstore.Blockstore, func() error, error) {
	shardingFunction, err := flatfs.ParseShardFunc(shardingFunctionString)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid sharding function '%s': %v", shardingFunctionString, err)
	}
	fmt.Printf("Creating new temporary flatfs blockstore at '%s'\n", blockstorePath)
	flatfsDatastore, err := flatfs.CreateOrOpen(blockstorePath, shardingFunction, false)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create flatfs blockstore: %v", err)
	}

	return blockstore.NewBlockstoreNoPrefix(flatfsDatastore), func() error {
		if err := flatfsDatastore.Sync(context.Background(), datastore.NewKey("/")); err != nil {
			return err
		}
		if err := flatfsDatastore.Close(); err != nil {
			return err
		}
		return nil
	}, nil
}
