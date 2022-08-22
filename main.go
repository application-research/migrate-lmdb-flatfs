package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"

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
	app.Description = "Replaces an lmdb blockstore with a new unprefixed flatfs blockstore and creates a backup of the old lmdb blockstore"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "sharding-function",
			Value: "/repo/flatfs/shard/v1/next-to-last/3",
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

		// Create the flatfs blockstore to write into in the tmp directory
		tmpFlatfsBlockstorePath := path.Join(
			path.Dir(absolutePath),
			fmt.Sprintf("migrate-lmdb-flatfs-tmp-%d", rand.Uint32()),
		)
		flatfsBlockstore, flatfsBlockstoreCloser, err := createTmpFlatfsBlockstore(
			tmpFlatfsBlockstorePath,
			ctx.String("sharding-function"),
		)
		if err != nil {
			return err
		}

		// Move all the blocks from lmdb blockstore to flatfs blockstore
		fmt.Printf("Writing blocks...\n")
		if err := transferBlocks(ctx.Context, lmdbBlockstore, flatfsBlockstore, lmdbBlockstoreSize); err != nil {
			return err
		}
		fmt.Printf("Done\n")

		if err := flatfsBlockstoreCloser(); err != nil {
			return fmt.Errorf("failed to close flatfs blockstore: %v", err)
		}

		// Move the old blockstore to the backup location so the new blockstore can be placed
		backupPath := strings.TrimRight(absolutePath, "/\\") + "-backup"
		fmt.Printf("Moving old blockstore to backup path '%s'\n", backupPath)
		if err := os.Rename(absolutePath, backupPath); err != nil {
			return fmt.Errorf("failed to move old blockstore to backup location: %v", err)
		}

		// Move the new blockstore to the target location
		fmt.Printf("Moving new blockstore to target path '%s'\n", absolutePath)
		if err := os.Rename(tmpFlatfsBlockstorePath, absolutePath); err != nil {
			return fmt.Errorf("failed to move new blockstore to target location: %v", err)
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
) error {

	allLMDBKeys, err := from.AllKeysChan(ctx)
	if err != nil {
		return fmt.Errorf("could not get all lmdb keys channel: %v", err)
	}

	var buffer []blocks.Block

	bar := pb.New64(size).Set(pb.Bytes, true).Set(pb.CleanOnFinish, true)
	bar.Start()
	defer bar.Finish()

	for cid := range allLMDBKeys {
		block, err := from.Get(ctx, cid)
		if err != nil {
			return fmt.Errorf("could not get expected block '%s' from lmdb blockstore: %v", cid, err)
		}

		buffer = append(buffer, block)
		if len(buffer) >= 100 {
			if err := to.PutMany(ctx, buffer); err != nil {
				return fmt.Errorf("could not write block '%s' to flatfs blockstore: %v", cid, err)
			}
			buffer = nil
		}

		bar.Add(len(block.RawData()))
	}

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

func createTmpFlatfsBlockstore(blockstorePath string, shardingFunctionString string) (blockstore.Blockstore, func() error, error) {
	shardingFunction, err := flatfs.ParseShardFunc(shardingFunctionString)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid sharding function '%s': %v", shardingFunctionString, err)
	}
	fmt.Printf("Creating new temporary flatfs blockstore at '%s'\n", blockstorePath)
	flatfsDatastore, err := flatfs.CreateOrOpen(blockstorePath, shardingFunction, false)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create temporary flatfs blockstore: %v", err)
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