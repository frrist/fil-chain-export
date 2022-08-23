package filexport

import (
	"context"
	"fmt"
	"io"

	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
	"golang.org/x/xerrors"
)

var log = logging.Logger("filexport")

func ExportRange(ctx context.Context, bs bstore.Blockstore, head, tail *types.TipSet, messages, receipts, stateroots bool, workers int64, w io.Writer) error {
	h := &car.CarHeader{
		Roots:   head.Cids(),
		Version: 1,
	}

	if err := car.WriteHeader(h, w); err != nil {
		return xerrors.Errorf("failed to write car header: %s", err)
	}

	return WalkRange(ctx, bs, head, tail, messages, receipts, stateroots, workers, func(c cid.Cid) error {
		blk, err := bs.Get(ctx, c)
		if err != nil {
			return fmt.Errorf("writing object to car, bs.Get: %w", err)
		}

		if err := carutil.LdWrite(w, c.Bytes(), blk.RawData()); err != nil {
			return fmt.Errorf("failed to write block to car output: %w", err)
		}
		return nil
	})
}

func WalkRange(ctx context.Context, bs bstore.Blockstore, head, tail *types.TipSet, messages, receipts, stateroots bool, workers int64, cb func(cid cid.Cid) error) error {
	tasks := []*walkTask{}
	for i := range head.Blocks() {
		tasks = append(tasks, &walkTask{
			c:        head.Blocks()[i].Cid(),
			taskType: 0,
		})
	}

	cfg := &walkSchedulerConfig{
		numWorkers:      workers,
		head:            head,
		tail:            tail,
		includeMessages: messages,
		includeState:    stateroots,
		includeReceipts: receipts,
	}

	pw, ctx := newWalkScheduler(ctx, bs, cfg, tasks...)
	results := make(chan *walkResult)
	pw.startScheduler(ctx)
	pw.startWorkers(ctx, results)

	done := make(chan struct{})
	var cbErr error
	go func() {
		defer close(done)
		for res := range results {
			if err := cb(res.c); err != nil {
				log.Errorw("export callback error", "error", err)
				cbErr = err
				return
			}
		}
	}()
	err := pw.grp.Wait()
	if err != nil {
		log.Errorw("walker scheduler", "error", err)
	}
	if cbErr != nil {
		return cbErr
	}
	close(results)
	return err
}
