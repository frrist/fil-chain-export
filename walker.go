package filexport

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	bstore "github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/sync/errgroup"
)

type walkTask struct {
	c        cid.Cid
	taskType taskType
}

type walkResult struct {
	c cid.Cid
}

type walkSchedulerConfig struct {
	numWorkers      int64
	head            *types.TipSet
	tail            *types.TipSet
	includeMessages bool
	includeReceipts bool
	includeState    bool
}

func newWalkScheduler(ctx context.Context, store bstore.Blockstore, cfg *walkSchedulerConfig, rootTasks ...*walkTask) (*walkScheduler, context.Context) {
	tailSet := cid.NewSet()
	for i := range cfg.tail.Cids() {
		tailSet.Add(cfg.tail.Cids()[i])
	}
	grp, ctx := errgroup.WithContext(ctx)
	s := &walkScheduler{
		store:      store,
		numWorkers: cfg.numWorkers,
		stack:      rootTasks,
		in:         make(chan *walkTask, cfg.numWorkers),
		out:        make(chan *walkTask, cfg.numWorkers),
		grp:        grp,
		tail:       tailSet,
		cfg:        cfg,
	}
	s.taskWg.Add(len(rootTasks))
	return s, ctx
}

type walkScheduler struct {
	store bstore.Blockstore
	// number of worker routine to spawn
	numWorkers int64
	// buffer holds tasks until they are processed
	stack []*walkTask
	// inbound and outbound tasks
	in, out chan *walkTask
	// tracks number of inflight tasks
	taskWg sync.WaitGroup
	// launches workers and collects errors if any occur
	grp *errgroup.Group
	// set of tasks seen
	seen sync.Map

	tail *cid.Set
	cfg  *walkSchedulerConfig
}

func (s *walkScheduler) enqueueIfNew(task *walkTask) {
	if task.c.Prefix().MhType == mh.IDENTITY {
		//log.Infow("ignored", "cid", todo.c.String())
		return
	}
	if task.c.Prefix().Codec != cid.Raw && task.c.Prefix().Codec != cid.DagCBOR {
		//log.Infow("ignored", "cid", todo.c.String())
		return
	}
	if _, ok := s.seen.Load(task.c); ok {
		return
	}
	//log.Infow("enqueue", "type", task.taskType.String(), "cid", task.c.String())
	s.taskWg.Add(1)
	s.in <- task
	s.seen.Store(task.c, struct{}{})
}

func (s *walkScheduler) startScheduler(ctx context.Context) {
	s.grp.Go(func() error {
		defer func() {
			close(s.out)
			// Because the workers may have exited early (due to the context being canceled).
			for range s.out {
				s.taskWg.Done()
			}
			// Because the workers may have enqueued additional tasks.
			for range s.in {
				s.taskWg.Done()
			}
			// now, the waitgroup should be at 0, and the goroutine that was _waiting_ on it should have exited.
		}()
		go func() {
			s.taskWg.Wait()
			close(s.in)
		}()
		for {
			if n := len(s.stack) - 1; n >= 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				case s.out <- s.stack[n]:
					s.stack[n] = nil
					s.stack = s.stack[:n]
				}
			} else {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case newJob, ok := <-s.in:
					if !ok {
						return nil
					}
					s.stack = append(s.stack, newJob)
				}
			}
		}
	})
}

func (s *walkScheduler) startWorkers(ctx context.Context, out chan *walkResult) {
	for i := int64(0); i < s.numWorkers; i++ {
		s.grp.Go(func() error {
			for task := range s.out {
				if err := s.work(ctx, task, out); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

type taskType int

// used for debugging
func (t taskType) String() string {
	switch t {
	case Block:
		return "block"
	case Message:
		return "message"
	case Receipt:
		return "receipt"
	case State:
		return "state"
	case Dag:
		return "dag"
	}
	panic(fmt.Sprintf("unknow task %d", t))
}

const (
	Block taskType = iota
	Message
	Receipt
	State
	Dag
)

func (s *walkScheduler) work(ctx context.Context, todo *walkTask, results chan *walkResult) error {
	defer s.taskWg.Done()
	// unseen cid, its a result
	results <- &walkResult{c: todo.c}

	// extract relevant dags to walk from the block
	if todo.taskType == 0 {
		if err := s.store.View(ctx, todo.c, func(data []byte) error {
			var b types.BlockHeader
			if err := b.UnmarshalCBOR(bytes.NewBuffer(data)); err != nil {
				return fmt.Errorf("unmarshaling block header (cid=%s): %w", todo.c, err)
			}
			if b.Height%1000 == 0 {
				log.Infow("export block", "height", b.Height)
			}
			if b.Height == 0 {
				log.Infow("GENESIS")
				for i := range b.Parents {
					s.enqueueIfNew(&walkTask{
						c:        b.Parents[i],
						taskType: Dag,
					})
				}
				s.enqueueIfNew(&walkTask{
					c:        b.ParentStateRoot,
					taskType: State,
				})
				return nil
			}
			// enqueue block parents
			for i := range b.Parents {
				s.enqueueIfNew(&walkTask{
					c:        b.Parents[i],
					taskType: Block,
				})
			}
			if s.cfg.tail.Height() >= b.Height {
				//log.Infow("tail reached", "cid", blk.String())
				return nil
			}

			if s.cfg.includeMessages {
				// enqueue block messages
				s.enqueueIfNew(&walkTask{
					c:        b.Messages,
					taskType: Message,
				})
			}
			if s.cfg.includeReceipts {
				// enqueue block receipts
				s.enqueueIfNew(&walkTask{
					c:        b.ParentMessageReceipts,
					taskType: Receipt,
				})
			}
			if s.cfg.includeState {
				s.enqueueIfNew(&walkTask{
					c:        b.ParentStateRoot,
					taskType: State,
				})
			}

			return nil
		}); err != nil {
			return err
		}
	}
	return s.store.View(ctx, todo.c, func(data []byte) error {
		return cbg.ScanForLinks(bytes.NewReader(data), func(c cid.Cid) {
			if todo.c.Prefix().Codec != cid.DagCBOR || todo.c.Prefix().MhType == mh.IDENTITY {
				return
			}

			s.enqueueIfNew(&walkTask{
				c:        c,
				taskType: Dag,
			})
		})
	})
}
