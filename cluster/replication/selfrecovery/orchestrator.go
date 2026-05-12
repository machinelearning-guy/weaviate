//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package selfrecovery triggers automatic SELF_RECOVERY replication
// ops for shards whose local directories are missing at node startup.
// Wired only into the startup path so newly-added empty replicas via
// scale-out are not misinterpreted as data loss. The actual file copy
// and state machine are handled by the existing replication FSM +
// consumer; this package only probes peers and registers ops.
//
// Per-shard decision: any peer reports data -> register op; all peers
// definitively report no data -> create empty + WARN; any peer
// unreachable -> backoff and retry.
package selfrecovery

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/copier"
	replicationtypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/diskio"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// ErrSelfRecoveryCancelled is the terminal-state sentinel returned by
// registerAndPoll when the FSM reports the op as CANCELLED. runOne uses
// it to distinguish operator-driven cancel (don't retry, count as
// "cancelled") from transient errors (retry with backoff).
var ErrSelfRecoveryCancelled = errors.New("self-recovery op was cancelled")

// ErrSelfRecoveryShardNotInSchema is the sentinel returned by AcceptEmpty
// (and any other operator endpoint that uses the schema gate) when the
// requested (collection, shard) is not present in the local schema. The
// REST handler maps it to 404 instead of 500.
var ErrSelfRecoveryShardNotInSchema = errors.New("shard not in local schema")

// RaftEntryPoint is the subset of *cluster.Raft used by the orchestrator;
// defined locally so tests can stub it.
type RaftEntryPoint interface {
	RegisterSelfRecovery(ctx context.Context, sourceNode, collection, shard, targetNode string) (strfmt.UUID, error)
	GetReplicationDetailsByReplicationId(ctx context.Context, uuid strfmt.UUID) (api.ReplicationDetailsResponse, error)
	GetReplicationDetailsByCollectionAndShard(ctx context.Context, collection, shard string) ([]api.ReplicationDetailsResponse, error)
	CancelReplication(ctx context.Context, uuid strfmt.UUID) error
}

type SchemaReader interface {
	ShardReplicas(class, shard string) (nodes []string, err error)
}

// PathResolver maps (collection, shard) to the local on-disk dir.
type PathResolver interface {
	ShardPath(collection, shard string) string
}

type ShardRef struct {
	Collection string
	Shard      string
}

// Orchestrator coordinates per-shard SELF_RECOVERY work on a single node.
// It is constructed once at startup and submitted to as the index init
// pass discovers shard directories that should be present but are not.
type Orchestrator struct {
	raft                   RaftEntryPoint
	schema                 SchemaReader
	pathResolver           PathResolver
	clientFactory          copier.FileReplicationServiceClientFactory
	nodeSelector           cluster.NodeSelector
	nodeName               string
	enabled                *runtime.DynamicValue[bool]
	concurrency            *runtime.DynamicValue[int]
	maintenanceModeEnabled func() bool // nil-safe; treated as off when nil
	onRecoveryComplete     func(ctx context.Context, collection, shard string) error
	raftBootstrapComplete  func() bool // nil-safe; treated as already-complete when nil
	logger                 logrus.FieldLogger
	pollInterval           time.Duration
	probeTimeout           time.Duration
	probeBackoffMin        time.Duration
	probeBackoffMax        time.Duration
	emptyFallbackHook      func(ShardRef) // nil unless overridden in tests
	metrics                *Metrics

	// Worker pool. Bounded queue prevents unbounded goroutine fan-out
	// when N>>concurrency shards need recovery (e.g. wiped node with
	// many shards). Concurrency = number of workers; queue capacity
	// holds the burst.
	poolOnce  sync.Once
	workQueue chan submission

	// Per-orchestrator RNG, seeded from wall-clock at construction so
	// different nodes shuffle peer order differently. Guarded by rngMu
	// because probeAndDecide runs from multiple workers concurrently.
	rngMu sync.Mutex
	rng   *rand.Rand
}

type submission struct {
	ctx context.Context
	ref ShardRef
}

// submitQueueCapacity bounds the in-flight submission backlog. If
// exceeded, Submit drops the request with a metric/log so memory stays
// bounded. Dropped shards are retried on the next node restart.
const submitQueueCapacity = 1024

type Config struct {
	Raft          RaftEntryPoint
	Schema        SchemaReader
	PathResolver  PathResolver
	ClientFactory copier.FileReplicationServiceClientFactory
	NodeSelector  cluster.NodeSelector
	NodeName      string
	Enabled       *runtime.DynamicValue[bool]
	Concurrency   *runtime.DynamicValue[int]
	// MaintenanceModeEnabled, when non-nil and returning true, makes
	// Submit a no-op so the orchestrator does not start new recoveries
	// during operator-declared maintenance windows. Already-running
	// recoveries are left to finish on their own.
	MaintenanceModeEnabled func() bool
	// OnRecoveryComplete promotes the in-memory wrapper after the
	// orchestrator's empty-fallback materialises an empty live dir.
	// (The SELF_RECOVERY-op path doesn't need it; the consumer's
	// LoadLocalShard handles the swap.)
	OnRecoveryComplete func(ctx context.Context, collection, shard string) error
	// RaftBootstrapComplete reports whether RAFT FSM replay is done.
	// Empty-fallback during the bootstrap window is downgraded
	// (likely a brand-new class added during downtime, not data loss).
	RaftBootstrapComplete func() bool
	Logger                logrus.FieldLogger
	// PollInterval is FSM-polling cadence after registering an op. 5s if zero.
	PollInterval time.Duration
	// ProbeTimeout caps a single ListFiles probe RPC. 5s if zero.
	ProbeTimeout time.Duration
}

func New(cfg Config) *Orchestrator {
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	probeTimeout := cfg.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = 5 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = logrus.NewEntry(logrus.New())
	}
	return &Orchestrator{
		raft:                   cfg.Raft,
		schema:                 cfg.Schema,
		pathResolver:           cfg.PathResolver,
		clientFactory:          cfg.ClientFactory,
		nodeSelector:           cfg.NodeSelector,
		nodeName:               cfg.NodeName,
		enabled:                cfg.Enabled,
		concurrency:            cfg.Concurrency,
		maintenanceModeEnabled: cfg.MaintenanceModeEnabled,
		onRecoveryComplete:     cfg.OnRecoveryComplete,
		raftBootstrapComplete:  cfg.RaftBootstrapComplete,
		logger:                 logger.WithField("component", "self_recovery"),
		pollInterval:           pollInterval,
		probeTimeout:           probeTimeout,
		probeBackoffMin:        5 * time.Second,
		probeBackoffMax:        5 * time.Minute,
		metrics:                GlobalMetrics(),
		rng:                    rand.New(rand.NewSource(cryptoSeed())),
	}
}

// cryptoSeed returns a process-unique int64 seed derived from crypto/rand.
// math/rand is used only for peer-shuffle load distribution (not for
// any security purpose); seeding from crypto/rand makes node startups
// pick independent peer orderings without relying on time/pid entropy.
func cryptoSeed() int64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// Fallback path: cryptographically poor but never reached on
		// a sane OS. Time + pid is still distinct across nodes.
		return time.Now().UnixNano() ^ int64(os.Getpid())
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// Submit asynchronously starts recovery for the shard. No-op when the
// feature flag is off or maintenance mode is on. Returns immediately;
// the worker pool (size = Config.Concurrency) drains the queue.
// If the queue is full (submitQueueCapacity bursts exceeded), the
// submission is dropped with a warning + metric — operator can re-trigger
// or wait for the next node restart.
func (o *Orchestrator) Submit(ctx context.Context, ref ShardRef) {
	if o.enabled == nil || !o.enabled.Get() {
		return
	}
	if o.maintenanceModeEnabled != nil && o.maintenanceModeEnabled() {
		o.logger.WithFields(logrus.Fields{
			"event":      "self_recovery.skipped_maintenance_mode",
			"collection": ref.Collection,
			"shard":      ref.Shard,
		}).Info("self-recovery skipped: node is in maintenance mode")
		return
	}
	o.poolOnce.Do(o.initPool)
	select {
	case o.workQueue <- submission{ctx: ctx, ref: ref}:
	default:
		if o.metrics != nil {
			o.metrics.SubmitDroppedTotal.Inc()
		}
		o.logger.WithFields(logrus.Fields{
			"event":      "self_recovery.submit_dropped",
			"collection": ref.Collection,
			"shard":      ref.Shard,
			"queue_cap":  submitQueueCapacity,
		}).Warn("self-recovery submission dropped: queue full")
	}
}

// Enabled reports whether the SELF_RECOVERY feature flag is on. Useful
// for callers that need to gate behavior (e.g. wrapper installation)
// before invoking Submit, which itself silently no-ops when off.
func (o *Orchestrator) Enabled() bool {
	return o.enabled != nil && o.enabled.Get()
}

// SubmitRecovery is the primitive-typed entry point for callers that
// can't import this package without a cycle.
func (o *Orchestrator) SubmitRecovery(ctx context.Context, collection, shard string) {
	o.Submit(ctx, ShardRef{Collection: collection, Shard: shard})
}

// restartTimeout caps the total time Restart spends cancelling and
// waiting for in-flight ops to settle. Without this, a partial-outage
// cluster (transient FSM errors) makes waitForOpTerminal spin forever.
const restartTimeout = 30 * time.Second

// Restart cancels any in-flight SELF_RECOVERY op for (collection,
// shard) targeting this node, waits for those ops to reach a terminal
// state (so the in-flight copier won't race the rmrf below), erases
// "<shard>.recovering/", then submits a fresh recovery. Bounded by
// restartTimeout: on timeout, leaves the recovery dir intact and
// returns ctx.Err() so the operator can retry.
func (o *Orchestrator) Restart(parentCtx context.Context, ref ShardRef) error {
	ctx, cancel := context.WithTimeout(parentCtx, restartTimeout)
	defer cancel()

	cancelled, err := o.cancelInflightSelfRecoveryOps(ctx, ref)
	if err != nil {
		return fmt.Errorf("restart recovery: cancel in-flight op(s): %w", err)
	}

	for _, uuid := range cancelled {
		if err := o.waitForOpTerminal(ctx, uuid); err != nil {
			return fmt.Errorf("restart recovery: wait for op %s to settle: %w", uuid, err)
		}
	}

	if o.pathResolver != nil {
		recoveryPath := o.pathResolver.ShardPath(ref.Collection, ref.Shard) + api.RecoveryFolderSuffix
		if err := os.RemoveAll(recoveryPath); err != nil {
			return fmt.Errorf("restart recovery: remove %q: %w", recoveryPath, err)
		}
	}

	o.logger.WithFields(logrus.Fields{
		"event":         "self_recovery.restart",
		"collection":    ref.Collection,
		"shard":         ref.Shard,
		"cancelled_ops": cancelled,
	}).Warn("operator restarted self-recovery from scratch")

	// Re-submit with WithoutCancel so the spawned recovery goroutine
	// survives the HTTP-request-bound parent ctx (which is canceled as
	// soon as the handler returns). Values (tracing/logging) are still
	// inherited from parentCtx.
	o.Submit(context.WithoutCancel(parentCtx), ref)
	return nil
}

// RestartRecovery is the primitive-typed entry point for callers that
// can't import this package without a cycle.
func (o *Orchestrator) RestartRecovery(ctx context.Context, collection, shard string) error {
	return o.Restart(ctx, ShardRef{Collection: collection, Shard: shard})
}

// cancelInflightSelfRecoveryOps cancels every non-terminal SELF_RECOVERY
// op on (collection, shard) targeting this node. Returns the UUIDs
// cancelled. A "not found" error means no ops at all and is success.
func (o *Orchestrator) cancelInflightSelfRecoveryOps(ctx context.Context, ref ShardRef) ([]strfmt.UUID, error) {
	if o.raft == nil {
		return nil, nil
	}
	ops, err := o.raft.GetReplicationDetailsByCollectionAndShard(ctx, ref.Collection, ref.Shard)
	if err != nil {
		if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var cancelled []strfmt.UUID
	for _, op := range ops {
		if op.TransferType != api.SELF_RECOVERY.String() {
			continue
		}
		if op.TargetNodeId != o.nodeName {
			continue
		}
		state := api.ShardReplicationState(op.Status.State)
		if state == api.READY || state == api.CANCELLED {
			continue
		}
		if err := o.raft.CancelReplication(ctx, op.Uuid); err != nil {
			return cancelled, fmt.Errorf("cancel op %s: %w", op.Uuid, err)
		}
		// CompletedTotal{result="cancelled"} is incremented by runOne
		// when it observes the FSM's CANCELLED state — the single
		// source of truth. Doing it here too would double-count, and
		// would also tick even if the cancel never propagated.
		cancelled = append(cancelled, op.Uuid)
	}
	return cancelled, nil
}

// vanishedGracePeriod is the extra wait after waitForOpTerminal
// observes ErrReplicationOperationNotFound — the consumer goroutine
// may still be mid-download; this gives it time to notice the
// cancellation and exit before RemoveAll wipes the recovery dir.
const vanishedGracePeriod = 10 * time.Second

// waitForOpTerminal polls the FSM until the op reaches READY or
// CANCELLED. A vanished op (force-deleted upstream) is treated as
// terminal but with an additional grace sleep so a still-running
// consumer goroutine can observe the cancellation. Bounded by the
// caller's ctx.
func (o *Orchestrator) waitForOpTerminal(ctx context.Context, uuid strfmt.UUID) error {
	if o.raft == nil {
		return nil
	}
	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		details, err := o.raft.GetReplicationDetailsByReplicationId(ctx, uuid)
		if err != nil {
			if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
				if !sleepCtx(ctx, vanishedGracePeriod) {
					return ctx.Err()
				}
				return nil
			}
			// transient (e.g. leader change) — keep polling
		} else {
			switch api.ShardReplicationState(details.Status.State) {
			case api.READY, api.CANCELLED:
				return nil
			case api.REGISTERED, api.HYDRATING, api.FINALIZING, api.DEHYDRATING:
				// non-terminal — keep polling
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// CleanupOrphanRecoveryDirs removes "<shard>.recovering/" dirs whose
// live "<shard>/" sibling exists. Reclaims disk after a
// downgrade-then-upgrade cycle. In-flight recoveries (no sibling) are
// untouched.
func (o *Orchestrator) CleanupOrphanRecoveryDirs(rootDataPath string) ([]string, error) {
	const suffix = api.RecoveryFolderSuffix
	if rootDataPath == "" {
		return nil, errors.New("cleanup orphan recovery dirs: empty root data path")
	}
	collections, err := os.ReadDir(rootDataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read data root %q: %w", rootDataPath, err)
	}
	var removed []string
	for _, c := range collections {
		if !c.IsDir() {
			continue
		}
		collDir := filepath.Join(rootDataPath, c.Name())
		shards, err := os.ReadDir(collDir)
		if err != nil {
			o.logger.WithError(err).WithField("dir", collDir).Warn("cleanup: cannot read collection dir")
			continue
		}
		for _, s := range shards {
			if !s.IsDir() || !strings.HasSuffix(s.Name(), suffix) {
				continue
			}
			recoveryDir := filepath.Join(collDir, s.Name())
			liveDir := filepath.Join(collDir, strings.TrimSuffix(s.Name(), suffix))
			if _, err := os.Stat(liveDir); err != nil {
				continue // no sibling: in-flight recovery to resume
			}
			if err := os.RemoveAll(recoveryDir); err != nil {
				o.logger.WithError(err).WithField("dir", recoveryDir).Warn("cleanup: failed to remove orphan recovery dir")
				continue
			}
			o.logger.WithField("dir", recoveryDir).Info("cleanup: removed orphan recovery dir")
			removed = append(removed, recoveryDir)
		}
	}
	return removed, nil
}

// AcceptEmpty is the operator escape hatch for the catastrophic-wipe
// case (no peer has the data). It removes "<shard>.recovering/" and
// creates an empty "<shard>/", then promotes the in-memory wrapper so
// the shard becomes serviceable (otherwise a RecoveringShard wrapper
// would stay load-blocked despite the on-disk dir existing). Does NOT
// cancel in-flight RAFT ops — operator should call
// /replication/replicate/{id}/cancel first.
func (o *Orchestrator) AcceptEmpty(ctx context.Context, ref ShardRef) (string, error) {
	if o.pathResolver == nil {
		return "", errors.New("accept-empty: no PathResolver configured")
	}
	// Refuse to materialise dirs for unknown (collection, shard) — keeps
	// the operator endpoint from creating arbitrary paths under the data
	// root if the schema doesn't actually have this shard.
	if o.schema != nil {
		if _, err := o.schema.ShardReplicas(ref.Collection, ref.Shard); err != nil {
			return "", fmt.Errorf("accept-empty: shard %s/%s: %w",
				ref.Collection, ref.Shard,
				errors.Join(ErrSelfRecoveryShardNotInSchema, err))
		}
	}
	livePath := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
	recoveryPath := livePath + api.RecoveryFolderSuffix

	if _, err := os.Stat(recoveryPath); err == nil {
		if err := os.RemoveAll(recoveryPath); err != nil {
			return "", fmt.Errorf("remove recovery dir %q: %w", recoveryPath, err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		// EACCES, EIO, ELOOP etc. — surface so the operator sees them.
		return "", fmt.Errorf("stat recovery dir %q: %w", recoveryPath, err)
	}
	if err := os.MkdirAll(livePath, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %q: %w", livePath, err)
	}
	if err := diskio.Fsync(filepath.Dir(livePath)); err != nil {
		return "", fmt.Errorf("fsync parent of %q: %w", livePath, err)
	}
	// Promote the in-memory wrapper so the shard transitions out of
	// RECOVERING and becomes serviceable. Mirrors the empty-fallback
	// path inside runOne; without this, the operator's "accept empty"
	// would leave the shard load-blocked behind a RecoveringShard
	// wrapper despite the on-disk dir being ready.
	if o.onRecoveryComplete != nil {
		if err := o.onRecoveryComplete(ctx, ref.Collection, ref.Shard); err != nil {
			return "", fmt.Errorf("accept-empty: promote in-memory wrapper for %s/%s: %w",
				ref.Collection, ref.Shard, err)
		}
	}
	if o.metrics != nil {
		o.metrics.AcceptEmptyTotal.Inc()
	}
	o.logger.WithFields(logrus.Fields{
		"event":      "self_recovery.accept_empty",
		"collection": ref.Collection,
		"shard":      ref.Shard,
		"path":       livePath,
	}).Warn("operator accepted empty shard; recovery aborted")
	return livePath, nil
}

// runOne is the per-shard worker.
func (o *Orchestrator) runOne(ctx context.Context, ref ShardRef) {
	logger := o.logger.WithFields(logrus.Fields{
		"event":      "self_recovery.started",
		"collection": ref.Collection,
		"shard":      ref.Shard,
	})
	logger.Info("starting self-recovery for shard")

	startedAt := time.Now()
	if o.metrics != nil {
		o.metrics.InProgress.Inc()
		defer o.metrics.InProgress.Dec()
	}

	const maxAttempts = 10
	attempts := 0

	backoff := o.probeBackoffMin
	for {
		if ctx.Err() != nil {
			return
		}
		if attempts >= maxAttempts {
			logger.WithField("attempts", attempts).Error("self-recovery exhausted retries; giving up")
			if o.metrics != nil {
				o.metrics.GiveupTotal.Inc()
				o.metrics.CompletedTotal.WithLabelValues("failure").Inc()
				o.metrics.DurationSeconds.WithLabelValues("failure").Observe(time.Since(startedAt).Seconds())
			}
			return
		}

		decision, err := o.probeAndDecide(ctx, ref)
		if err != nil {
			attempts++
			logger.WithError(err).Warn("self-recovery probe failed; will retry")
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, o.probeBackoffMax)
			continue
		}

		switch decision.action {
		case actionRegisterOp:
			if o.metrics != nil {
				o.metrics.StartedTotal.WithLabelValues(decision.sourceNode).Inc()
			}
			if err := o.registerAndPoll(ctx, ref, decision.sourceNode); err != nil {
				// If the op was force-deleted upstream (class/tenant
				// removed, operator ForceDelete*), there is nothing to
				// re-register against — exit cleanly instead of
				// looping until maxAttempts.
				if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
					logger.WithError(err).WithField("source_node", decision.sourceNode).
						Info("self-recovery op was force-deleted; abandoning")
					if o.metrics != nil {
						o.metrics.CompletedTotal.WithLabelValues("cancelled").Inc()
					}
					return
				}
				// Operator-driven cancel via /replication/replicate/{id}/cancel
				// is a terminal outcome — retrying would re-register a fresh
				// SELF_RECOVERY op and negate the cancel.
				if errors.Is(err, ErrSelfRecoveryCancelled) {
					logger.WithError(err).WithField("source_node", decision.sourceNode).
						Info("self-recovery op cancelled; abandoning")
					if o.metrics != nil {
						o.metrics.CompletedTotal.WithLabelValues("cancelled").Inc()
						o.metrics.DurationSeconds.WithLabelValues("cancelled").Observe(time.Since(startedAt).Seconds())
					}
					return
				}
				attempts++
				logger.WithError(err).WithField("source_node", decision.sourceNode).
					Warn("self-recovery register/poll failed; will retry")
				if !sleepCtx(ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff, o.probeBackoffMax)
				continue
			}
			if o.metrics != nil {
				o.metrics.CompletedTotal.WithLabelValues("success").Inc()
				o.metrics.DurationSeconds.WithLabelValues("success").Observe(time.Since(startedAt).Seconds())
			}
			logger.WithFields(logrus.Fields{
				"event":       "self_recovery.completed",
				"source_node": decision.sourceNode,
				"duration_ms": time.Since(startedAt).Milliseconds(),
			}).Info("self-recovery completed")
			return

		case actionEmptyFallback:
			if err := o.emptyFallback(ref); err != nil {
				logger.WithError(err).Error("self-recovery empty-fallback failed")
				if o.metrics != nil {
					o.metrics.CompletedTotal.WithLabelValues("failure").Inc()
					o.metrics.DurationSeconds.WithLabelValues("failure").Observe(time.Since(startedAt).Seconds())
				}
				return
			}
			// Promote the in-memory wrapper so reads/writes resume.
			if o.onRecoveryComplete != nil {
				if err := o.onRecoveryComplete(ctx, ref.Collection, ref.Shard); err != nil {
					logger.WithError(err).Error("self-recovery: promote after empty-fallback failed")
				}
			}
			// Bootstrap-window empty-fallback is most likely a class
			// added during this node's downtime, not data loss.
			// Demote signal severity and route to a separate counter so
			// catastrophic-wipe alerts (no_data_empty_total) stay clean.
			bootstrapWindow := o.raftBootstrapComplete != nil && !o.raftBootstrapComplete()
			if o.metrics != nil {
				if bootstrapWindow {
					o.metrics.NoDataDuringBootstrapTotal.Inc()
				} else {
					o.metrics.NoDataEmptyTotal.Inc()
				}
				o.metrics.CompletedTotal.WithLabelValues("success").Inc()
				o.metrics.DurationSeconds.WithLabelValues("empty_fallback").Observe(time.Since(startedAt).Seconds())
			}
			fallbackFields := logrus.Fields{
				"event":        "self_recovery.empty_fallback",
				"probed_peers": decision.probedPeers,
				"duration_ms":  time.Since(startedAt).Milliseconds(),
				"collection":   ref.Collection,
				"shard":        ref.Shard,
				"action_taken": "created_empty_shard",
			}
			if bootstrapWindow {
				logger.WithFields(fallbackFields).
					Info("no peer has data for shard during bootstrap; treating as fresh class")
			} else {
				fallbackFields["recoverable"] = false
				fallbackFields["operator_note"] = "if data is recoverable from backup, restore now"
				logger.WithFields(fallbackFields).
					Warn("no peer has data for shard; created empty shard")
			}
			if o.emptyFallbackHook != nil {
				o.emptyFallbackHook(ref)
			}
			return

		case actionRetry:
			attempts++
			logger.WithField("retry_in", backoff.String()).Debug("self-recovery: peers unreachable, will retry")
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, o.probeBackoffMax)
			continue
		}
	}
}

type recoveryAction int

const (
	actionRegisterOp recoveryAction = iota
	actionEmptyFallback
	actionRetry
)

type probeDecision struct {
	action      recoveryAction
	sourceNode  string
	probedPeers []string
}

func (o *Orchestrator) probeAndDecide(ctx context.Context, ref ShardRef) (probeDecision, error) {
	replicas, err := o.schema.ShardReplicas(ref.Collection, ref.Shard)
	if err != nil {
		return probeDecision{}, fmt.Errorf("read shard replicas: %w", err)
	}

	peers := make([]string, 0, len(replicas))
	for _, n := range replicas {
		if n != o.nodeName {
			peers = append(peers, n)
		}
	}
	if len(peers) == 0 {
		// Only us per schema: nothing to recover from.
		return probeDecision{action: actionEmptyFallback, probedPeers: nil}, nil
	}

	// Shuffle so different recovering nodes don't all pick the same
	// peer if multiple report data. The RNG is seeded from crypto/rand
	// (see cryptoSeed) so nodes shuffle independently; the shuffle
	// itself is non-security-sensitive load balancing — math/rand is
	// the appropriate primitive.
	o.rngMu.Lock()
	o.rng.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	o.rngMu.Unlock()

	type probeResult struct {
		peer       string
		hasData    bool
		definitive bool
		err        error
	}
	results := make([]probeResult, len(peers))
	var wg sync.WaitGroup
	for i, peer := range peers {
		i, peer := i, peer
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			h, d, e := o.probePeer(ctx, peer, ref)
			results[i] = probeResult{peer: peer, hasData: h, definitive: d, err: e}
		}, o.logger)
	}
	wg.Wait()

	var (
		probedPeers        []string
		anyDefinitiveEmpty bool
		anyUnreachable     bool
		firstSource        string
	)
	for _, r := range results {
		probedPeers = append(probedPeers, r.peer)
		if r.err != nil {
			anyUnreachable = true
			if o.metrics != nil {
				o.metrics.UnreachablePeerTotal.WithLabelValues(r.peer).Inc()
			}
			o.logger.WithError(r.err).WithFields(logrus.Fields{
				"event":      "self_recovery.peer_probe",
				"collection": ref.Collection,
				"shard":      ref.Shard,
				"peer":       r.peer,
				"result":     "unreachable",
			}).Debug("peer probe failed")
			continue
		}
		if r.hasData && firstSource == "" {
			firstSource = r.peer
		}
		if r.definitive && !r.hasData {
			anyDefinitiveEmpty = true
		}
	}

	if firstSource != "" {
		return probeDecision{
			action:      actionRegisterOp,
			sourceNode:  firstSource,
			probedPeers: probedPeers,
		}, nil
	}
	if anyUnreachable {
		// Don't silently create empty when probes were inconclusive.
		return probeDecision{action: actionRetry, probedPeers: probedPeers}, nil
	}
	if anyDefinitiveEmpty {
		return probeDecision{action: actionEmptyFallback, probedPeers: probedPeers}, nil
	}
	return probeDecision{action: actionRetry, probedPeers: probedPeers}, nil
}

// probePeer reports whether peer has data for the shard. definitive=true
// means the peer answered (with or without data); err != nil means
// transport/timeout — caller should retry later, not fall through.
func (o *Orchestrator) probePeer(ctx context.Context, peer string, ref ShardRef) (hasData bool, definitive bool, err error) {
	addr := o.nodeSelector.NodeAddress(peer)
	if addr == "" {
		return false, false, fmt.Errorf("no address for peer %q", peer)
	}
	port, err := o.nodeSelector.NodeGRPCPort(peer)
	if err != nil {
		return false, false, fmt.Errorf("get gRPC port for peer %q: %w", peer, err)
	}

	probeCtx, cancel := context.WithTimeout(ctx, o.probeTimeout)
	defer cancel()

	client, err := o.clientFactory(probeCtx, net.JoinHostPort(addr, fmt.Sprintf("%d", port)))
	if err != nil {
		return false, false, fmt.Errorf("connect to peer %q: %w", peer, err)
	}

	resp, err := client.ListFiles(probeCtx, &protocol.ListFilesRequest{
		IndexName: ref.Collection,
		ShardName: ref.Shard,
	})
	if err != nil {
		// gRPC status drives the decision: NotFound = definitive
		// no-data; Unavailable = peer itself recovering / busy →
		// transient. Anything else is treated as transport error.
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.NotFound:
				return false, true, nil
			case codes.Unavailable:
				return false, false, fmt.Errorf("peer %q unavailable: %w", peer, err)
			default:
				// other codes fall through to substring fallback / generic
			}
		}
		// Older peers don't carry typed codes — fall back to
		// substring matching so a rolling upgrade doesn't break.
		if isShardAbsentErr(err) {
			return false, true, nil
		}
		// "not paused for transfer" means the shard exists on the
		// peer; ListFiles refuses without a PauseFileActivity first.
		// The probe is read-only and only needs a yes/no, so treat
		// this as a positive (has-data) answer. The consumer pauses
		// the source itself when the SELF_RECOVERY op runs.
		if isShardPresentButNotPausedErr(err) {
			return true, true, nil
		}
		return false, false, fmt.Errorf("list files on peer %q: %w", peer, err)
	}
	if resp == nil || len(resp.FileNames) == 0 {
		return false, true, nil
	}
	return true, true, nil
}

// isShardAbsentErr is the rolling-upgrade fallback for peers that
// haven't yet been upgraded to send typed gRPC codes. Match only
// shard-specific phrasings (the canonical phrasing on the index layer
// is "shard not found" / "shard is nil") — a bare "not found" substring
// would misclassify unrelated errors (e.g. "file X not found") and push
// the orchestrator into the empty-fallback branch.
func isShardAbsentErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "incoming list files get shard is nil"):
		return true
	case strings.Contains(msg, "shard is nil"):
		return true
	case strings.Contains(msg, "shard not found"):
		return true
	}
	return false
}

// isShardPresentButNotPausedErr matches the ListFiles error returned
// when the shard exists but file activity has not been paused. The
// probe doesn't pause (the consumer does, when the op runs), so this
// is a positive answer: the source has the shard.
func isShardPresentButNotPausedErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "is not paused for transfer")
}

// registerAndPoll registers a SELF_RECOVERY op and polls the FSM until
// terminal state. nil on READY, error on CANCELLED or poll failure.
// Returns ErrReplicationOperationNotFound if the op vanished from the
// FSM (e.g. operator-driven force-delete after class/tenant deletion).
func (o *Orchestrator) registerAndPoll(ctx context.Context, ref ShardRef, sourceNode string) error {
	uuid, err := o.raft.RegisterSelfRecovery(ctx, sourceNode, ref.Collection, ref.Shard, o.nodeName)
	if err != nil {
		return fmt.Errorf("register self-recovery op: %w", err)
	}

	o.logger.WithFields(logrus.Fields{
		"event":       "self_recovery.op_registered",
		"collection":  ref.Collection,
		"shard":       ref.Shard,
		"source_node": sourceNode,
		"op_uuid":     uuid,
	}).Info("self-recovery op registered; polling for completion")

	// "transient" failures don't necessarily mean the op is gone — they
	// can happen during leader change. We tolerate a bounded number of
	// consecutive not-found errors before concluding the op was force-
	// deleted (class/tenant removed, manual ForceDelete*, etc.).
	const notFoundThreshold = 3
	notFoundCount := 0

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			details, err := o.raft.GetReplicationDetailsByReplicationId(ctx, uuid)
			if err != nil {
				if errors.Is(err, replicationtypes.ErrReplicationOperationNotFound) {
					notFoundCount++
					if notFoundCount >= notFoundThreshold {
						return fmt.Errorf("self-recovery op %s vanished from FSM (force-deleted upstream): %w", uuid, replicationtypes.ErrReplicationOperationNotFound)
					}
				}
				continue // transient (e.g. leader change) — keep polling
			}
			notFoundCount = 0
			switch api.ShardReplicationState(details.Status.State) {
			case api.READY:
				return nil
			case api.CANCELLED:
				return fmt.Errorf("self-recovery op %s: %w", uuid, ErrSelfRecoveryCancelled)
			case api.REGISTERED, api.HYDRATING, api.FINALIZING, api.DEHYDRATING:
				// non-terminal — keep polling
			}
		}
	}
}

// emptyFallback creates an empty live shard dir; reached only when all
// probed peers were reachable and definitively reported no data.
func (o *Orchestrator) emptyFallback(ref ShardRef) error {
	if o.pathResolver == nil {
		return errors.New("empty-fallback: no PathResolver configured")
	}
	dir := o.pathResolver.ShardPath(ref.Collection, ref.Shard)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", dir, err)
	}
	if err := diskio.Fsync(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("fsync parent of %q: %w", dir, err)
	}
	return nil
}

// initPool spawns the worker pool on first Submit. Worker count =
// Config.Concurrency (default 1). The buffered queue absorbs bursts up
// to submitQueueCapacity; beyond that, Submit drops with a warning.
func (o *Orchestrator) initPool() {
	n := 1
	if o.concurrency != nil {
		if v := o.concurrency.Get(); v > 0 {
			n = v
		}
	}
	o.workQueue = make(chan submission, submitQueueCapacity)
	for i := 0; i < n; i++ {
		enterrors.GoWrapper(func() {
			for sub := range o.workQueue {
				o.runOne(sub.ctx, sub.ref)
			}
		}, o.logger)
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
