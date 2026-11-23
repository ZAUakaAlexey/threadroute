package record

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
)

// Writer defaults.
const (
	DefaultSegmentMaxBytes int64 = 256 << 20 // 256 MiB, uncompressed
	DefaultSegmentMaxAge         = time.Hour
	// DefaultRetention is the lifetime of a recording from decision 0004.
	DefaultRetention = 14 * 24 * time.Hour
	// DefaultCheckInterval is how often the Writer closes an old segment and
	// applies retention when no entries arrive.
	DefaultCheckInterval = time.Minute
)

// ErrClosed is returned by Writer methods after Close.
var ErrClosed = errors.New("record writer is closed")

// WriterOptions configure a Writer. A zero value takes the default.
type WriterOptions struct {
	// Dir is the output directory. It is created with mode 0700. It must not
	// be inside a git working tree.
	Dir string
	// SegmentMaxBytes closes a segment once this many uncompressed bytes
	// were written to it.
	SegmentMaxBytes int64
	// SegmentMaxAge closes a segment once it is this old.
	SegmentMaxAge time.Duration
	// Retention deletes complete segments whose start time is older than
	// this. The start time is the time in the file name.
	Retention time.Duration
	// MaxTotalBytes, when above zero, deletes the oldest complete segments
	// until the files in Dir take no more than this many bytes on disk.
	MaxTotalBytes int64
	// CheckInterval is how often a background goroutine closes an old
	// segment and applies retention. It uses real time.
	CheckInterval time.Duration
	// Now returns the current time. Tests inject a fake clock here.
	Now func() time.Time
}

func (o WriterOptions) withDefaults() WriterOptions {
	if o.SegmentMaxBytes == 0 {
		o.SegmentMaxBytes = DefaultSegmentMaxBytes
	}
	if o.SegmentMaxAge == 0 {
		o.SegmentMaxAge = DefaultSegmentMaxAge
	}
	if o.Retention == 0 {
		o.Retention = DefaultRetention
	}
	if o.CheckInterval == 0 {
		o.CheckInterval = DefaultCheckInterval
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

func (o WriterOptions) validate() error {
	var errs []error
	if o.Dir == "" {
		errs = append(errs, errors.New("dir is empty"))
	}
	if o.SegmentMaxBytes < 0 {
		errs = append(errs, fmt.Errorf("segment max bytes %d is negative", o.SegmentMaxBytes))
	}
	if o.SegmentMaxAge < 0 {
		errs = append(errs, fmt.Errorf("segment max age %s is negative", o.SegmentMaxAge))
	}
	if o.Retention < 0 {
		errs = append(errs, fmt.Errorf("retention %s is negative", o.Retention))
	}
	if o.MaxTotalBytes < 0 {
		errs = append(errs, fmt.Errorf("max total bytes %d is negative", o.MaxTotalBytes))
	}
	if o.CheckInterval < 0 {
		errs = append(errs, fmt.Errorf("check interval %s is negative", o.CheckInterval))
	}
	return errors.Join(errs...)
}

// Writer appends entries to rotated segment files in one directory.
// All methods are safe for concurrent use.
type Writer struct {
	opts    WriterOptions
	logger  *slog.Logger
	metrics *writerMetrics

	mu      sync.Mutex
	closed  bool
	err     error // sticky error after a failed write, see fail
	cur     *segment
	enc     *zstd.Encoder
	nextSeq uint64
	lock    *os.File
	// complete holds, per partition, the highest offset that is stored in
	// a complete segment written by this Writer.
	complete map[int32]int64

	done chan struct{}
	wg   sync.WaitGroup
}

// segment is the open segment, written under its temporary name.
type segment struct {
	start     time.Time
	tempPath  string
	finalPath string
	file      *os.File
	out       *countingWriter
	rawBytes  int64
	// positions holds, per partition, the highest offset in this segment.
	positions map[int32]int64
}

// OpenWriter checks the directory, takes the directory lock, removes
// leftover temporary segments of a crashed run, applies retention once and
// starts the background check. The caller must call Close.
//
// Only one Writer may use a directory at a time. A second one fails with an
// error wrapping ErrDirLocked.
func OpenWriter(opts WriterOptions, logger *slog.Logger, reg prometheus.Registerer) (*Writer, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("open record writer: %w", err)
	}
	dir, err := filepath.Abs(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("open record writer: %w", err)
	}
	opts.Dir = dir
	if err := CheckNotInGitTree(dir); err != nil {
		return nil, fmt.Errorf("open record writer: %w", err)
	}
	if err := prepareDir(dir, logger); err != nil {
		return nil, fmt.Errorf("open record writer: %w", err)
	}
	// The lock is taken before temporary files are removed, so a second
	// writer never deletes the open segment of the first one.
	lock, err := lockDir(dir)
	if err != nil {
		return nil, fmt.Errorf("open record writer: %w", err)
	}
	fail := func(err error) (*Writer, error) {
		_ = unlockDir(lock)
		return nil, fmt.Errorf("open record writer: %w", err)
	}
	metrics, err := newWriterMetrics(reg)
	if err != nil {
		return fail(err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return fail(fmt.Errorf("create zstd encoder: %w", err))
	}

	w := &Writer{
		opts:     opts,
		logger:   logger,
		metrics:  metrics,
		enc:      enc,
		lock:     lock,
		complete: map[int32]int64{},
		done:     make(chan struct{}),
	}
	if err := w.cleanTemp(); err != nil {
		return fail(err)
	}
	w.mu.Lock()
	w.applyRetentionLocked()
	w.mu.Unlock()

	w.wg.Go(w.checkLoop)
	return w, nil
}

// prepareDir creates dir with mode 0700, or narrows the mode of an existing
// directory to 0700.
func prepareDir(dir string, logger *slog.Logger) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if fi.Mode().Perm() != dirPerm {
		if err := os.Chmod(dir, dirPerm); err != nil {
			return fmt.Errorf("set mode 0700 on %s: %w", dir, err)
		}
		logger.Warn("output directory mode changed to 0700", "dir", dir, "old_mode", fi.Mode().Perm().String())
	}
	return nil
}

// cleanTemp removes temporary segments left by a crash and finds the next
// sequence number.
//
// The Recorder commits offsets only for complete segments, so the records in
// a temporary segment were never committed and are read again from Kafka.
// Removing the file therefore loses nothing.
func (w *Writer) cleanTemp() error {
	infos, err := listSegments(w.opts.Dir)
	if err != nil {
		return err
	}
	for _, info := range infos {
		if info.seq >= w.nextSeq {
			w.nextSeq = info.seq + 1
		}
		if !info.temp {
			continue
		}
		path := filepath.Join(w.opts.Dir, info.name)
		var size int64
		if fi, err := os.Stat(path); err == nil {
			size = fi.Size()
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove leftover temporary segment %s: %w", path, err)
		}
		w.logger.Warn("removed leftover temporary segment of an earlier run", "file", info.name, "bytes", size)
	}
	return nil
}

// Append writes one entry to the open segment, opening or rotating segments
// as needed. An entry that cannot be stored returns an error that wraps
// ErrInvalidEntry, and the Writer stays usable.
func (w *Writer) Append(e Entry) error {
	line, err := e.marshalLine()
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	now := w.opts.Now()
	if w.cur != nil && now.Sub(w.cur.start) >= w.opts.SegmentMaxAge {
		if err := w.closeSegmentLocked(); err != nil {
			return err
		}
	}
	if w.cur == nil {
		if err := w.openSegmentLocked(now); err != nil {
			return err
		}
	}
	if _, err := w.enc.Write(line); err != nil {
		return w.failLocked(opAppend, fmt.Errorf("write to %s: %w", w.cur.tempPath, err))
	}
	w.cur.rawBytes += int64(len(line))
	if old, ok := w.cur.positions[e.Partition]; !ok || e.Offset > old {
		w.cur.positions[e.Partition] = e.Offset
	}
	w.metrics.entries.Inc()
	w.metrics.bytes.WithLabelValues(stageRaw).Add(float64(len(line)))

	if w.cur.rawBytes >= w.opts.SegmentMaxBytes {
		if err := w.closeSegmentLocked(); err != nil {
			return err
		}
		w.applyRetentionLocked()
	}
	return nil
}

// Flush makes every entry appended so far durable in the current segment
// file: it flushes the compressor and calls fsync. The segment stays open
// under its temporary name, so a flush does not make entries complete and
// is no reason to commit offsets.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	if w.cur == nil {
		return nil
	}
	if err := w.enc.Flush(); err != nil {
		return w.failLocked(opFlush, fmt.Errorf("flush %s: %w", w.cur.tempPath, err))
	}
	if err := w.cur.file.Sync(); err != nil {
		return w.failLocked(opFlush, fmt.Errorf("sync %s: %w", w.cur.tempPath, err))
	}
	return nil
}

// Rotate completes the open segment now, if there is one.
func (w *Writer) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	if w.cur == nil {
		return nil
	}
	return w.closeSegmentLocked()
}

// CompletePositions returns, per partition, the highest offset stored in a
// complete segment written by this Writer. Entries in the open segment are
// not included. It also works after Close.
func (w *Writer) CompletePositions() map[int32]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return maps.Clone(w.complete)
}

// Close completes the open segment, stops the background check, releases
// the directory lock and makes the Writer unusable. It is safe to call more than once and from any
// goroutine.
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.done)
	var err error
	if w.cur != nil && w.err == nil {
		err = w.closeSegmentLocked()
	} else if w.cur != nil {
		// The segment is broken. Leave the temporary file, the next open
		// removes it.
		_ = w.cur.file.Close()
		w.cur = nil
	}
	w.mu.Unlock()
	w.wg.Wait()
	if uerr := unlockDir(w.lock); uerr != nil && err == nil {
		err = uerr
	}
	if err != nil {
		return fmt.Errorf("close record writer: %w", err)
	}
	return nil
}

func (w *Writer) usableLocked() error {
	if w.closed {
		return ErrClosed
	}
	return w.err
}

// failLocked records a write error. After a failed write the compressed
// stream is in an unknown state, so every later call returns the same
// error and the temporary file is left for the next open to remove.
func (w *Writer) failLocked(op string, err error) error {
	w.metrics.errors.WithLabelValues(op).Inc()
	w.err = fmt.Errorf("record writer failed: %w", err)
	return w.err
}

func (w *Writer) openSegmentLocked(now time.Time) error {
	seq := w.nextSeq
	final := filepath.Join(w.opts.Dir, segmentName(now, seq))
	temp := final + tempSuffix
	f, err := os.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		w.metrics.errors.WithLabelValues(opRotate).Inc()
		return fmt.Errorf("create segment %s: %w", temp, err)
	}
	w.nextSeq++
	out := &countingWriter{w: f, counter: w.metrics.bytes.WithLabelValues(stageCompressed)}
	w.enc.Reset(out)
	w.cur = &segment{start: now, tempPath: temp, finalPath: final, file: f, out: out, positions: map[int32]int64{}}
	return nil
}

// closeSegmentLocked ends the zstd frame, syncs the file and renames it to
// its final name, so readers see it as complete.
func (w *Writer) closeSegmentLocked() error {
	s := w.cur
	if err := w.enc.Close(); err != nil {
		return w.failLocked(opRotate, fmt.Errorf("finish %s: %w", s.tempPath, err))
	}
	if err := s.file.Sync(); err != nil {
		return w.failLocked(opRotate, fmt.Errorf("sync %s: %w", s.tempPath, err))
	}
	if err := s.file.Close(); err != nil {
		return w.failLocked(opRotate, fmt.Errorf("close %s: %w", s.tempPath, err))
	}
	if err := os.Rename(s.tempPath, s.finalPath); err != nil {
		return w.failLocked(opRotate, fmt.Errorf("rename %s: %w", s.tempPath, err))
	}
	w.cur = nil
	if err := syncDir(w.opts.Dir); err != nil {
		// The rename may be lost on a power failure, so the positions of
		// this segment are not reported as complete and its offsets are
		// not committed. A restart then reads the records again: they can
		// appear twice, but they are never lost.
		w.metrics.errors.WithLabelValues(opRotate).Inc()
		w.logger.Warn("sync output directory failed", "dir", w.opts.Dir, "error", err)
	} else {
		for p, o := range s.positions {
			if old, ok := w.complete[p]; !ok || o > old {
				w.complete[p] = o
			}
		}
	}
	w.metrics.segments.Inc()
	w.logger.Info("segment complete", "file", filepath.Base(s.finalPath), "raw_bytes", s.rawBytes, "compressed_bytes", s.out.n.Load())
	return nil
}

// checkLoop closes an old segment and applies retention when no entries
// arrive. It stops on Close.
func (w *Writer) checkLoop() {
	t := time.NewTicker(w.opts.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-t.C:
			w.check()
		}
	}
}

// check runs one background check.
func (w *Writer) check() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.usableLocked() != nil {
		return
	}
	if w.cur != nil && w.opts.Now().Sub(w.cur.start) >= w.opts.SegmentMaxAge {
		if err := w.closeSegmentLocked(); err != nil {
			w.logger.Error("close old segment failed", "error", err)
			return
		}
	}
	w.applyRetentionLocked()
}

// applyRetentionLocked deletes complete segments that are too old, then the
// oldest complete segments while the total size is above the limit. The
// open segment is never deleted. Errors are logged and counted, because a
// failed delete is retried on the next check.
func (w *Writer) applyRetentionLocked() {
	infos, err := listSegments(w.opts.Dir)
	if err != nil {
		w.metrics.errors.WithLabelValues(opRetention).Inc()
		w.logger.Error("retention: list segments failed", "error", err)
		return
	}
	cutoff := w.opts.Now().Add(-w.opts.Retention)

	type file struct {
		info segmentInfo
		size int64
	}
	var complete []file
	var total int64
	for _, info := range infos {
		fi, err := os.Stat(filepath.Join(w.opts.Dir, info.name))
		if err != nil {
			continue
		}
		total += fi.Size()
		if !info.temp {
			complete = append(complete, file{info: info, size: fi.Size()})
		}
	}

	remove := func(f file, reason string) bool {
		if err := os.Remove(filepath.Join(w.opts.Dir, f.info.name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			w.metrics.errors.WithLabelValues(opRetention).Inc()
			w.logger.Error("retention: delete segment failed", "file", f.info.name, "error", err)
			return false
		}
		total -= f.size
		w.metrics.segmentsDeleted.WithLabelValues(reason).Inc()
		w.logger.Info("retention: segment deleted", "file", f.info.name, "reason", reason)
		return true
	}

	// complete is sorted oldest first.
	i := 0
	for ; i < len(complete) && complete[i].info.start.Before(cutoff); i++ {
		remove(complete[i], reasonAge)
	}
	if w.opts.MaxTotalBytes <= 0 {
		return
	}
	for ; i < len(complete) && total > w.opts.MaxTotalBytes; i++ {
		remove(complete[i], reasonSize)
	}
}

// countingWriter counts the compressed bytes written to the file.
type countingWriter struct {
	w       io.Writer
	n       atomic.Int64
	counter prometheus.Counter
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n.Add(int64(n))
	c.counter.Add(float64(n))
	return n, err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
