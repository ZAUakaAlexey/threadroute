package record

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

// Reader reads the complete segments of a recording directory in order.
// Temporary segments are skipped.
type Reader struct {
	dir string
}

// NewReader returns a Reader for dir.
func NewReader(dir string) *Reader {
	return &Reader{dir: dir}
}

// Segments returns the paths of the complete segments, oldest first.
func (r *Reader) Segments() ([]string, error) {
	infos, err := listSegments(r.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, info := range infos {
		if !info.temp {
			out = append(out, filepath.Join(r.dir, info.name))
		}
	}
	return out, nil
}

// All yields every entry of every complete segment in order. After the first
// error it yields that error and stops. The error for a corrupt segment
// names the file and the line, but never includes the line content.
func (r *Reader) All() iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		paths, err := r.Segments()
		if err != nil {
			yield(Entry{}, err)
			return
		}
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			yield(Entry{}, fmt.Errorf("create zstd decoder: %w", err))
			return
		}
		defer dec.Close()
		for _, path := range paths {
			if !readSegment(dec, path, yield) {
				return
			}
		}
	}
}

// readSegment yields the entries of one segment. It returns false when the
// caller stopped or an error was yielded.
func readSegment(dec *zstd.Decoder, path string, yield func(Entry, error) bool) bool {
	f, err := os.Open(path)
	if err != nil {
		yield(Entry{}, fmt.Errorf("open segment %s: %w", path, err))
		return false
	}
	defer f.Close()
	if err := dec.Reset(f); err != nil {
		yield(Entry{}, fmt.Errorf("segment %s: %w", path, err))
		return false
	}
	br := bufio.NewReaderSize(dec, 256<<10)
	for line := 1; ; line++ {
		b, err := br.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(b) > 0 {
				yield(Entry{}, fmt.Errorf("segment %s line %d: last line has no newline", path, line))
				return false
			}
			return true
		}
		if err != nil {
			yield(Entry{}, fmt.Errorf("segment %s line %d: read: %w", path, line, err))
			return false
		}
		b = bytes.TrimSuffix(b, []byte{'\n'})
		e, err := unmarshalLine(b)
		if err != nil {
			yield(Entry{}, fmt.Errorf("segment %s line %d: %w", path, line, err))
			return false
		}
		if !yield(e, nil) {
			return false
		}
	}
}
