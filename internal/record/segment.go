package record

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Segment file names.
//
// A complete segment is named seg-<start>-<seq>.ndjson.zst, where <start> is
// the UTC time the segment was opened (20261123T205500.123456Z) and <seq> is
// a zero-padded sequence number. Both parts have a fixed width, so names sort
// by time and then by sequence. A segment that is still being written has the
// extra suffix .tmp.
const (
	segmentPrefix = "seg-"
	segmentSuffix = ".ndjson.zst"
	tempSuffix    = ".tmp"
	timeLayout    = "20060102T150405.000000Z"
	seqWidth      = 10
	filePerm      = 0o600
	dirPerm       = 0o700
)

// segmentName returns the complete name of a segment.
func segmentName(start time.Time, seq uint64) string {
	return fmt.Sprintf("%s%s-%0*d%s", segmentPrefix, start.UTC().Format(timeLayout), seqWidth, seq, segmentSuffix)
}

// segmentInfo is a parsed segment name.
type segmentInfo struct {
	name  string
	start time.Time
	seq   uint64
	temp  bool
}

// parseSegmentName parses a complete or temporary segment name. It returns
// false for any other file.
func parseSegmentName(name string) (segmentInfo, bool) {
	info := segmentInfo{name: name}
	base := name
	if s, ok := strings.CutSuffix(base, tempSuffix); ok {
		base, info.temp = s, true
	}
	base, ok := strings.CutSuffix(base, segmentSuffix)
	if !ok {
		return segmentInfo{}, false
	}
	base, ok = strings.CutPrefix(base, segmentPrefix)
	if !ok {
		return segmentInfo{}, false
	}
	ts, seq, ok := strings.Cut(base, "-")
	if !ok || len(ts) != len(timeLayout) || len(seq) != seqWidth {
		return segmentInfo{}, false
	}
	start, err := time.Parse(timeLayout, ts)
	if err != nil {
		return segmentInfo{}, false
	}
	n, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return segmentInfo{}, false
	}
	info.start, info.seq = start, n
	return info, true
}

// listSegments returns the segment files in dir, sorted by name. It returns
// complete and temporary segments, and ignores every other file.
func listSegments(dir string) ([]segmentInfo, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("list segments in %s: %w", dir, err)
	}
	var out []segmentInfo
	for _, de := range des {
		if !de.Type().IsRegular() {
			continue
		}
		if info, ok := parseSegmentName(de.Name()); ok {
			out = append(out, info)
		}
	}
	slices.SortFunc(out, func(a, b segmentInfo) int { return strings.Compare(a.name, b.name) })
	return out, nil
}
