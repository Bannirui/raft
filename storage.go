// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"sync"

	pb "go.etcd.io/raft/v3/raftpb"
)

// ErrCompacted is returned by Storage.Entries/Compact when a requested
// index is unavailable because it predates the last snapshot.
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
// index is older than the existing snapshot.
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// ErrUnavailable is returned by Storage interface when the requested log entries
// are unavailable.
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
// snapshot is temporarily unavailable.
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
type Storage interface {
	// TODO(tbg): split this into two interfaces, LogStorage and StateStorage.

	// InitialState returns the saved HardState and ConfState information.
	InitialState() (pb.HardState, pb.ConfState, error)

	// Entries returns a slice of consecutive log entries in the range [lo, hi),
	// starting from lo. The maxSize limits the total size of the log entries
	// returned, but Entries returns at least one entry if any.
	//
	// The caller of Entries owns the returned slice, and may append to it. The
	// individual entries in the slice must not be mutated, neither by the Storage
	// implementation nor the caller. Note that raft may forward these entries
	// back to the application via Ready struct, so the corresponding handler must
	// not mutate entries either (see comments in Ready struct).
	//
	// Since the caller may append to the returned slice, Storage implementation
	// must protect its state from corruption that such appends may cause. For
	// example, common ways to do so are:
	//  - allocate the slice before returning it (safest option),
	//  - return a slice protected by Go full slice expression, which causes
	//  copying on appends (see MemoryStorage).
	//
	// Returns ErrCompacted if entry lo has been compacted, or ErrUnavailable if
	// encountered an unavailable entry in [lo, hi).
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)

	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (pb.Snapshot, error)
}

type inMemStorageCallStats struct {
	initialState, firstIndex, lastIndex, entries, term, snapshot int
}

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
type MemoryStorage struct {
	// Protects access to all fields. Most methods of MemoryStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	sync.Mutex

	hardState pb.HardState
	// 记录是最新的snap 如果是启动恢复就记录用来恢复的那个snap
	snapshot  pb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	// 这个里面放在就是所有没有被snap删除的日志 一定为有个哨兵
	// 启动时没有snap 也没有wal 那么哨兵就是(term=0 index=0)
	// 启动时有snap snap的term和index会被封装成哨兵放在首位(term=snap的term index=snap的index)
	// 启动时没有wal 此时里面还是就躺着哨兵
	// 启动时有wal 里面除了哨兵 如实放着wal里面的日志
	ents []pb.Entry

	callStats inMemStorageCallStats
}

// NewMemoryStorage creates an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		// When starting from scratch populate the list with a dummy entry at term zero.
		// 在用snap和wal恢复数据前 会初始化一个Storage 在初始化的时候就保证了ents不空 避免了新系统没有历史数据还要判空的场景
		ents: make([]pb.Entry, 1),
	}
}

// InitialState implements the Storage interface.
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	ms.callStats.initialState++
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

// SetHardState saves the current HardState.
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.entries++
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		getLogger().Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := limitSize(ms.ents[lo-offset:hi-offset], entryEncodingSize(maxSize))
	// NB: use the full slice expression to limit what the caller can do with the
	// returned slice. For example, an append will reallocate and copy this slice
	// instead of corrupting the neighbouring ms.ents.
	return ents[:len(ents):len(ents)], nil
}

// Term implements the Storage interface.
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.term++
	offset := ms.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}

// LastIndex implements the Storage interface.
func (ms *MemoryStorage) LastIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.lastIndex++
	return ms.lastIndex(), nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	// 能直接用脚标直接索引 说明虽然系统刚启动 但是在MemoryStorage#ents中已经有了数据 说明raft做了边界处理 为了省去判空和数组越界 它初始化了哨兵数据
	return ms.ents[0].Index + uint64(len(ms.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.firstIndex++
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	ms.callStats.snapshot++
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// 用snap恢复数据
// 在raft中几乎只要有数据的地方 为了避免对边界的考虑 都会放置一个哨兵 snap也不例外 即使没有snap文件 也会创建个内存上的概念snap 它的term是0 index是0
// snap有效的话 恢复完后storage#ents的哨兵就是(term=snap的term index=snap的index)
// snap无效的话 恢复完后storage#ents的哨兵依然是之前storage初始化的(term=0 index=0)
// @Param snap 准备用来恢复数据的snap
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	// handle check for old snapshot being applied
	// storage初始化的时候只会在ents中放一个(term=0 index=0)的哨兵 其他的都是默认初始化值 那么这个ms.snapshot.Metadata.Index就是0
	// 它的语义是storage的最新的一个snap
	msIndex := ms.snapshot.Metadata.Index
	// 用来恢复数据的snap文件可能存在 可能不存在 如果不存在 此时的snap就是哨兵 它的index就是默认值0
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		// 为什么做这个 上一个snap的index肯定是<准备用的snap的index 为什么有=呢 如果上一个snap和这一个相等就没必要用来作恢复 效果不变
		// 所以在初始启动时候 没有snap文件场景下 会走到这
		// 那么此时storage#ents中还是只有一个哨兵(term=0 index=0)
		return ErrSnapOutOfDate
	}
	// snap是有效的 用它来恢复数据
	ms.snapshot = snap
	// 此时storage#ents的哨兵就变了(term=0 index=0)->(term=snap的term index=snap的index)
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	if i > ms.lastIndex() {
		getLogger().Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > ms.lastIndex() {
		getLogger().Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	// NB: allocate a new slice instead of reusing the old ms.ents. Entries in
	// ms.ents are immutable, and can be referenced from outside MemoryStorage
	// through slices returned by ms.Entries().
	ents := make([]pb.Entry, 1, uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
// 在做wal回放的时候会调到这
// @Param entries wal中的数据
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()
	// storage中第一个index raft已经做了兼容考虑 不管有没有snap文件 这个地方都能取到正确的值
	// 如果空机器启动啥也没的时候 这个地方拿到的first就是1
	first := ms.firstIndex()
	// 要回放的最后一个index
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		// 要回放的都已经在storage中了
		return nil
	}
	// truncate compacted entries
	if first > entries[0].Index {
		// 要回放的数据有部分已经存在storage中了 做截断
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// NB: full slice expression protects ms.ents at index >= offset from
		// rewrites, as they may still be referenced from outside MemoryStorage.
		ms.ents = append(ms.ents[:offset:offset], entries...)
	case uint64(len(ms.ents)) == offset:
		ms.ents = append(ms.ents, entries...)
	default:
		getLogger().Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}
