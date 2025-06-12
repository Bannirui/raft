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

	pb "go.etcd.io/raft/v3/raftpb"
)

// Bootstrap initializes the RawNode for first use by appending configuration
// changes for the supplied peers. This method returns an error if the Storage
// is nonempty.
//
// It is recommended that instead of calling this method, applications bootstrap
// their state manually by setting up a Storage that has a first index > 1 and
// which stores the desired ConfState as its InitialState.
// 方法的名称可能有误导 并不是真正的启动节点
// raft是典型的EDA架构 log entry就是线程事件循环的事件源
// 在系统最开始启动时 磁盘上是干净的 没有任何log entry 所以这个地方仅仅在给启动做准备工作 人为构建log entry 所以这个方法就做了两件事
// 1 初始化节点角色是Follower 等心跳超时事件进来 发现自己是Follower 触发选主
// 2 造日志entry 在选主流程中要用到
func (rn *RawNode) Bootstrap(peers []Peer) error {
	if len(peers) == 0 {
		return errors.New("must provide at least one peer to Bootstrap")
	}
	// 在初始化storage的时候会默认放上一个哨兵 它的index=0 这个地方拿到的lastIndex=0 语义是在storage#ents中最后一条数据的index
	lastIndex, err := rn.raft.raftLog.storage.LastIndex()
	if err != nil {
		return err
	}

	if lastIndex != 0 {
		// 这个地方为什么要这样校验呢 因为能调用这个方法的地方只有一个 就是raft启动时候节点机器是干净的 没有snap 也没有wal 需要手工造日志entry 让后面的选主流程能走下去
		return errors.New("can't bootstrap a nonempty Storage")
	}

	// We've faked out initial entries above, but nothing has been
	// persisted. Start with an empty HardState (thus the first Ready will
	// emit a HardState update for the app to persist).
	rn.prevHardSt = emptyState

	// TODO(tbg): remove StartNode and give the application the right tools to
	// bootstrap the initial membership in a cleaner way.
	// 角色初始化为Follower 为什么是Follower 因为心跳超时事件后发现自己是Follower就会触发选主
	rn.raft.becomeFollower(1, None)
	ents := make([]pb.Entry, len(peers))
	for i, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		data, err := cc.Marshal()
		if err != nil {
			return err
		}

		ents[i] = pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: uint64(i + 1), Data: data}
	}
	rn.raft.raftLog.append(ents...)

	// Now apply them, mainly so that the application can call Campaign
	// immediately after StartNode in tests. Note that these nodes will
	// be added to raft twice: here and when the application's Ready
	// loop calls ApplyConfChange. The calls to addNode must come after
	// all calls to raftLog.append so progress.next is set after these
	// bootstrapping entries (it is an error if we try to append these
	// entries since they have already been committed).
	// We do not set raftLog.applied so the application will be able
	// to observe all conf changes via Ready.CommittedEntries.
	//
	// TODO(bdarnell): These entries are still unstable; do we need to preserve
	// the invariant that committed < unstable?
	rn.raft.raftLog.committed = uint64(len(ents))
	for _, peer := range peers {
		rn.raft.applyConfChange(pb.ConfChange{NodeID: peer.ID, Type: pb.ConfChangeAddNode}.AsV2())
	}
	return nil
}
