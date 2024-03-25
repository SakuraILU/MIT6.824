package raft

import "fmt"

type LogEntry struct {
	Term    int         // 该条日志的Term
	Index   int         // 该条日志的Index
	Command interface{} // 该条日志的命令
}

type Logs struct {
	Entries []*LogEntry
}

func MakeLogs(lastIncludedIndex, lastIncludedTerm int) *Logs {
	logs := make([]*LogEntry, 0)
	logs = append(logs, &LogEntry{Term: lastIncludedTerm, Index: lastIncludedIndex})
	return &Logs{
		Entries: logs,
	}
}

func (l *Logs) realIndex(index int) int {
	return index - l.getLastIncludedIndex()
}

func (l *Logs) getLogLength() int {
	return l.Entries[len(l.Entries)-1].Index + 1
}

func (l *Logs) getLog(index int) *LogEntry {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic(fmt.Sprintf("Invalid index: %d, valid range: [%d, %d]", index, l.getLastIncludedIndex(), l.getLogLength()))
	}
	return l.Entries[realIndex]
}

func (l *Logs) getLogsFrom(index int) []*LogEntry {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic(fmt.Sprintf("Invalid index: %d, valid range: [%d, %d]", index, l.getLastIncludedIndex(), l.getLogLength()))
	}
	return l.Entries[realIndex:]
}

func (l *Logs) appendLog(entry *LogEntry) {
	if entry.Index != l.getLastIndex()+1 {
		panic(fmt.Sprintf("Invalid index: %d, valid index: %d", entry.Index, l.getLastIndex()+1))
	}

	l.Entries = append(l.Entries, entry)
}

func (l *Logs) appendLogs(index int, entries []*LogEntry) {
	if len(entries) == 0 {
		return
	}

	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic(fmt.Sprintf("Invalid index: %d, valid range: [%d, %d]", index, l.getLastIncludedIndex(), l.getLogLength()))
	}
	// check index
	if index <= l.getLastIndex() && l.Entries[realIndex].Index != index {
		panic(fmt.Sprintf("Invalid index: %d, valid index: %d", index, l.Entries[realIndex].Index))
	}

	// l.Entries = append(l.Entries[:realIndex], entries...)

	// 追加或覆盖日志条目
	// a important bug!
	// 这里可能存在过时RPC！先来新的logs，再来旧的logs，这时候就不能直接截断然后append了，
	// 否则新的logs会被截断掉，导致日志变成旧版本，长度减小，这是不对的！
	// 这个bug是在Snapshot(index, snapshot)中发现的，理论上用户只会Snapshot lastApplied之前的logs
	// 但是却发生了index >= logs.getLogLength()的情况...怀疑是logs安装的问题，加了一些panic后发现
	// commitIndex和logs一会儿大一会儿小 =》过时RPC
	//
	// 修复这个bug，遍历entries，逐个比较，如果任期不匹配，删除这个索引及之后的所有条目
	// 类似于TCP中的receiver的整流器
	startIndex := realIndex
	for i, entry := range entries {
		if startIndex+i < len(l.Entries) {
			if l.Entries[startIndex+i].Term != entry.Term {
				// 如果任期不匹配，删除这个索引及之后的所有条目
				l.Entries = l.Entries[:startIndex+i]
			}
		}
		if startIndex+i >= len(l.Entries) {
			// 如果当前索引超出现有日志长度，追加新条目
			l.Entries = append(l.Entries, entries[i:]...)
			break
		}
	}
}

func (l *Logs) getLastIndex() int {
	return l.Entries[len(l.Entries)-1].Index
}

func (l *Logs) getLastTerm() int {
	return l.Entries[len(l.Entries)-1].Term
}

func (l *Logs) getLastIncludedIndex() int {
	return l.Entries[0].Index
}

func (l *Logs) getLastIncludedTerm() int {
	return l.Entries[0].Term
}

func (l *Logs) truncLogBefore(index int) {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic("Invalid index")
	}

	newEntries := make([]*LogEntry, 0)
	newEntries = append(newEntries, l.Entries[realIndex:]...)
	l.Entries = newEntries

	DPrintf("InstallSnapshot: %d", index)
}
