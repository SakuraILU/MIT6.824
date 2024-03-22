package raft

type LogEntry struct {
	Term    int         // 该条日志的Term
	Index   int         // 该条日志的Index
	Command interface{} // 该条日志的命令
}

type Logs struct {
	Entries           []*LogEntry
	LastIncludedTerm  int
	LastIncludedIndex int
}

func MakeLogs(lastIncludedIndex, lastIncludedTerm int) *Logs {
	logs := make([]*LogEntry, 0)
	logs = append(logs, &LogEntry{Term: lastIncludedTerm, Index: lastIncludedIndex})
	return &Logs{
		Entries:           logs,
		LastIncludedTerm:  lastIncludedTerm,
		LastIncludedIndex: lastIncludedIndex,
	}
}

func (l *Logs) realIndex(index int) int {
	return index - l.LastIncludedIndex
}

func (l *Logs) getLogLength() int {
	return l.LastIncludedIndex + len(l.Entries)
}

func (l *Logs) getLog(index int) *LogEntry {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic("Invalid index")
	}
	return l.Entries[realIndex]
}

func (l *Logs) getLogsFrom(index int) []*LogEntry {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic("Invalid index")
	}
	return l.Entries[realIndex:]
}

func (l *Logs) appendLog(entry *LogEntry) {
	l.Entries = append(l.Entries, entry)
}

func (l *Logs) appendLogs(index int, entries []*LogEntry) {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic("Invalid index")
	}

	l.Entries = append(l.Entries[:realIndex], entries...)
}

func (l *Logs) getLastIndex() int {
	return l.Entries[len(l.Entries)-1].Index
}

func (l *Logs) getLastTerm() int {
	return l.Entries[len(l.Entries)-1].Term
}

func (l *Logs) getLastIncludedIndex() int {
	return l.LastIncludedIndex
}

func (l *Logs) getLastIncludedTerm() int {
	return l.LastIncludedTerm
}

func (l *Logs) truncLogBefore(index int) {
	realIndex := l.realIndex(index)
	if realIndex < 0 {
		panic("Invalid index")
	}

	newEntries := make([]*LogEntry, 0)
	newEntries = append(newEntries, l.Entries[realIndex:]...)
	l.Entries = newEntries
	l.LastIncludedTerm = l.Entries[0].Term
	l.LastIncludedIndex = l.Entries[0].Index

	DPrintf("InstallSnapshot: %d", l.LastIncludedIndex)
}
