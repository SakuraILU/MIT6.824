package kvraft

const (
	GET    string = "Get"
	PUT           = "Put"
	APPEND        = "Append"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Id of a command, including the clerkId and the sequence number
type OpId struct {
	ClerkId int64
	SeqId   int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	SeqId   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId int64
	SeqId   int64
}

type GetReply struct {
	Err   Err
	Value string
}
