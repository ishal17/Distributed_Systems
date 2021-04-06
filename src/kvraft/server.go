package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0
const Timeout = 900

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	K       string
	V       string
	Client  int64
	Request int
	OpType  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientRequests map[int64]int
	keyValuePairs  map[string]string
	commits        map[int]chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op1 := Op{K: args.Key, Client: args.Client, Request: args.Request, OpType: "Get"}
	index, _, isLeader := kv.rf.Start(op1)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChan(index)

	select {
	case <-time.After(Timeout * time.Millisecond):
		reply.Err = ErrWrongLeader
	case op := <-ch:
		if op != op1 {
			reply.Err = ErrWrongLeader
		} else {
			kv.mu.Lock()
			v, ok := kv.keyValuePairs[args.Key]
			if ok {
				kv.clientRequests[args.Client] = args.Request
				reply.Value = v
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op1 := Op{K: args.Key, V: args.Value, Client: args.Client, Request: args.Request, OpType: args.Op}
	index, _, isLeader := kv.rf.Start(op1)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChan(index)

	select {
	case <-time.After(Timeout * time.Millisecond):
		reply.Err = ErrWrongLeader
	case op := <-ch:
		if op != op1 {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	}
}

func (kv *KVServer) getChan(index int) chan Op {
	kv.mu.Lock()
	ch, ok := kv.commits[index]
	if ok {
		kv.mu.Unlock()
		return ch
	}
	ch = make(chan Op, 1)
	kv.commits[index] = ch
	kv.mu.Unlock()
	return ch
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.clientRequests = make(map[int64]int)
	kv.keyValuePairs = make(map[string]string)
	kv.commits = make(map[int]chan Op)

	go kv.Serve()

	return kv
}

func (kv *KVServer) Serve() {
	for {
		message := <-kv.applyCh
		op := message.Command.(Op)

		ch, ok := kv.commits[message.CommandIndex]
		if ok {
			ch <- op
		} else {
			kv.commits[message.CommandIndex] = make(chan Op, 1)
		}

		kv.mu.Lock()
		v, ok := kv.clientRequests[op.Client]
		if ok && v >= op.Request {
			kv.mu.Unlock()
			continue
		}

		kv.clientRequests[op.Client] = op.Request
		if op.OpType == "Put" {
			kv.keyValuePairs[op.K] = op.V
		} else {
			kv.keyValuePairs[op.K] += op.V
		}
		kv.mu.Unlock()
	}
}
