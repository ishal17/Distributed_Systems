package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	client  int64
	request int
	mu      sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.client = nrand()
	ck.request = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	var aArgs PutAppendArgs // dummy
	ck.mu.Lock()
	gArgs := GetArgs{Key: key, Client: ck.client, Request: ck.request}
	ck.request++
	ck.mu.Unlock()

	ok := false
	res := ""
	for !ok {
		ok, res = ck.CallServers("KVServer.Get", gArgs, aArgs)
	}
	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var gArgs GetArgs // dummy
	ck.mu.Lock()
	aArgs := PutAppendArgs{Key: key, Value: value, Op: op, Client: ck.client, Request: ck.request}
	ck.request++
	ck.mu.Unlock()

	ok := false
	for !ok {
		ok, _ = ck.CallServers("KVServer.PutAppend", gArgs, aArgs)
	}
}

func (ck *Clerk) CallServers(command string, gArgs GetArgs, aArgs PutAppendArgs) (bool, string) {
	if command == "KVServer.Get" {
		for _, c := range ck.servers {
			var reply GetReply
			ok := c.Call(command, &gArgs, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return true, reply.Value
			}
		}
	} else {
		for _, c := range ck.servers {
			var reply PutAppendReply
			ok := c.Call(command, &aArgs, &reply)
			if ok && reply.Err != ErrWrongLeader {
				return true, ""
			}
		}
	}
	return false, ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
