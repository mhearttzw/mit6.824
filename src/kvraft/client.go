package raftkv

import (
	"github.com/mhearttzw/mit/src/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

// You will probably have to modify your Clerk to remember which server turned out to be the leader for the last RPC,
// and send the next RPC to that server first.
// This will avoid wasting time searching for the leader on every RPC, which may help you pass some of the tests quickly enough.
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64	// unique identifier
	lastLeaderId int
	seqNum int	// msg unique identifier
	mu sync.Mutex
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
	// You'll have to add code here.
	ck.cid = nrand()
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

	// You will have to modify this function.
	args := &GetArgs{
		Key:key,
	}
	idx := ck.lastLeaderId
	for {
		reply := &GetReply{}
		ok := ck.servers[idx].Call("KVServer.Get", args, reply)
		if ok && !reply.WrongLeader {
			// todo how to deal with ERR
			ck.lastLeaderId = idx
			return reply.Value
		}
		idx = (idx + 1)%len(ck.servers)
		time.Sleep(time.Duration(50)*time.Millisecond)
	}

	return ""
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
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		Cid: ck.cid,
		SeqNum: ck.seqNum,
	}
	idx := ck.lastLeaderId
	for {
		reply := &PutAppendReply{}
		ck.mu.Lock()
		ck.seqNum++
		ck.mu.Unlock()
		//DPrintf("Client request server %v to call PutAppend method, args: %+v", idx, args)
		ok := ck.servers[idx].Call("KVServer.PutAppend", args, reply)
		//DPrintf("Client request server %v to call PutAppend method finished, reply: %+v", idx, reply)
		if ok && !reply.WrongLeader {
			// todo how to deal with ERR
			ck.lastLeaderId = idx
			//DPrintf("===================Client request server %v to call PutAppend method success!", idx)
			return
		}
		idx = (idx + 1)%len(ck.servers)
		time.Sleep(time.Duration(50)*time.Millisecond)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
