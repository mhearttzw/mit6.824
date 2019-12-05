package raftkv

import (
	"bytes"
	"github.com/mhearttzw/mit/src/labgob"
	"github.com/mhearttzw/mit/src/labrpc"
	"github.com/mhearttzw/mit/src/raft"
	"log"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

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
	Type  string
	Key   string
	Value string
	// You will need to uniquely identify client operations to ensure that the key/value service executes each one just once.
	Cid    int64 // clerk unique id
	SeqNum int   // op unique id, auto increment
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        map[string]string // store msg
	eleChMap  map[int]chan Op   // notify clerk that command has been applied into state machine
	maxSeqMap map[int64]int     // ensure client's msg orderly, key is unique clientId
	persister *raft.Persister          // Object to hold this peer's persisted state

}

// Hint: A kvserver should not complete a Get() RPC if it is not part of a majority (so that it does not serve stale data).
// A simple solution is to enter every Get() (as well as each Put() and Append()) in the Raft log.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{
		Type:   "Get",
		Key:    args.Key,
		Value:  strconv.FormatInt(nrand(), 10),
		Cid:    -1,
		SeqNum: -1,
	}
	//DPrintf("Server: %v, call Get method, command: %+v", kv.me, command)
	index, _, isLeader := kv.rf.Start(command)
	reply.WrongLeader = true
	if !isLeader {
		return
	}
	// obtain notify channel
	eleCh := kv.getEleCh(index, true)
	notifyOp := kv.beNotified(eleCh, index)
	if isOpEqual(command, notifyOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		reply.Value = kv.db[command.Key]
		kv.mu.Unlock()
	}
	return
}

// Hint:
// Your solution needs to handle the case in which a leader has called Start() for a Clerk's RPC,
// but loses its leadership before the request is committed to the log.
// In this case you should arrange for the Clerk to re-send the request to other servers until it finds the new leader.
// One way to do this is for the server to detect that it has lost leadership,
// by noticing that a different request has appeared at the index returned by Start(), or that Raft's term has changed.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{
		Type:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Cid:    args.Cid,
		SeqNum: args.SeqNum,
	}
	//DPrintf("Server: %v, execute PutAppend method just started, command: %+v", kv.me, command)
	index, _, isLeader := kv.rf.Start(command)
	//DPrintf("Server: %v, execute PutAppend method finished, index: %v, isLeader: %v", kv.me, index, isLeader)

	reply.WrongLeader = true
	if !isLeader {
		return
	}
	// obtain channel
	eleCh := kv.getEleCh(index, true)
	notifyOp := kv.beNotified(eleCh, index)
	//DPrintf("OriginOp: %+v, notifyOp: %+v", command, notifyOp)
	if isOpEqual(command, notifyOp) {
		reply.WrongLeader = false
	}
	return
}

func isOpEqual(originOp Op, committedOp Op) bool {
	return originOp.Type == committedOp.Type && originOp.Key == committedOp.Key && originOp.Value == committedOp.Value &&
		originOp.Cid == committedOp.Cid && originOp.SeqNum == committedOp.SeqNum
}

func (kv *KVServer) getEleCh(index int, createIfNotExist bool) (chan Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.eleChMap[index]; !ok {
		if !createIfNotExist {return nil}
		eleCh := make(chan Op, 1)
		kv.eleChMap[index] = eleCh
		return eleCh
	}
	//DPrintf("Oops, fail to get eleCh!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	return kv.eleChMap[index]
}

// notify clerk when command has been committed
func (kv *KVServer) beNotified(eleCh chan Op, index int) (notifyOp Op) {
	select {
	case notifyOp := <-eleCh:
		//DPrintf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>BeNotified method, receive notifyOp!")
		close(eleCh)
		kv.mu.Lock()
		delete(kv.eleChMap, index)
		kv.mu.Unlock()
		return notifyOp
	case <-time.After(600 * time.Millisecond):
		//DPrintf("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<BeNotified method, not receive notifyOp!")
		return Op{}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	kv.db = make(map[string]string)
	kv.eleChMap = make(map[int]chan Op)
	kv.maxSeqMap = make(map[int64]int)

	// initialize from re-start
	kv.persister = persister
	kv.readSnapshot(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for {
			select {
			// when msg is committed, apply msg into state machine
			case applyMsg := <-kv.applyCh:

				//DPrintf("======================================Server: %v, apply msg: %+v", kv.me, applyMsg)
				//
				// in Lab 3 you'll want to send other kinds of messages (e.g.,
				// snapshots) on the applyCh; at that point you can add fields to
				// ApplyMsg, but set CommandValid to false for these other uses.
				//
				if !applyMsg.CommandValid {
					kv.readSnapshot(applyMsg.Snapshot)
					continue
				}
				op := applyMsg.Command.(Op)
				//DPrintf("===============:----------:======================Server: %v, apply msg convert to op: %+v", kv.me, op)
				index := applyMsg.CommandIndex

				// apply msg into state machine
				kv.mu.Lock()
				maxSeq, ok := kv.maxSeqMap[op.Cid]
				if !ok || op.SeqNum > maxSeq {
					switch op.Type {
					case "Put":
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.maxSeqMap[op.Cid] = op.SeqNum
				}
				kv.mu.Unlock()

				// do snapshot

				if kv.needSnapshot() {
					DPrintf("Server: %v, do snapshot, curIdx: %v", kv.me, index)
					go kv.doSnapshot(index)
				}

				// notify client
				if eleCh := kv.getEleCh(index, false); eleCh != nil {
					//DPrintf("!==================!===============!==============!Server: %v, send command through eleCh, op: %+v", kv.me, op)
					send(eleCh, op)
				}

			}
		}
	}()

	return kv
}

// helper function
func send(eleCh chan Op, op Op) {
	select {
	case <-eleCh:
	default:
	}
	eleCh <- op
}

func (kv *KVServer) needSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.maxraftstate >0 &&  kv.persister.RaftStateSize() > kv.maxraftstate
}

// snapshot function
func (kv *KVServer) doSnapshot(curIdx int) {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	kv.mu.Lock()
	enc.Encode(kv.db)
	enc.Encode(kv.maxSeqMap)
	kv.mu.Unlock()
	kv.rf.DoSnapshot(curIdx, w.Bytes())
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1{
		return
	}
	r := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	var db map[string]string
	var maxSeqMap map[int64]int
	if dec.Decode(&db) != nil || dec.Decode(&maxSeqMap) != nil {
		log.Fatalf("Read snapshot ERROR from server %v", kv.me)
		return
	}
	kv.db = db
	kv.maxSeqMap = maxSeqMap
}