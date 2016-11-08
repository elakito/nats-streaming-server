package stores

import (
	"testing"
	"github.com/nats-io/nats-streaming-server/spb"
)

func newHybridStore(t *testing.T, dataStore string, limits *StoreLimits, options ...FileStoreOption) (*HybridStore, *RecoveredState, error) {
	opts := DefaultFileStoreOptions
	// Set those options based on command line parameters.
	// Each test may override those.
	if disableMsgsCaching {
		opts.CacheMsgs = false
	}
	if disableBufferWriters {
		opts.BufferSize = 0
	}
	// Apply the provided options
	for _, opt := range options {
		if err := opt(&opts); err != nil {
			return nil, nil, err
		}
	}
	fs, state, err := NewHybridStore(dataStore, limits, AllOptions(&opts))
	if err != nil {
		return nil, nil, err
	}
	return fs, state, nil
}

func createDefaultHybridStore(t *testing.T, options ...FileStoreOption) *HybridStore {
	limits := testDefaultStoreLimits
	fs, state, err := newHybridStore(t, defaultDataStore, &limits, options...)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	if state == nil {
		info := testDefaultServerInfo

		if err := fs.Init(&info); err != nil {
			stackFatalf(t, "Unexpected error durint Init: %v", err)
		}
	}
	return fs
}

func openDefaultHybridStore(t *testing.T, options ...FileStoreOption) (*HybridStore, *RecoveredState) {
	limits := testDefaultStoreLimits
	fs, state, err := newHybridStore(t, defaultDataStore, &limits, options...)
	if err != nil {
		stackFatalf(t, "Unable to create a FileStore instance: %v", err)
	}
	return fs, state
}

func TestHSBasicCreate(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	testBasicCreate(t, fs, TypeFile)
}

func TestHSInit(t *testing.T) {
	ms := createDefaultHybridStore(t)
	defer ms.Close()

	info := spb.ServerInfo{
		ClusterID:   "id",
		Discovery:   "discovery",
		Publish:     "publish",
		Subscribe:   "subscribe",
		Unsubscribe: "unsubscribe",
		Close:       "close",
	}
	// Should not fail
	if err := ms.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}
	info.ClusterID = "newId"
	// Should not fail
	if err := ms.Init(&info); err != nil {
		t.Fatalf("Error during init: %v", err)
	}
}

func TestHSNothingRecoveredOnFreshStart(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultHybridStore(t)
	defer fs.Close()

	testNothingRecoveredOnFreshStart(t, fs)
}

func TestHSNewChannel(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultHybridStore(t)
	defer fs.Close()

	testNewChannel(t, fs)
}

func TestHSCloseIdempotent(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultHybridStore(t)
	defer fs.Close()

	testCloseIdempotent(t, fs)
}

func TestHSBasicMsgStore(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultHybridStore(t)
	defer fs.Close()

	testBasicMsgStore(t, fs)
}

func TestHSMsgsState(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fs := createDefaultHybridStore(t)
	defer fs.Close()

	testMsgsState(t, fs)
}

func TestHSBasicRecovery(t *testing.T) {
	cleanupDatastore(t, defaultDataStore)
	defer cleanupDatastore(t, defaultDataStore)

	fooRecovered := false
	barRecovered := false
	memfooRecovered := false
	membarRecovered := false

	fs := createDefaultHybridStore(t)
	defer fs.Close()

	if fs.LookupChannel("foo") != nil {
		fooRecovered = true
	}
	if fs.LookupChannel("bar") != nil {
		barRecovered = true
	}
	if fs.LookupChannel("memfoo") != nil {
		memfooRecovered = true
	}
	if fs.LookupChannel("membar") != nil {
		membarRecovered = true
	}

	// Nothing should be recovered
	if fooRecovered || barRecovered || memfooRecovered || membarRecovered {
		t.Fatalf("Unexpected recovery: foo=%v bar=%v memfoo=%v, membar=%v",
			fooRecovered, barRecovered, memfooRecovered, membarRecovered)
	}

	foo1 := storeMsg(t, fs, "foo", []byte("foomsg"))
	foo2 := storeMsg(t, fs, "foo", []byte("foomsg"))
	foo3 := storeMsg(t, fs, "foo", []byte("foomsg"))

	bar1 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar2 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar3 := storeMsg(t, fs, "bar", []byte("barmsg"))
	bar4 := storeMsg(t, fs, "bar", []byte("barmsg"))

	memfoo1 := storeMsg(t, fs, "memfoo", []byte("foomsg"))
	memfoo2 := storeMsg(t, fs, "memfoo", []byte("foomsg"))
	memfoo3 := storeMsg(t, fs, "memfoo", []byte("foomsg"))

	membar1 := storeMsg(t, fs, "membar", []byte("barmsg"))
	membar2 := storeMsg(t, fs, "membar", []byte("barmsg"))
	membar3 := storeMsg(t, fs, "membar", []byte("barmsg"))
	membar4 := storeMsg(t, fs, "membar", []byte("barmsg"))

	sub1 := storeSub(t, fs, "foo")
	sub2 := storeSub(t, fs, "bar")

	memsub1 := storeSub(t, fs, "memfoo")
	memsub2 := storeSub(t, fs, "membar")

	storeSubPending(t, fs, "foo", sub1, foo1.Sequence, foo2.Sequence, foo3.Sequence)
	storeSubAck(t, fs, "foo", sub1, foo1.Sequence, foo3.Sequence)

	storeSubPending(t, fs, "bar", sub2, bar1.Sequence, bar2.Sequence, bar3.Sequence, bar4.Sequence)
	storeSubAck(t, fs, "bar", sub2, bar4.Sequence)

	storeSubPending(t, fs, "memfoo", memsub1, memfoo1.Sequence, memfoo2.Sequence, memfoo3.Sequence)
	storeSubAck(t, fs, "memfoo", memsub1, memfoo1.Sequence, memfoo3.Sequence)

	storeSubPending(t, fs, "membar", memsub2, membar1.Sequence, membar2.Sequence, membar3.Sequence, membar4.Sequence)
	storeSubAck(t, fs, "membar", memsub2, membar4.Sequence)

	fs.Close()

	fs, state := openDefaultHybridStore(t)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected state to be recovered")
	}
	subs := state.Subs

	// Check that subscriptions are restored
	for channel, recoveredSubs := range subs {
		if len(recoveredSubs) != 1 {
			t.Fatalf("Incorrect size of recovered subs. Expected 1, got %v ", len(recoveredSubs))
		}
		recSub := recoveredSubs[0]
		subID := recSub.Sub.ID

		switch channel {
		case "foo":
			if subID != sub1 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", sub1, subID)
			}
			for seq := range recSub.Pending {
				if seq != foo2.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub1: %v", seq)
				}
			}
			break
		case "bar":
			if subID != sub2 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", sub2, subID)
			}
			for seq := range recSub.Pending {
				if seq != bar1.Sequence && seq != bar2.Sequence && seq != bar3.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub2: %v", seq)
				}
			}
			break
		case "memfoo":
			if subID != memsub1 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", memsub1, subID)
			}
			for seq := range recSub.Pending {
				if seq != memfoo2.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub1: %v", seq)
				}
			}
			break
		case "membar":
			if subID != memsub2 {
				t.Fatalf("Invalid subscription id. Expected %v, got %v", memsub2, subID)
			}
			for seq := range recSub.Pending {
				if seq != membar1.Sequence && seq != membar2.Sequence && seq != membar3.Sequence {
					t.Fatalf("Unexpected recovered pending seqno for sub2: %v", seq)
				}
			}
			break
		default:
			t.Fatalf("Recovered unknown channel: %v", channel)
		}
	}

	cs := fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected channel foo to exist")
	}
	// In message store, the first message should still be foo1,
	// regardless of what has been consumed.
	m := cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != foo1.Sequence {
		t.Fatalf("Unexpected message for foo channel: %v", m)
	}
	// Check that messages recovered from MsgStore are never
	// marked as redelivered.
	checkRedelivered := func(ms MsgStore) bool {
		start, end := ms.FirstAndLastSequence()
		for i := start; i <= end; i++ {
			if m := ms.Lookup(i); m != nil && m.Redelivered {
				return true
			}
		}
		return false
	}
	if checkRedelivered(cs.Msgs) {
		t.Fatalf("Messages in MsgStore should not be marked as redelivered")
	}

	cs = fs.LookupChannel("bar")
	if cs == nil {
		t.Fatalf("Expected channel bar to exist")
	}
	// In message store, the first message should still be bar1,
	// regardless of what has been consumed.
	m = cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != bar1.Sequence {
		t.Fatalf("Unexpected message for bar channel: %v", m)
	}
	if checkRedelivered(cs.Msgs) {
		t.Fatalf("Messages in MsgStore should not be marked as redelivered")
	}

	cs = fs.LookupChannel("baz")
	if cs != nil {
		t.Fatal("Expected to get nil channel for baz, got something instead")
	}

	cs = fs.LookupChannel("memfoo")
	if cs == nil {
		t.Fatalf("Expected channel memfoo to exist")
	}
	// In message store, there should be no message.
	m = cs.Msgs.FirstMsg()
	if m != nil {
		t.Fatalf("Unexpected message for foo channel")
	}

	cs = fs.LookupChannel("membar")
	if cs == nil {
		t.Fatalf("Expected channel bar to exist")
	}
	// In message store, there should be no message.
	m = cs.Msgs.FirstMsg()
	if m != nil {
		t.Fatalf("Unexpected message for foo channel")
	}

	// store a new message and verify its sequence info
	foo4 := storeMsg(t, fs, "foo", []byte("foomsg2"))
	if foo4 == nil || foo4.Sequence != 4 {
		t.Fatalf("Unexpected store response: %v", foo4)
	}
	// retrieve the stored message and verify its sequence info
	// and that the old and new messages are be available
	cs = fs.LookupChannel("foo")
	if cs == nil {
		t.Fatalf("Expected channel foo to exist")
	}
	m = cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != foo1.Sequence {
		t.Fatalf("Unexpected first message for channel: %v", m)
	}
	m = cs.Msgs.LastMsg()
	if m == nil || m.Sequence != foo4.Sequence {
		t.Fatalf("Unexpected last message for channel: %v", m)
	}

	memfoo4 := storeMsg(t, fs, "memfoo", []byte("foomsg2"))
	if memfoo4 == nil || memfoo4.Sequence != 4 {
		t.Fatalf("Unexpected store response: %v", memfoo4)
	}
	// retrieve the stored message and verify its sequence info
	// and that only the new messages are be available
	cs = fs.LookupChannel("memfoo")
	if cs == nil {
		t.Fatalf("Expected channel memfoo to exist")
	}
	m = cs.Msgs.FirstMsg()
	if m == nil || m.Sequence != memfoo4.Sequence {
		t.Fatalf("Unexpected first message for channel: %v", m)
	}
	m = cs.Msgs.LastMsg()
	if m == nil || m.Sequence != memfoo4.Sequence {
		t.Fatalf("Unexpected last message for channel: %v", m)
	}
}