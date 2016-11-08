// this is a hybrid store that uses filestore for storing the broker metadata and memstore for storing the messages.
//

// Copyright 2016 Apcera Inc. All rights reserved.

package stores

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/nats-io/nats-streaming-server/spb"
	"github.com/nats-io/nats-streaming-server/util"
)

const (
	// Name of the last sequence file.
	lastseqFileName = "lastseq.dat"
)

// HybridStore is the storage interface for STAN servers, backed by files and optinally use memory for the messages
type HybridStore struct {
	FileStore
}

type HybridMemMsgStore struct {
	MemoryMsgStore
	seqFile *os.File
}

////////////////////////////////////////////////////////////////////////////
// HybridStore methods
////////////////////////////////////////////////////////////////////////////

// NewHybridStore returns a factory for stores backed by files for the broker status information and by either file
// or memory for the messages, thereby capable of recovering any state present.
// If not limits are provided, the store will be created with
// DefaultStoreLimits.
// modified from from NewFileStore to replace its MsgStore setup to use a custom memory based MsgStore
func NewHybridStore(rootDir string, limits *StoreLimits, options ...FileStoreOption) (*HybridStore, *RecoveredState, error) {
	ms := &HybridStore{}
	ms.rootDir = rootDir
	ms.opts = DefaultFileStoreOptions
	ms.init(TypeHybrid, limits)

	for _, opt := range options {
		if err := opt(&ms.opts); err != nil {
			return nil, nil, err
		}
	}
	// Convert the compact interval in time.Duration
	ms.compactItvl = time.Duration(ms.opts.CompactInterval) * time.Second
	// Create the table using polynomial in options
	if ms.opts.CRCPolynomial == int64(crc32.IEEE) {
		ms.crcTable = crc32.IEEETable
	} else {
		ms.crcTable = crc32.MakeTable(uint32(ms.opts.CRCPolynomial))
	}

	if err := os.MkdirAll(rootDir, os.ModeDir+os.ModePerm); err != nil && !os.IsExist(err) {
		return nil, nil, fmt.Errorf("unable to create the root directory [%s]: %v", rootDir, err)
	}

	var err error
	var recoveredState *RecoveredState
	var serverInfo *spb.ServerInfo
	var recoveredClients []*Client
	var recoveredSubs = make(RecoveredSubscriptions)
	var channels []os.FileInfo
	var msgStore MsgStore
	var subStore *FileSubStore

	// Ensure store is closed in case of return with error
	defer func() {
		if err != nil {
			ms.Close()
		}
	}()

	// Open/Create the server file (note that this file must not be opened,
	// in APPEND mode to allow truncate to work).
	fileName := filepath.Join(ms.rootDir, serverFileName)
	ms.serverFile, err = openFile(fileName, os.O_RDWR, os.O_CREATE)
	if err != nil {
		return nil, nil, err
	}

	// Open/Create the client file.
	fileName = filepath.Join(ms.rootDir, clientsFileName)
	ms.clientsFile, err = openFile(fileName)
	if err != nil {
		return nil, nil, err
	}

	// Recover the server file.
	serverInfo, err = ms.recoverServerInfo()
	if err != nil {
		return nil, nil, err
	}
	// If the server file is empty, then we are done
	if serverInfo == nil {
		// We return the file store instance, but no recovered state.
		return ms, nil, nil
	}

	// Recover the clients file
	recoveredClients, err = ms.recoverClients()
	if err != nil {
		return nil, nil, err
	}

	// Get the channels (there are subdirectories of rootDir)
	channels, err = ioutil.ReadDir(rootDir)
	if err != nil {
		return nil, nil, err
	}
	// Go through the list
	for _, c := range channels {
		// Channels are directories. Ignore simple files
		if !c.IsDir() {
			continue
		}

		channel := c.Name()
		channelDirName := filepath.Join(rootDir, channel)

		// currently channel determines which persistence option is used (mem* -> memory; otherwise file)
		if strings.HasPrefix(channel, "mem") {
			// Recover the sequence status for this channel
			msgStore, err = ms.newHybridMemMsgStore(channelDirName, channel, true)
			if err != nil {
				break
			}
		} else {
			// Recover messages for this channel
			msgStore, err = ms.newFileMsgStore(channelDirName, channel, true)
			if err != nil {
				break
			}

		}
		// Recover subscriptions for this channel
		subStore, err = ms.newFileSubStore(channelDirName, channel, true)
		if err != nil {
			msgStore.Close()
			break
		}

		// For this channel, construct an array of RecoveredSubState
		rssArray := make([]*RecoveredSubState, 0, len(subStore.subs))

		// Fill that array with what we got from newFileSubStore.
		for _, sub := range subStore.subs {
			// The server is making a copy of rss.Sub, still it is not
			// a good idea to return a pointer to an object that belong
			// to the store. So make a copy and return the pointer to
			// that copy.
			csub := *sub.sub
			rss := &RecoveredSubState{
				Sub:     &csub,
				Pending: make(PendingAcks),
			}
			// If we recovered any seqno...
			if len(sub.seqnos) > 0 {
				// Lookup messages, and if we find those, update the
				// Pending map.
				for seq := range sub.seqnos {
					rss.Pending[seq] = struct{}{}
				}
			}
			// Add to the array of recovered subscriptions
			rssArray = append(rssArray, rss)
		}

		// This is the recovered subscription state for this channel
		recoveredSubs[channel] = rssArray

		ms.channels[channel] = &ChannelStore{
			Subs: subStore,
			Msgs: msgStore,
		}
	}
	if err != nil {
		return nil, nil, err
	}
	// Create the recovered state to return
	recoveredState = &RecoveredState{
		Info:    serverInfo,
		Clients: recoveredClients,
		Subs:    recoveredSubs,
	}
	return ms, recoveredState, nil
}

// CreateChannel creates a ChannelStore for the given channel, and returns
// `true` to indicate that the channel is new, false if it already exists.
// modified from FileStore.CreateChannel to replace its MsgStore setup to use a custom memory based MsgStore
func (ms *HybridStore) CreateChannel(channel string, userData interface{}) (*ChannelStore, bool, error) {
	ms.Lock()
	defer ms.Unlock()
	channelStore := ms.channels[channel]
	if channelStore != nil {
		return channelStore, false, nil
	}

	// Check for limits
	if err := ms.canAddChannel(); err != nil {
		return nil, false, err
	}

	// We create the channel here...

	channelDirName := filepath.Join(ms.rootDir, channel)
	if err := os.MkdirAll(channelDirName, os.ModeDir+os.ModePerm); err != nil {
		return nil, false, err
	}

	var err error
	var msgStore MsgStore
	var subStore SubStore

	// currently channel determines which persistence option is used (mem* -> memory; otherwise file)
	if strings.HasPrefix(channel, "mem") {
		msgStore, err = ms.newHybridMemMsgStore(channelDirName, channel, false)
		if err != nil {
			return nil, false, err
		}
	} else {
		msgStore, err = ms.newFileMsgStore(channelDirName, channel, false)
		if err != nil {
			return nil, false, err
		}
	}
	subStore, err = ms.newFileSubStore(channelDirName, channel, false)
	if err != nil {
		msgStore.Close()
		return nil, false, err
	}

	channelStore = &ChannelStore{
		Subs:     subStore,
		Msgs:     msgStore,
		UserData: userData,
	}

	ms.channels[channel] = channelStore

	return channelStore, true, nil
}

// newHybridMemMsgStore returns a new instace of a custom memory based MsgStore that persists the last sequence number.
func (ms *HybridStore) newHybridMemMsgStore(channelDirName, channel string, doRecover bool) (*HybridMemMsgStore, error) {
	var err error
	mms := &HybridMemMsgStore{}
	mms.MemoryMsgStore.msgs = make(map[uint64]*pb.MsgProto, 64)

	msgStoreLimits := ms.limits.MsgStoreLimits
	// See if there is an override
	thisChannelLimits, exists := ms.limits.PerChannel[channel]
	if exists {
		// Use this channel specific limits
		msgStoreLimits = thisChannelLimits.MsgStoreLimits
	}

	mms.init(channel, &msgStoreLimits)
	fileName := filepath.Join(channelDirName, lastseqFileName)
	mms.seqFile, err = openFile(fileName)
	if err != nil {
		return nil, err
	}
	if doRecover {
		if err := mms.recoverSequence(); err != nil {
			mms.Close()
			return nil, fmt.Errorf("unable to create sequence store for [%s]: %v", channel, err)
		}
	}
	return mms, nil
}

// newFileMsgStore returns a new instace of a FileMsgStore.
// modfied from FileStore.newFileMsgStore
func (ms *HybridStore) newFileMsgStore(channelDirName, channel string, doRecover bool) (*FileMsgStore, error) {
	var err error
	var useIdxFile bool

	// Create an instance and initialize
	fms := &FileMsgStore{
		msgs:         make(map[uint64]*msgRecord, 64),
		wOffset:      int64(4), // The very first record starts after the file version record
		bufferedMsgs: make([]uint64, 0, 1),
		cache:        ms.opts.CacheMsgs,
		opts:         &ms.opts,
		crcTable:     ms.crcTable,
	}

	// Defaults to the global limits
	msgStoreLimits := ms.limits.MsgStoreLimits
	// See if there is an override
	thisChannelLimits, exists := ms.limits.PerChannel[channel]
	if exists {
		// Use this channel specific limits
		msgStoreLimits = thisChannelLimits.MsgStoreLimits
	}
	fms.init(channel, &msgStoreLimits)

	fms.slCountLim = ms.limits.MaxMsgs / (numFiles - 1)
	if fms.slCountLim < 1 {
		fms.slCountLim = 1
	}
	fms.slSizeLim = uint64(ms.limits.MaxBytes / (numFiles - 1))
	if fms.slSizeLim < 1 {
		fms.slSizeLim = 1
	}

	maxBufSize := ms.opts.BufferSize
	if maxBufSize > 0 {
		fms.bw = newBufferWriter(msgBufMinShrinkSize, maxBufSize)
	}
	done := false
	for i := 0; err == nil && i < numFiles; i++ {
		// Fully qualified file name.
		fileName := filepath.Join(channelDirName, fmt.Sprintf("msgs.%d.dat", (i+1)))
		idxFName := filepath.Join(channelDirName, fmt.Sprintf("msgs.%d.idx", (i+1)))

		// Create slice
		fms.files[i] = &fileSlice{fileName: fileName, idxFName: idxFName}

		if done {
			continue
		}

		// On recovery...
		if doRecover {
			// We previously pre-created all files, we don't anymore.
			// So if a slice is not present, we are done.
			if s, statErr := os.Stat(fileName); s == nil || statErr != nil {
				done = true
				continue
			}
			// If an index file is present, recover only the index file, not the data.
			if s, statErr := os.Stat(idxFName); s != nil && statErr == nil {
				useIdxFile = true
			}
		}
		// On create, simply create the first file, on recovery we need to recover
		// each existing file
		if i == 0 || doRecover {
			err = fms.openDataAndIndexFiles(fileName, idxFName)
		}
		// Should we try to recover (startup case)
		if err == nil && doRecover {
			done, err = fms.recoverOneMsgFile(useIdxFile, i)
		}
	}
	if err == nil {
		// Apply message limits (no need to check if there are limits
		// defined, the call won't do anything if they aren't).
		err = fms.enforceLimits(false)
		// If there is at least one message and age limit is present...
		if err == nil && fms.totalCount > 0 && ms.limits.MaxAge > time.Duration(0) {
			fms.Lock()
			fms.allDone.Add(1)
			// Create the timer with any duration.
			fms.ageTimer = time.AfterFunc(time.Hour, fms.expireMsgs)
			fms.Unlock()
			// And now force the execution of the expireMsgs callback.
			// This will take care of expiring messages that should
			// already be expired, and will set properly the timer.
			fms.expireMsgs()
		}
	}
	if err == nil && maxBufSize > 0 {
		fms.Lock()
		fms.allDone.Add(1)
		fms.tasksTimer = time.AfterFunc(time.Second, fms.backgroundTasks)
		fms.Unlock()
	}
	// Cleanup on error
	if err != nil {
		// The buffer writer may not be fully set yet
		if fms.bw != nil && fms.bw.buf == nil {
			fms.bw = nil
		}
		fms.Close()
		fms = nil
		action := "create"
		if doRecover {
			action = "recover"
		}
		err = fmt.Errorf("unable to %s message store for [%s]: %v", action, channel, err)
		return nil, err
	}

	return fms, nil
}

// Store a given message in memory and update the last sequence
// modified from MemoryStore.Store to persist the last sequence number.
func (ms *HybridMemMsgStore) Store(data []byte) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()

	if ms.first == 0 {
		ms.first = 1
	}
	ms.last++
	m := &pb.MsgProto{
		Sequence:  ms.last,
		Subject:   ms.subject,
		Data:      data,
		Timestamp: time.Now().UnixNano(),
	}
	ms.msgs[ms.last] = m
	ms.totalCount++
	ms.totalBytes += uint64(m.Size())
	buf := make([]byte, 8)
	util.ByteOrder.PutUint64(buf, ms.last)
	ms.seqFile.WriteAt(buf, 4)
	// If there is an age limit and no timer yet created, do so now
	if ms.limits.MaxAge > time.Duration(0) && ms.ageTimer == nil {
		ms.wg.Add(1)
		ms.ageTimer = time.AfterFunc(ms.limits.MaxAge, ms.expireMsgs)
	}

	// Check if we need to remove any (but leave at least the last added)
	maxMsgs := ms.limits.MaxMsgs
	maxBytes := ms.limits.MaxBytes
	if maxMsgs > 0 || maxBytes > 0 {
		for ms.totalCount > 1 &&
			((maxMsgs > 0 && ms.totalCount > maxMsgs) ||
				(maxBytes > 0 && (ms.totalBytes > uint64(maxBytes)))) {
			ms.removeFirstMsg()
			if !ms.hitLimit {
				ms.hitLimit = true
				Noticef(droppingMsgsFmt, ms.subject, ms.totalCount, ms.limits.MaxMsgs, ms.totalBytes, ms.limits.MaxBytes)
			}
		}
	}

	return ms.last, nil
}

// recoverSequence recovers the sequence status for this store.
func (ms *HybridMemMsgStore) recoverSequence() error {
	buf := make([]byte, 8)
	if _, err := ms.seqFile.ReadAt(buf, 4); err != nil {
		return err
	}
	ms.last = util.ByteOrder.Uint64(buf)
	ms.first = ms.last + 1
	return nil
}
