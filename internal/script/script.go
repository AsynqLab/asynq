package script

import (
	"embed"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
)

//go:embed *.lua
var luaScripts embed.FS

var (
	scriptCache     = make(map[string]*redis.Script)
	scriptCacheLock sync.RWMutex
)

func loadLuaScript(name string) (*redis.Script, error) {
	scriptCacheLock.RLock()
	script, ok := scriptCache[name]
	scriptCacheLock.RUnlock()

	if ok {
		return script, nil
	}

	scriptCacheLock.Lock()
	defer scriptCacheLock.Unlock()

	// Double-check in case another goroutine has loaded the script
	if script, ok := scriptCache[name]; ok {
		return script, nil
	}

	content, err := luaScripts.ReadFile(fmt.Sprintf("%s.lua", name))
	if err != nil {
		return nil, fmt.Errorf("failed to read Lua script %s: %w", name, err)
	}

	script = redis.NewScript(string(content))
	scriptCache[name] = script
	return script, nil
}

var (
	EnqueueCmd              *redis.Script
	EnqueueUniqueCmd        *redis.Script
	DequeueCmd              *redis.Script
	DoneCmd                 *redis.Script
	DoneUniqueCmd           *redis.Script
	MarkAsCompleteCmd       *redis.Script
	MarkAsCompleteUniqueCmd *redis.Script
	RequeueCmd              *redis.Script
	AddToGroupCmd           *redis.Script
	AddToGroupUniqueCmd     *redis.Script
	ScheduleCmd             *redis.Script
	ScheduleUniqueCmd       *redis.Script
	RetryCmd                *redis.Script
	ArchiveCmd              *redis.Script
	ForwardCmd              *redis.Script
)

const (
	enqueueCmd              = "enqueue"
	enqueueUniqueCmd        = "enqueue_unique"
	dequeueCmd              = "dequeue"
	doneCmd                 = "done"
	doneUniqueCmd           = "done_unique"
	markAsCompleteCmd       = "mark_as_completed"
	markAsCompleteUniqueCmd = "mark_as_completed_unique"
	requeueCmd              = "requeue"
	addToGroupCmd           = "add_to_group"
	addToGroupUniqueCmd     = "add_to_group_unique"
	scheduleCmd             = "schedule"
	scheduleUniqueCmd       = "schedule_unique"
	retryCmd                = "retry"
	archiveCmd              = "archive"
	forwardCmd              = "forward"
)

func init() {
	var err error
	EnqueueCmd, err = loadLuaScript(enqueueCmd)
	if err != nil {
		panic(err)
	}

	EnqueueUniqueCmd, err = loadLuaScript(enqueueUniqueCmd)
	if err != nil {
		panic(err)
	}

	DequeueCmd, err = loadLuaScript(dequeueCmd)
	if err != nil {
		panic(err)
	}

	DoneCmd, err = loadLuaScript(doneCmd)
	if err != nil {
		panic(err)
	}

	DoneUniqueCmd, err = loadLuaScript(doneUniqueCmd)
	if err != nil {
		panic(err)
	}

	MarkAsCompleteCmd, err = loadLuaScript(markAsCompleteCmd)
	if err != nil {
		panic(err)
	}

	MarkAsCompleteUniqueCmd, err = loadLuaScript(markAsCompleteUniqueCmd)
	if err != nil {
		panic(err)
	}

	RequeueCmd, err = loadLuaScript(requeueCmd)
	if err != nil {
		panic(err)
	}

	AddToGroupCmd, err = loadLuaScript(addToGroupCmd)
	if err != nil {
		panic(err)
	}

	AddToGroupUniqueCmd, err = loadLuaScript(addToGroupUniqueCmd)
	if err != nil {
		panic(err)
	}

	ScheduleCmd, err = loadLuaScript(scheduleCmd)
	if err != nil {
		panic(err)
	}

	ScheduleUniqueCmd, err = loadLuaScript(scheduleUniqueCmd)
	if err != nil {
		panic(err)
	}

	RetryCmd, err = loadLuaScript(retryCmd)
	if err != nil {
		panic(err)
	}

	ArchiveCmd, err = loadLuaScript(archiveCmd)
	if err != nil {
		panic(err)
	}

	ForwardCmd, err = loadLuaScript(forwardCmd)
	if err != nil {
		panic(err)
	}
}
