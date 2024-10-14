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
	EnqueueCmd                     *redis.Script
	DequeueCmd                     *redis.Script
	DoneCmd                        *redis.Script
	DoneUniqueCmd                  *redis.Script
	MarkAsCompleteCmd              *redis.Script
	MarkAsCompleteUniqueCmd        *redis.Script
	RequeueCmd                     *redis.Script
	AddToGroupCmd                  *redis.Script
	AddToGroupUniqueCmd            *redis.Script
	ScheduleCmd                    *redis.Script
	ScheduleUniqueCmd              *redis.Script
	RetryCmd                       *redis.Script
	ArchiveCmd                     *redis.Script
	ForwardCmd                     *redis.Script
	AggregationCheckCmd            *redis.Script
	ReadAggregationSetCmd          *redis.Script
	DeleteAggregationSetCmd        *redis.Script
	ReclaimStateAggregationSetsCmd *redis.Script
	DeleteExpiredCompletedTasksCmd *redis.Script
	ListLeaseExpiredCmd            *redis.Script
	WriteServerStateCmd            *redis.Script
	ClearServerStateCmd            *redis.Script
	WriteSchedulerEntriesCmd       *redis.Script
	RecordSchedulerEnqueueEventCmd *redis.Script
)

const (
	enqueueCmd                     = "enqueue"
	dequeueCmd                     = "dequeue"
	doneCmd                        = "done"
	doneUniqueCmd                  = "done_unique"
	markAsCompleteCmd              = "mark_as_completed"
	markAsCompleteUniqueCmd        = "mark_as_completed_unique"
	requeueCmd                     = "requeue"
	addToGroupCmd                  = "add_to_group"
	addToGroupUniqueCmd            = "add_to_group_unique"
	scheduleCmd                    = "schedule"
	scheduleUniqueCmd              = "schedule_unique"
	retryCmd                       = "retry"
	archiveCmd                     = "archive"
	forwardCmd                     = "forward"
	aggregationCheckCmd            = "aggregation_check"
	readAggregationSetCmd          = "read_aggregate_set"
	deleteAggregationSetCmd        = "delete_aggregation_set"
	reclaimStateAggregationSetsCmd = "reclaim_state_aggregation_sets"
	deleteExpiredCompletedTasksCmd = "delete_expired_completed_tasks"
	listLeaseExpiredCmd            = "list_lease_expired"
	writeServerStateCmd            = "write_server_state"
	clearServerStateCmd            = "clear_server_state"
	writeSchedulerEntriesCmd       = "write_scheduler_entries"
	recordSchedulerEnqueueEventCmd = "record_scheduler_enqueue_event"
)

func init() {
	var err error
	EnqueueCmd, err = loadLuaScript(enqueueCmd)
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

	AggregationCheckCmd, err = loadLuaScript(aggregationCheckCmd)
	if err != nil {
		panic(err)
	}

	ReadAggregationSetCmd, err = loadLuaScript(readAggregationSetCmd)
	if err != nil {
		panic(err)
	}

	DeleteAggregationSetCmd, err = loadLuaScript(deleteAggregationSetCmd)
	if err != nil {
		panic(err)
	}

	ReclaimStateAggregationSetsCmd, err = loadLuaScript(reclaimStateAggregationSetsCmd)
	if err != nil {
		panic(err)
	}

	DeleteExpiredCompletedTasksCmd, err = loadLuaScript(deleteExpiredCompletedTasksCmd)
	if err != nil {
		panic(err)
	}

	ListLeaseExpiredCmd, err = loadLuaScript(listLeaseExpiredCmd)
	if err != nil {
		panic(err)
	}

	WriteServerStateCmd, err = loadLuaScript(writeServerStateCmd)
	if err != nil {
		panic(err)
	}

	ClearServerStateCmd, err = loadLuaScript(clearServerStateCmd)
	if err != nil {
		panic(err)
	}

	WriteSchedulerEntriesCmd, err = loadLuaScript(writeSchedulerEntriesCmd)
	if err != nil {
		panic(err)
	}

	RecordSchedulerEnqueueEventCmd, err = loadLuaScript(recordSchedulerEnqueueEventCmd)
	if err != nil {
		panic(err)
	}
}
