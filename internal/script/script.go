package script

import (
	"embed"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed *.lua
var luaScripts embed.FS

func loadLuaScript(name string) (string, error) {
	content, err := luaScripts.ReadFile(fmt.Sprintf("%s.lua", name))
	if err != nil {
		return "", fmt.Errorf("failed to read Lua script %s: %w", name, err)
	}
	return string(content), nil
}

var (
	EnqueueCmd       *redis.Script
	EnqueueUniqueCmd *redis.Script
	DequeueCmd       *redis.Script
	DoneCmd          *redis.Script
)

const (
	enqueueCmd       = "enqueue"
	enqueueUniqueCmd = "enqueueUnique"
	dequeueCmd       = "dequeue"
	doneCmd          = "done"
)

// Use this function to initialize your Redis scripts
func init() {
	enqueueLua, err := loadLuaScript(enqueueCmd)
	if err != nil {
		panic(err)
	}

	enqueueUniqueLua, err := loadLuaScript(enqueueUniqueCmd)
	if err != nil {
		panic(err)
	}

	dequeueLua, err := loadLuaScript(dequeueCmd)
	if err != nil {
		panic(err)
	}

	doneLua, err := loadLuaScript(doneCmd)
	if err != nil {
		panic(err)
	}

	// Initialize Redis scripts here
	EnqueueCmd = redis.NewScript(enqueueLua)
	EnqueueUniqueCmd = redis.NewScript(enqueueUniqueLua)
	DequeueCmd = redis.NewScript(dequeueLua)
	DoneCmd = redis.NewScript(doneLua)
}
