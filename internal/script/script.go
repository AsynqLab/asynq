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

var EnqueueCmd *redis.Script

// Use this function to initialize your Redis scripts
func init() {
	enqueueLua, err := loadLuaScript("enqueue")
	if err != nil {
		panic(err)
	}

	// Initialize Redis scripts here
	EnqueueCmd = redis.NewScript(enqueueLua)
}
