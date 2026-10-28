// Command fake-telegram runs telegramtest's fake Bot API for tests in other
// processes. It prints the base URL, then serves one JSON command per stdin
// line, answering each with one JSON line:
//
//	{"op":"say","user":7,"chat":7,"text":"hi"}             {"ok":true}
//	{"op":"click","user":7,"chat":7,"message_id":3,"data":"approve:x"}
//	{"op":"wait","contains":"echo","timeout_ms":5000}      {"ok":true,"message":{...}}
//	{"op":"sent"}                                          {"ok":true,"sent":[...]}
//
// It exits when stdin closes.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/invokr-agent/agent-relay/telegramtest"
)

func main() {
	token := flag.String("token", "123:fake-token-fake-token-fake-token-x", "the bot token the fake accepts")
	flag.Parse()
	s := telegramtest.New(*token)
	defer s.Close()
	fmt.Println(s.URL())
	enc := json.NewEncoder(os.Stdout)
	in := bufio.NewScanner(os.Stdin)
	for in.Scan() {
		var c struct {
			Op, Text, Data, Contains string
			User, Chat               int64
			MessageID                int64 `json:"message_id"`
			TimeoutMS                int   `json:"timeout_ms"`
		}
		_ = json.Unmarshal(in.Bytes(), &c)
		out := map[string]any{"ok": true}
		switch c.Op {
		case "say":
			s.UserSays(c.User, c.Chat, c.Text)
		case "click":
			s.UserClicks(c.User, c.Chat, c.MessageID, c.Data)
		case "wait":
			out["message"], out["ok"] = s.WaitText(time.Duration(c.TimeoutMS)*time.Millisecond, c.Contains)
		case "sent":
			out["sent"] = s.Sent()
		default:
			out = map[string]any{"ok": false, "error": "unknown op " + c.Op}
		}
		_ = enc.Encode(out)
	}
}
