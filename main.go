package main

import (
	"github.com/salsita-integrations/salsabot-slack/handler"

	"github.com/meeko-contrib/go-meeko-webhook-receiver/receiver"
	"github.com/meeko/go-meeko/agent"
)

func main() {
	receiver.ListenAndServe(handler.New(agent.Logging()))
}
