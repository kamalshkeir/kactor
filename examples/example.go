package main

import (
	"fmt"
	"time"

	"github.com/kamalshkeir/kactor"
	"github.com/kamalshkeir/lg"
)

func example() {
	client, err := kactor.NewClient(kactor.ClientConfig{
		Address: "localhost:9313",
		Path:    "/ws/kactor",
	})
	if lg.CheckError(err) {
		return
	}
	defer client.Close()
	count := 0
	client.Subscribe("chat", "client-go", func(message map[string]any, sub kactor.Subscription) {
		lg.Printfs("got message: %v\n", message)
		count++
		if count == 4 {
			sub.Unsubscribe()
		}
	})
	time.Sleep(5 * time.Second)

	pubOk := client.Publish("server", map[string]any{
		"msg": "hello from go client",
	}, nil)
	if !pubOk {
		lg.ErrorC("couldn't publish")
	} else {
		lg.Info("published to server topic")
	}
	fmt.Scanln()
}
