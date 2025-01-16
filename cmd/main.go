package main

import (
	"fmt"

	"github.com/kamalshkeir/kactor"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

func main() {
	srv := kactor.NewBusServer(ksmux.Config{
		Address: ":9313",
	})
	bus := srv.Bus()
	app := srv.App()
	app.LocalTemplates("examples")
	// init app templates and statics
	if err := app.LocalStatics("clients", "/assets"); lg.CheckError(err) {
		return
	}

	bus.Subscribe("server", "srv1", func(m map[string]any, s kactor.Subscription) {
		fmt.Println("got", m)
	})
	app.Get("/", func(c *ksmux.Context) {
		c.Html("example.html", nil)
	})
	app.Get("/pub", func(c *ksmux.Context) {
		success := bus.PublishToWithRetry("chat", "client-go", map[string]any{
			"content": "yooo",
		}, &kactor.RetryConfig{
			MaxAttempts: 3,
			MaxBackoff:  4,
		}, &kactor.PublishOptions{
			OnSuccess: func() {
				fmt.Printf("[ENDPOINT DEBUG] Successfully published message to chat client-go\n")
			},
			OnFailure: func(err error) {
				fmt.Printf("[ENDPOINT DEBUG] Failed to publish message to browserTopic: %v\n", err)
			},
		})
		if success {
			fmt.Printf("[ENDPOINT DEBUG] Message queued for publishing\n")
			c.Text("ok")
		} else {
			fmt.Printf("[ENDPOINT DEBUG] Failed to queue message for publishing\n")
			c.Error("Failed to publish message")
		}
	})
	srv.Run()
}
