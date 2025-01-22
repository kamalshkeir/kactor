package main

import (
	"github.com/kamalshkeir/kactor"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
)

func main() {
	srv := kactor.NewBusServer(ksmux.Config{
		Address: ":9313",
	}).WithDebug(false)

	app := srv.App()
	app.LocalTemplates("examples")
	// init app templates and statics
	if err := app.LocalStatics("clients", "/assets"); lg.CheckError(err) {
		return
	}

	app.Get("/test", func(c *ksmux.Context) {
		c.Html("example.html", nil)
	})

	app.Get("/pub", func(c *ksmux.Context) {
		srv.PublishToServer(false, "localhost:9314", map[string]any{
			"msg": "yoooooooooooooooo",
		}, nil)
	})
	srv.Run()
}
