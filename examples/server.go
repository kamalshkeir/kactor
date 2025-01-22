package main

import (
	"fmt"

	"github.com/kamalshkeir/kactor"
	"github.com/kamalshkeir/ksmux"
)

func main() {
	srv := kactor.NewBusServer(ksmux.Config{
		Address: ":9314",
	}).WithDebug(false)
	srv.OnServerData(func(m map[string]any) {
		fmt.Println("got", m)
	})
	srv.Run()
}
