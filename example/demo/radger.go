package main

import (
	"github.com/kataras/iris"
	"github.com/readystock/arctonyx"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"os"
)

type ValueStruct struct {
	Value string
}

func main() {
	randomName, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}
	tmpDir, _ := ioutil.TempDir("", randomName.String())
	defer os.RemoveAll(tmpDir)
	store, err := arctonyx.CreateStore(tmpDir, ":6500", ":6501", "")
	if err != nil {
		panic(err)
	}
	app := iris.Default()
	app.Get("/{key:string}", func(ctx iris.Context) {
		key := ctx.Params().Get("key")
		val, err := store.Get([]byte(key))
		if err != nil {
			ctx.StatusCode(500)
			ctx.JSON(err.Error())
		} else {
			ctx.JSON(ValueStruct{
				Value: string(val),
			})
		}
	})
	app.Post("/{key:string}", func(ctx iris.Context) {
		key := ctx.Params().Get("key")
		value := ValueStruct{}
		err := ctx.ReadJSON(&value)
		if err != nil {
			ctx.StatusCode(500)
			ctx.JSON(err.Error())
		}
		err = store.Set([]byte(key), []byte(value.Value))
		if err != nil {
			ctx.StatusCode(500)
			ctx.JSON(err.Error())
		}
	})
	// listen and serve on http://0.0.0.0:8080.
	app.Run(iris.Addr(":8080"))
}
