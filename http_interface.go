package main

import (
	"encoding/json"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"log"
)

// Returns a 204 (used for health checks).
func ShardPing(ctx *fasthttp.RequestCtx) {
	ctx.Response.SetStatusCode(204)
}

// Make sure the shard is authorized.
func CheckClusterAuthorization(ToWrap func(ctx *fasthttp.RequestCtx)) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		if InnerClusterToken == "" || string(ctx.Request.Header.Peek("Inner-Cluster-Token")) != InnerClusterToken {
			ctx.Response.SetStatusCode(403)
			Text := []byte("Forbidden.")
			ctx.Response.SetBody(Text)
			return
		}
		ToWrap(ctx)
	}
}

// Sends the shard config.
func ShardConfigHTTP(ctx *fasthttp.RequestCtx) {
	b, err := json.Marshal(ShardInstance)
	if err != nil {
		panic(err)
	}
	ctx.Response.SetStatusCode(200)
	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.SetBody(b)
}

// Sends all databases in this shard.
func DatabasesHTTP(ctx *fasthttp.RequestCtx) {
	Core.ArrayLock.Lock()
	b, err := json.Marshal(Core.Structure)
	if err != nil {
		panic(err)
	}
	Core.ArrayLock.Unlock()
	ctx.Response.SetStatusCode(200)
	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.SetBody(b)
}

// Handles a new shard.
func NewShardHTTP(ctx *fasthttp.RequestCtx) {
	// TODO: Finish this.
	ctx.Response.SetStatusCode(204)
}

// Initialises routes used inside the cluster.
func InnerClusterRoutesInit(router *fasthttprouter.Router) {
	router.GET("/_shard/ping", ShardPing)
	router.GET("/_shard/config", CheckClusterAuthorization(ShardConfigHTTP))
	router.GET("/_shard/dbs", CheckClusterAuthorization(DatabasesHTTP))
	router.POST("/_shard/new", CheckClusterAuthorization(NewShardHTTP))
}

// Initialises the HTTP part of this database.
func HTTPInit() {
	router := fasthttprouter.New()
	InnerClusterRoutesInit(router)
	println("Serving on port 7010.")
	log.Fatal(fasthttp.ListenAndServe(":7010", router.Handler))
}
