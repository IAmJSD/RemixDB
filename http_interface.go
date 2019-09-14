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
	var Item NewShardStructure
	err := json.Unmarshal(ctx.Request.Body(), &Item)
	if err != nil {
		panic(err)
	}
	InsertShard(Item.ShardID, Item.ShardURL)
	ctx.Response.SetStatusCode(204)
}

// Marks a shard as ready.
func ReadyShardHTTP(ctx *fasthttp.RequestCtx) {
	MarkShardAsReady(ctx.UserValue("shard").(string))
	ctx.Response.SetStatusCode(204)
}

// Inserts data into a database.
func InsertDataHTTP(ctx *fasthttp.RequestCtx) {
	var Item RemoteInsertStructure
	err := json.Unmarshal(ctx.Request.Body(), &Item)
	if err != nil {
		panic(err)
	}
	err = Core.Insert(Item.DB, Item.Table, Item.Key, &Item.Item)
	ctx.Response.SetStatusCode(200)
	var Response *string
	if err != nil {
		e := err.Error()
		Response = &e
	}
	ctx.Response.Header.SetContentType("application/json")
	b, err := json.Marshal(&Response)
	ctx.Response.SetBody(b)
}

// Gets a item from the local DB.
func GetDataHTTP(ctx *fasthttp.RequestCtx) {
	d, err := Core.Get(ctx.UserValue("db").(string), ctx.UserValue("table").(string), ctx.UserValue("item").(string))
	var Response RemoteShardGetResponse
	if err == nil {
		Response = RemoteShardGetResponse{
			Err:  nil,
			Data: d,
		}
	} else {
		e := err.Error()
		Response = RemoteShardGetResponse{
			Err:  &e,
			Data: nil,
		}
	}
	b, err := json.Marshal(&Response)
	if err != nil {
		panic(err)
	}
	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.SetStatusCode(200)
	ctx.Response.SetBody(b)
}

// Creates a database (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func NewDBHTTP(ctx *fasthttp.RequestCtx) {
	_ = Core.CreateDatabase(ctx.UserValue("db").(string))
	ctx.Response.SetStatusCode(204)
}

// Creates a index (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func NewIndexHTTP(ctx *fasthttp.RequestCtx) {
	var keys []string
	err := json.Unmarshal(ctx.UserValue("keys").([]byte), &keys)
	if err != nil {
		panic(err)
	}
	_ = Core.CreateIndex(ctx.UserValue("db").(string), ctx.UserValue("table").(string), ctx.UserValue("index").(string), keys)
	ctx.Response.SetStatusCode(204)
}

// Creates a table (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func NewTableHTTP(ctx *fasthttp.RequestCtx) {
	_ = Core.CreateTable(ctx.UserValue("db").(string), ctx.UserValue("table").(string))
	ctx.Response.SetStatusCode(204)
}

// Deletes a database (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func DeleteDBHTTP(ctx *fasthttp.RequestCtx) {
	_ = Core.DeleteDatabase(ctx.UserValue("db").(string))
	ctx.Response.SetStatusCode(204)
}

// Deletes a index (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func DeleteIndexHTTP(ctx *fasthttp.RequestCtx) {
	_ = Core.DeleteIndex(ctx.UserValue("db").(string), ctx.UserValue("table").(string), ctx.UserValue("index").(string))
	ctx.Response.SetStatusCode(204)
}

// Deletes a index (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func DeleteRecordHTTP(ctx *fasthttp.RequestCtx) {
	_ = Core.DeleteRecord(ctx.UserValue("db").(string), ctx.UserValue("table").(string), ctx.UserValue("key").(string))
	ctx.Response.SetStatusCode(204)
}

// Deletes a table (errors can be suppressed, if there was a caught issue, it would happen on the local shard first).
func DeleteTableHTTP(ctx *fasthttp.RequestCtx) {
	_ = Core.DeleteTable(ctx.UserValue("db").(string), ctx.UserValue("table").(string))
	ctx.Response.SetStatusCode(204)
}

// Gets all table keys.
func TableKeysHTTP(ctx *fasthttp.RequestCtx) {
	keys, err := Core.TableKeys(ctx.UserValue("db").(string), ctx.UserValue("table").(string))
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(&keys)
	if err != nil {
		panic(err)
	}
	ctx.Response.SetStatusCode(200)
	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.SetBody(b)
}

// Initialises routes used inside the cluster.
func InnerClusterRoutesInit(router *fasthttprouter.Router) {
	router.GET("/_shard/ping", ShardPing)
	router.GET("/_shard/config", CheckClusterAuthorization(ShardConfigHTTP))
	router.GET("/_shard/dbs", CheckClusterAuthorization(DatabasesHTTP))
	router.POST("/_shard/new", CheckClusterAuthorization(NewShardHTTP))
	router.GET("/_shard/ready/:shard", CheckClusterAuthorization(ReadyShardHTTP))
	router.POST("/_shard/insert", CheckClusterAuthorization(InsertDataHTTP))
	router.GET("/_shard/get/:db/:table/:item", CheckClusterAuthorization(GetDataHTTP))
	router.GET("/_shard/new_db/:db", CheckClusterAuthorization(NewDBHTTP))
	router.GET("/_shard/new_index/:db/:table/:index/:keys", CheckClusterAuthorization(NewIndexHTTP))
	router.GET("/_shard/new_table/:db/:table", CheckClusterAuthorization(NewTableHTTP))
	router.GET("/_shard/delete_db/:db", CheckClusterAuthorization(DeleteDBHTTP))
	router.GET("/_shard/delete_index/:db/:table/:index", CheckClusterAuthorization(DeleteIndexHTTP))
	router.GET("/_shard/delete_record/:db/:table/:key", CheckClusterAuthorization(DeleteRecordHTTP))
	router.GET("/_shard/delete_table/:db/:table", CheckClusterAuthorization(DeleteTableHTTP))
	router.GET("/_shard/table_keys/:db/:table", CheckClusterAuthorization(TableKeysHTTP))
}

// Initialises the HTTP part of this database.
func HTTPInit() {
	router := fasthttprouter.New()
	InnerClusterRoutesInit(router)
	println("Serving on port 7010.")
	log.Fatal(fasthttp.ListenAndServe(":7010", router.Handler))
}
