package main

import (
	"encoding/json"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

// All access control information.
type AccessControlInformation struct {
	Admin          bool                                              `json:"admin"`
	Create         bool                                              `json:"create"`
	Read           bool                                              `json:"read"`
	Write          bool                                              `json:"write"`
	DBOverrides    *map[string]*AccessControlInformation             `json:"db_overrides"`
	TableOverrides *map[string]*map[string]*AccessControlInformation `json:"table_overrides"`
}

// The generic response.
type GenericResponse struct {
	Error *string      `json:"error"`
	Data  *interface{} `json:"data"`
}

// Checks the alleged token. The struct containing token access control information will be returned.
func CheckAllegedToken(Token string) *AccessControlInformation {
	// Checks if a token is safe.
	if len(Token) == 0 {
		return nil
	}

	// Gets the token from the database.
	data, err := ShardInstance.Get("remixdb", "tokens", Token)
	if err != nil {
		return nil
	}

	var i AccessControlInformation
	j, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(j, &i)
	if err != nil {
		panic(err)
	}

	return &i
}

// Sends a JSON response.
func SendJSONResponse(response interface{}, ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.SetContentType("application/json")
	b, err := json.Marshal(&response)
	if err != nil {
		panic(err)
	}
	ctx.Response.SetBody(b)
}

// Sends unauthorized.
func SendUnauthorized(ctx *fasthttp.RequestCtx) {
	ctx.Response.SetStatusCode(403)
	nope := "Unauthorized."
	SendJSONResponse(GenericResponse{
		Error: &nope,
		Data:  nil,
	}, ctx)
}

// A wrapper for authorization.
func TokenWrapper(ToWrap func(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation)) func(ctx *fasthttp.RequestCtx) {
	return func(ctx *fasthttp.RequestCtx) {
		Token := ctx.Request.Header.Peek("Token-Auth")
		x := CheckAllegedToken(string(Token))
		if x == nil {
			SendUnauthorized(ctx)
			return
		}
		ToWrap(ctx, x)
	}
}

// Gets a item from the DB.
func GETItemHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Read
	DB := ctx.UserValue("db").(string)
	Table := ctx.UserValue("table").(string)
	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Read
		}
	}
	if AccessControl.TableOverrides != nil {
		DBTableOverride := (*AccessControl.TableOverrides)[DB]
		if DBTableOverride != nil {
			TableOverride := (*DBTableOverride)[Table]
			if TableOverride != nil {
				Perm = TableOverride.Read
			}
		}
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	Item := ctx.UserValue("item").(string)
	g, err := ShardInstance.Get(DB, Table, Item)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		e := err.Error()
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}
	ctx.Response.SetStatusCode(200)
	SendJSONResponse(GenericResponse{
		Error: nil,
		Data:  g,
	}, ctx)
}

// Gets the database information.
func GETDatabaseHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Read
	DB := ctx.UserValue("db").(string)
	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Read
		}
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	ctx.Response.SetStatusCode(200)
	DBData := ShardInstance.Database(DB)
	SendJSONResponse(GenericResponse{
		Error: nil,
		Data:  ToInterfacePtr(DBData),
	}, ctx)
}

// Inserts the database.
func PUTTableHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Write
	DB := ctx.UserValue("db").(string)
	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Write
		}
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	Table := ctx.UserValue("table").(string)

	err := ShardInstance.CreateTable(DB, Table)
	if err == nil {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(400)
		e := err.Error()
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	}
}

// Inserts the table.
func PUTDatabaseHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Write

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	DB := ctx.UserValue("db").(string)

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	err := ShardInstance.CreateDatabase(DB)
	if err == nil {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(400)
		e := err.Error()
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	}
}

// Gets the table information.
func GETTableHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Read
	DB := ctx.UserValue("db").(string)
	Table := ctx.UserValue("table").(string)
	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Read
		}
	}
	if AccessControl.TableOverrides != nil {
		DBTableOverride := (*AccessControl.TableOverrides)[DB]
		if DBTableOverride != nil {
			TableOverride := (*DBTableOverride)[Table]
			if TableOverride != nil {
				Perm = TableOverride.Read
			}
		}
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	ctx.Response.SetStatusCode(200)
	DBData := ShardInstance.Table(DB, Table)
	SendJSONResponse(GenericResponse{
		Error: nil,
		Data:  ToInterfacePtr(DBData),
	}, ctx)
}

// Gets the table keys.
func GETTableKeysHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Read
	DB := ctx.UserValue("db").(string)
	Table := ctx.UserValue("table").(string)
	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Read
		}
	}
	if AccessControl.TableOverrides != nil {
		DBTableOverride := (*AccessControl.TableOverrides)[DB]
		if DBTableOverride != nil {
			TableOverride := (*DBTableOverride)[Table]
			if TableOverride != nil {
				Perm = TableOverride.Read
			}
		}
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	DBData, err := ShardInstance.TableKeys(DB, Table)
	if err != nil {
		e := err.Error()
		ctx.Response.SetStatusCode(400)
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  ToInterfacePtr(DBData),
		}, ctx)
	}
}

// Deletes a database.
func DELETEDatabaseHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Admin
	DB := ctx.UserValue("db").(string)

	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Admin
		}
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	err := ShardInstance.DeleteDatabase(DB)
	if err == nil {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(400)
		e := err.Error()
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	}
}

// Deletes a table.
func DELETETableHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Admin
	DB := ctx.UserValue("db").(string)
	Table := ctx.UserValue("table").(string)

	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Admin
		}
	}
	if AccessControl.TableOverrides != nil {
		DBTableOverride := (*AccessControl.TableOverrides)[DB]
		if DBTableOverride != nil {
			TableOverride := (*DBTableOverride)[Table]
			if TableOverride != nil {
				Perm = TableOverride.Admin
			}
		}
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	err := ShardInstance.DeleteTable(DB, Table)
	if err == nil {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(400)
		e := err.Error()
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	}
}

// Deletes the item.
func DELETEItemHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Write
	DB := ctx.UserValue("db").(string)
	Table := ctx.UserValue("table").(string)
	Item := ctx.UserValue("item").(string)

	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Write
		}
	}
	if AccessControl.TableOverrides != nil {
		DBTableOverride := (*AccessControl.TableOverrides)[DB]
		if DBTableOverride != nil {
			TableOverride := (*DBTableOverride)[Table]
			if TableOverride != nil {
				Perm = TableOverride.Write
			}
		}
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	err := ShardInstance.DeleteRecord(DB, Table, Item)
	if err == nil {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(400)
		e := err.Error()
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	}
}

// Allows a user to insert a item into the DB.
func POSTItemHTTP(ctx *fasthttp.RequestCtx, AccessControl *AccessControlInformation) {
	Perm := AccessControl.Write
	DB := ctx.UserValue("db").(string)
	Table := ctx.UserValue("table").(string)
	if AccessControl.DBOverrides != nil {
		DBOverride := (*AccessControl.DBOverrides)[DB]
		if DBOverride != nil {
			Perm = DBOverride.Write
		}
	}
	if AccessControl.TableOverrides != nil {
		DBTableOverride := (*AccessControl.TableOverrides)[DB]
		if DBTableOverride != nil {
			TableOverride := (*DBTableOverride)[Table]
			if TableOverride != nil {
				Perm = TableOverride.Write
			}
		}
	}

	if DB == "remixdb" && !AccessControl.Admin {
		// Nope! This requires admin.
		SendUnauthorized(ctx)
		return
	}

	if DB == "__internal" {
		// Here be dragons!
		e := "This is an internal database used by RemixDB on a per-shard basis. Here be dragons!"
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
		return
	}

	if !Perm {
		SendUnauthorized(ctx)
		return
	}

	var Response {}interface
	var Data []byte
	_, err := req.Body.Read(Data)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(Data, &Response)
	if err != nil {
		panic(err)
	}

	err := ShardInstance.Insert(DB, Table, ctx.UserValue("item").(string), &Response)
	if err != nil {
		e := err.Error()
		ctx.Response.SetStatusCode(400)
		SendJSONResponse(GenericResponse{
			Error: &e,
			Data:  nil,
		}, ctx)
	} else {
		ctx.Response.SetStatusCode(200)
		SendJSONResponse(GenericResponse{
			Error: nil,
			Data:  nil,
		}, ctx)
	}
}

// Initialises all the HTTP endpoints.
func EndpointsInit(router *fasthttprouter.Router) {
	router.GET("/v1/record/:db/:table/:item", TokenWrapper(GETItemHTTP))
	router.POST("/v1/record/:db/:table/:item", TokenWrapper(POSTItemHTTP))
	router.DELETE("/v1/record/:db/:table/:item", TokenWrapper(DELETEItemHTTP))
	router.GET("/v1/database/:db", TokenWrapper(GETDatabaseHTTP))
	router.PUT("/v1/database/:db", TokenWrapper(PUTDatabaseHTTP))
	router.DELETE("/v1/database/:db", TokenWrapper(DELETEDatabaseHTTP))
	router.GET("/v1/table/:db/:table", TokenWrapper(GETTableHTTP))
	router.GET("/v1/table/:db/:table/keys", TokenWrapper(GETTableKeysHTTP))
	router.PUT("/v1/table/:db/:table", TokenWrapper(PUTTableHTTP))
	router.DELETE("/v1/table/:db/:table", TokenWrapper(DELETETableHTTP))
}
