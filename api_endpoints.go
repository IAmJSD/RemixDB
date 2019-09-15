package main

import (
	"encoding/json"
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
)

// All access control information.
type AccessControlInformation struct {
	Admin bool `json:"admin"`
	Create bool `json:"create"`
	Read bool `json:"read"`
	Write bool `json:"write"`
	DBOverrides *map[string]*AccessControlInformation `json:"db_overrides"`
	TableOverrides *map[string]*map[string]*AccessControlInformation `json:"table_overrides"`
}

// The generic response.
type GenericResponse struct {
	Error *string `json:"error"`
	Data *interface{} `json:"data"`
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

// Initialises all the HTTP endpoints.
func EndpointsInit(router *fasthttprouter.Router) {
	router.GET("/v1/get/:db/:table/:item", TokenWrapper(GETItemHTTP))
}
