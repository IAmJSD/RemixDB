package main

import (
	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"
	"log"
)

// Initialises the HTTP part of this database.
func HTTPInit() {
	router := fasthttprouter.New()
	InnerClusterRoutesInit(router)
	EndpointsInit(router)
	println("Serving on port 7010.")
	log.Fatal(fasthttp.ListenAndServe(":7010", router.Handler))
}
