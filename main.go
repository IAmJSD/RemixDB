package main

import (
	"log"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func Time1() {
	defer timeTrack(time.Now(), "DBCreate")
	Core.CreateDatabase("test")
}

func Time2() {
	defer timeTrack(time.Now(), "TableCreate")
	Core.CreateTable("test", "a")
}

func Time3() {
	defer timeTrack(time.Now(), "IndexCreate")
	Core.CreateIndex("test", "a", "a", []string{"a"})
}

func Time4() {
	defer timeTrack(time.Now(), "Insert")
	Core.Insert("test", "a", "world", ToInterfacePtr(map[string]interface{}{
		"a": "b",
	}))
}

func Time5() {
	defer timeTrack(time.Now(), "UncachedGet")
	Core.Get("test", "a", "world")
}

func Time6() {
	defer timeTrack(time.Now(), "CachedGet")
	Core.Get("test", "a", "world")
}

func main() {
	println("RemixDB. Copyright (C) Jake Gealer 2019.")
	NewMemoryCache()
	println("Created a in-memory cache with a maximum usage of 100MB.")
	NewDBCore()
	println("Database initialised.")

	Time1()
	Time2()
	Time3()
	Time4()
	Time5()
	Time6()
}
