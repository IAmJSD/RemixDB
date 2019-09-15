package main

func main() {
	println("RemixDB. Copyright (C) Jake Gealer 2019.")
	NewMemoryCache()
	println("Created a in-memory cache with a maximum usage of 100MB.")
	NewDBCore()
	println("Database initialised.")
	ShardInit()
	println("Sharding initialised.")
	if ShardInstance.Database("remixdb") == nil {
		err := ShardInstance.CreateDatabase("remixdb")
		if err != nil {
			panic(err)
		}
		err = ShardInstance.CreateTable("remixdb", "tokens")
		if err != nil {
			panic(err)
		}
	}
	HTTPInit()
}
