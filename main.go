package main

func main() {
	println("RemixDB. Copyright (C) Jake Gealer 2019.")
	NewMemoryCache()
	println("Created a in-memory cache with a maximum usage of 100MB.")
	NewDBCore()
	println("Database initialised.")
}
