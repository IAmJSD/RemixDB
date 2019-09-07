package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

// Defines the index structure.
type Index struct {
	Name string `json:"n"`
	Keys []string `json:"k"`
	IndexLock *sync.Mutex `json:"-"`
	MapPreload *map[string]*[]string `json:"-"`
	CurrentIndexDoc int `json:"-"`
}

// Initialises the index.
func (i *Index) Init(Base string, DatabaseName string, TableName string) {
	if i.IndexLock == nil {
		i.IndexLock = &sync.Mutex{}
	}
	if i.MapPreload == nil {
		i.IndexLock.Lock()
		i.MapPreload = &map[string]*[]string{}
		IndexDir := path.Join(Base, "dbs", DatabaseName, TableName, "i", i.Name)
		if _, err := os.Stat(IndexDir); os.IsNotExist(err) {
			err = os.Mkdir(IndexDir, 0777)
			if err != nil {
				panic(err)
			}
		}
		f, _ := ioutil.ReadDir(IndexDir)
		i.CurrentIndexDoc = len(f)
		if i.CurrentIndexDoc != 0 {
			data, err := ioutil.ReadFile(path.Join(IndexDir, "0"))
			if err != nil {
				panic(err)
			}
			err = json.Unmarshal(data, i.MapPreload)
			if err != nil {
				panic(err)
			}
		}
		i.IndexLock.Unlock()
	}
}

// Insets into a index.
func (i *Index) Insert(Base string, DatabaseName string, TableName string, Key string, Item string) {
	IndexFile := "0"
	var IndexFilePath string
	var MapSave *map[string]*[]string
	i.IndexLock.Lock()

	if len(*i.MapPreload) == 50000 {
		// Load the last part of the index from disk. If it's also the length of 50,000, make a new index file.
		var DiskLoad map[string]*[]string
		IndexFile = string(i.CurrentIndexDoc - 1)
		IndexFilePath = path.Join(Base, "dbs", DatabaseName, TableName, "i", i.Name, IndexFile)
		f, err := ioutil.ReadFile(IndexFilePath)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(f, &DiskLoad)
		if err != nil {
			panic(err)
		}
		if len(DiskLoad) == 50000 {
			MapSave = &map[string]*[]string{}
			IndexFile = string(i.CurrentIndexDoc)
			i.CurrentIndexDoc++
		} else {
			MapSave = &DiskLoad
		}
	} else {
		// This is in memory! Grab the preload.
		MapSave = i.MapPreload
		IndexFilePath = path.Join(Base, "dbs", DatabaseName, TableName, "i", i.Name, IndexFile)
	}

	if (*MapSave)[Key] == nil {
		(*MapSave)[Key] = &[]string{}
	}
	Appended := append(*(*MapSave)[Key], Item)
	(*MapSave)[Key] = &Appended

	b, err := json.Marshal(MapSave)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(IndexFilePath)
	if err != nil {
		panic(err)
	}
	_, err = f.Write(b)
	if err != nil {
		panic(err)
	}
	i.IndexLock.Unlock()
}

// Deletes an item from this index.
func (i *Index) DeleteItem(Base string, DatabaseName string, TableName string, Item string) {
	// Locks the index lock.
	i.IndexLock.Lock()

	// We will check preload first.
	for k, v := range *i.MapPreload {
		for index, x := range *v {
			if x == Item {
				Data := make([]string, len(*v) - 1)
				z := 0
				for CurrentIndex, d := range *v {
					if index == CurrentIndex {
						continue
					}
					Data[z] = d
					z++
				}
				(*i.MapPreload)[k] = &Data
				IndexFilePath := path.Join(Base, "dbs", DatabaseName, TableName, "i", i.Name, "0")
				f, err := os.Create(IndexFilePath)
				if err != nil {
					panic(err)
				}
				b, err := json.Marshal(i.MapPreload)
				if err != nil {
					panic(err)
				}
				_, err = f.Write(b)
				if err != nil {
					panic(err)
				}
				i.IndexLock.Unlock()
				return
			}
		}
	}

	// No luck with the preloaded data, lets open the other index files if they exist.
	if i.CurrentIndexDoc > 1 {
		x := 1
		for i.CurrentIndexDoc != x {
			IndexFilePath := path.Join(Base, "dbs", DatabaseName, TableName, "i", i.Name, string(x))
			var Loaded map[string]*[]string
			d, err := ioutil.ReadFile(IndexFilePath)
			if err != nil {
				panic(err)
			}
			err = json.Unmarshal(d, &Loaded)
			if err != nil {
				panic(err)
			}
			for k, v := range Loaded {
				for index, x := range *v {
					if x == Item {
						Data := make([]string, len(*v) - 1)
						z := 0
						for CurrentIndex, d := range *v {
							if index == CurrentIndex {
								continue
							}
							Data[z] = d
							z++
						}
						Loaded[k] = &Data
						IndexFilePath := path.Join(Base, "dbs", DatabaseName, TableName, "i", i.Name, x)
						f, err := os.Create(IndexFilePath)
						if err != nil {
							panic(err)
						}
						b, err := json.Marshal(&Loaded)
						if err != nil {
							panic(err)
						}
						_, err = f.Write(b)
						if err != nil {
							panic(err)
						}
						i.IndexLock.Unlock()
						return
					}
				}
			}
			x++
		}
	}

	// Unlocks the index lock.
	i.IndexLock.Unlock()
}
