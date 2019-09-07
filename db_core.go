// This handles the core DB functionality.
// None of the core functionality accounts for what shard stuff goes onto or anything else sharding related - consult sharding.go for that!

package main

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

var Core *DBCore

type DBStructure struct {
	Name string `json:"n"`
	Tables []*Table `json:"t"`
}

type Index struct {
	Name string `json:"n"`
	Keys []string `json:"k"`
	IndexLock *sync.Mutex `json:"-"`
	MapPreload *map[string]*[]string `json:"-"`
	CurrentIndexDoc int `json:"-"`
}

type Table struct {
	Name string `json:"n"`
	Indexes []*Index `json:"i"`
}

type DBCore struct {
	Base string
	Structure *[]*DBStructure
	BaseFSLock *sync.Mutex
	ArrayLock *sync.Mutex
	TableLocksLock *sync.Mutex
	DBTableLockMap *map[string]*map[string]*sync.Mutex
}

// Creates the DB core.
func NewDBCore() {
	x, _ := os.Getwd()
	join := path.Join(x, "remixdb_data")
	if _, err := os.Stat(join); os.IsNotExist(err) {
		// Folder doesn't exist. Make the folder.
		err := os.Mkdir(join, 0777)
		if err != nil {
			panic(err)
		}
	}
	if _, err := os.Stat(path.Join(join, "dbs")); os.IsNotExist(err) {
		// Folder doesn't exist. Make the folder.
		err := os.Mkdir(path.Join(join, "dbs"), 0777)
		if err != nil {
			panic(err)
		}
	}
	dbs := make([]*DBStructure, 0)
	structure := path.Join(join, "structure")
	BaseFSLock := sync.Mutex{}
	ArrayLock := sync.Mutex{}
	TableLocksLock := sync.Mutex{}
	Core = &DBCore{
		Base:  join,
		Structure: &dbs,
		BaseFSLock:  &BaseFSLock,
		ArrayLock: &ArrayLock,
		TableLocksLock: &TableLocksLock,
		DBTableLockMap: &map[string]*map[string]*sync.Mutex{},
	}
	if _, err := os.Stat(structure); os.IsNotExist(err) {
		// Lets create the DB structure.
		Core.SaveStructure()
	} else {
		// Lets load the DB structure.
		d, err := ioutil.ReadFile(structure)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(d, &dbs)
		if err != nil {
			panic(err)
		}
	}
}

// Get a copy of the DB structure if it exists.
func (d *DBCore) Database(Database string) *DBStructure {
	// Locks the array lock.
	d.ArrayLock.Lock()

	// Defines the DB structure.
	var db DBStructure

	// Defines if a item was found.
	found := false

	// Gets the database from the array if it exists.
	for _, v := range *d.Structure {
		if v.Name == Database {
			db = *v
			found = true
			break
		}
	}

	// Unlocks the array lock.
	d.ArrayLock.Unlock()

	// Creates a pointer if found.
	var ptr *DBStructure
	if found {
		ptr = &db
	}

	// Returns the pointer.
	return ptr
}

// Saves a copy of the DB structure.
func (d *DBCore) SaveStructure() {
	// Locks the base FS lock.
	d.BaseFSLock.Lock()

	// Saves the current structure.
	f, err := os.Create(path.Join(d.Base, "structure"))
	if err != nil {
		panic(err)
	}
	data, err := json.Marshal(d.Structure)
	if err != nil {
		panic(err)
	}
	_, err = f.Write(data)
	if err != nil {
		panic(err)
	}

	// Unlocks the base FS lock.
	d.BaseFSLock.Unlock()
}

// Gets the table structure if it exists.
func (d *DBCore) Table(DatabaseName string, TableName string) *Table {
	// Get the DB.
	db := d.Database(DatabaseName)
	if db == nil {
		return nil
	}

	// Tries to find the table.
	var ptr *Table
	for _, v := range db.Tables {
		if v.Name == TableName {
			// Clone and create a pointer.
			clone := *v
			ptr = &clone
		}
	}

	// Returns the pointer.
	return ptr
}

// Creates a database.
func (d *DBCore) CreateDatabase(DatabaseName string) *error {
	// Check if the database already exists.
	if d.Database(DatabaseName) != nil {
		err := errors.New(`The database "` + DatabaseName + `" already exists.`)
		return &err
	}

	// Locks the array lock.
	d.ArrayLock.Lock()

	// Append the database to the DB array.
	appended := append(*d.Structure, &DBStructure{
		Name:   DatabaseName,
		Tables: []*Table{},
	})
	d.Structure = &appended

	// Unlocks the array lock.
	d.ArrayLock.Unlock()

	// Saves the structure.
	d.SaveStructure()

	// Locks the base FS lock.
	d.BaseFSLock.Lock()

	// Creates all the needed folders.
	DBFolder := path.Join(d.Base, "dbs", DatabaseName)
	err := os.Mkdir(DBFolder, 0777)
	if err != nil {
		panic(err)
	}

	// Unlocks the base FS lock.
	d.BaseFSLock.Unlock()

	// Yay no errors! Return a null pointer for the error.
	return nil
}

// Creates a table.
func (d *DBCore) CreateTable(DatabaseName string, TableName string) *error {
	// Get the database relating to this table.
	db := d.Database(DatabaseName)
	if db == nil {
		err := errors.New(`The database "` + DatabaseName + `" does not exist.`)
		return &err
	}

	// Locks the array lock.
	d.ArrayLock.Lock()

	// Iterate all the tables, see if it already exists.
	for _, v := range db.Tables {
		if v.Name == TableName {
			err := errors.New(`The table "` + TableName + `" already exists.`)
			d.ArrayLock.Unlock()
			return &err
		}
	}

	// Append the table and create the structure. Since the database item is a copy, we will iterate through to find it again.
	// This might make making tables a few nanoseconds longer - but hey, it's not an action that is likely to need speed.
	for _, v := range *d.Structure {
		if v.Name == DatabaseName {
			v.Tables = append(v.Tables, &Table{
				Name: TableName,
				Indexes: []*Index{},
			})
			break
		}
	}
	TableDir := path.Join(d.Base, "dbs", DatabaseName, TableName)
	err := os.Mkdir(TableDir, 0777)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(path.Join(TableDir, "r"), 0777)
	if err != nil {
		panic(err)
	}
	err = os.Mkdir(path.Join(TableDir, "i"), 0777)
	if err != nil {
		panic(err)
	}

	// Unlocks the array lock.
	d.ArrayLock.Unlock()

	// Save the structure.
	d.SaveStructure()

	// Yay no errors! Return a null pointer for the error.
	return nil
}

// Gets the table lock and creates one if it does not exist.
func (d *DBCore) GetTableLock(DatabaseName string, TableName string) *sync.Mutex {
	// Locks the table locks lock.
	d.TableLocksLock.Lock()

	// Defines the lock.
	DB := (*d.DBTableLockMap)[DatabaseName]
	if DB == nil {
		DB = &map[string]*sync.Mutex{}
		(*d.DBTableLockMap)[DatabaseName] = DB
	}
	lock := (*DB)[TableName]
	if lock == nil {
		lock = &sync.Mutex{}
		(*DB)[TableName] = lock
	}

	// Unlocks the table locks lock.
	d.TableLocksLock.Unlock()

	// Returns the lock.
	return lock
}

// Gets a item from a table.
func (d *DBCore) Get(DatabaseName string, TableName string, Item string) (*interface{}, *error) {
	// Checks the table exists.
	if d.Table(DatabaseName, TableName) == nil {
		err := errors.New(`The table "` + TableName + `" does not exist.`)
		return nil, &err
	}

	// Defines what it will be marshalled into.
	var item interface{}

	// Defines the cache key.
	CacheKey := DatabaseName + ":" + TableName + ":" + Item

	// See if the item is in the cache.
	CacheResult := Cache.Get(CacheKey)
	if CacheResult != nil {
		err := json.Unmarshal(*CacheResult, &item)
		if err != nil {
			panic(err)
		}
		return &item, nil
	}

	// Try and get the item from the filesystem.
	lock := d.GetTableLock(DatabaseName, TableName)
	lock.Lock()
	ItemDir := path.Join(d.Base, "dbs", DatabaseName, TableName, "r", Item)
	if _, err := os.Stat(ItemDir); os.IsNotExist(err) {
		err := errors.New("The item specified does not exist.")
		lock.Unlock()
		return nil, &err
	}
	data, err := ioutil.ReadFile(ItemDir)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(data, &item)
	if err != nil {
		panic(err)
	}
	Cache.Set(CacheKey, data)
	lock.Unlock()

	// Return the value.
	return &item, nil
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
	i.Init(Base, DatabaseName, TableName)
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

// Inserts a item into the database.
func (d *DBCore) Insert(DatabaseName string, TableName string, Key string, Item *interface{}) *error {
	// Checks the table exists.
	Table := d.Table(DatabaseName, TableName)
	if Table == nil {
		err := errors.New(`The table "` + TableName + `" does not exist.`)
		return &err
	}

	// Check if a record exists already.
	r, _ := d.Get(DatabaseName, TableName, Key)
	if r != nil {
		err := errors.New(`The record "` + Key + `" already exists.`)
		return &err
	}

	// Locks the table.
	lock := d.GetTableLock(DatabaseName, TableName)
	lock.Lock()

	// Inserts the item into the filesystem.
	ItemDir := path.Join(d.Base, "dbs", DatabaseName, TableName, "r", Key)
	f, err := os.Create(ItemDir)
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(Item)
	if err != nil {
		panic(err)
	}
	_, err = f.Write(b)
	if err != nil {
		panic(err)
	}

	// Unlocks the table.
	lock.Unlock()

	// Inserts into any relevant indexes if needed.
	cast, ok := (*Item).(map[string]interface{})
	if ok {
		for _, v := range Table.Indexes {
			IndexBy := make([]interface{}, 0)
			IndexFits := true
			for _, k := range v.Keys {
				if cast[k] == nil {
					IndexFits = false
					break
				} else {
					IndexBy = append(IndexBy, cast[k])
				}
			}

			// Why? Fuck knows. Go dislikes having interface{} [] as a type for a map key apparently.
			j, err := json.Marshal(IndexBy)
			if err != nil {
				panic(err)
			}

			if IndexFits {
				v.Insert(d.Base, DatabaseName, TableName, string(j), Key)
			}
		}
	}

	// Everything worked! Return a null for error.
	return nil
}

// Deletes an item from this index.
func (i *Index) DeleteItem(Base string, DatabaseName string, TableName string, Item string) {
	// Initialises the index.
	i.Init(Base, DatabaseName, TableName)

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

// Deletes a record from a table.
func (d *DBCore) DeleteRecord(DatabaseName string, TableName string, Item string) *error {
	// Check if the item actually exists.
	record, err := d.Get(DatabaseName, TableName, Item)
	if err != nil {
		return err
	}

	// Locks the table.
	lock := d.GetTableLock(DatabaseName, TableName)
	lock.Lock()

	// Deletes the record.
	e := os.Remove(path.Join(d.Base, "dbs", DatabaseName, TableName, "r", Item))
	if e != nil {
		panic(e)
	}

	// Unlocks the table.
	lock.Unlock()

	// Checks if any indexes are used and handles them if they are.
	cast, ok := (*record).(map[string]interface{})
	if ok {
		Table := d.Table(DatabaseName, TableName)
		for _, v := range Table.Indexes {
			IndexFits := true
			for _, k := range v.Keys {
				if cast[k] == nil {
					IndexFits = false
					break
				}
			}

			if IndexFits {
				v.DeleteItem(d.Base, DatabaseName, TableName, Item)
			}
		}
	}

	// Wipe the item from the cache.
	Cache.Delete(DatabaseName + ":" + TableName + ":" + Item)

	// Yay! Return a null pointer for errors.
	return nil
}
