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

// Inserts a item into the database.
func (d *DBCore) Insert(DatabaseName string, TableName string, Key string, Item *interface{}) *error {
	// Checks the table exists.
	if d.Table(DatabaseName, TableName) == nil {
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

	// TODO: Index insertion here.

	// Unlocks the table.
	lock.Unlock()

	// Everything worked! Return a null for error.
	return nil
}
