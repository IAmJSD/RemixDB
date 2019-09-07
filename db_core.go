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
	for _, db := range dbs {
		for _, table := range db.Tables {
			for _, index := range table.Indexes {
				index.Init(join, db.Name, table.Name)
			}
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

	// Checks the table exists.
	if d.Table(DatabaseName, TableName) == nil {
		err := errors.New(`The table "` + TableName + `" does not exist.`)
		return nil, &err
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

// Deletes a index.
func (d *DBCore) DeleteIndex(DatabaseName string, TableName string, IndexName string) *error {
	// Gets the table lock.
	lock := d.GetTableLock(DatabaseName, TableName)
	lock.Lock()

	// Gets the table.
	table := d.Table(DatabaseName, TableName)
	if table == nil {
		err := errors.New(`The table "` + TableName + `" does not exist.`)
		lock.Unlock()
		return &err
	}

	// Deletes the index if it exists.
	exists := false
	for index, v := range table.Indexes {
		if v.Name == IndexName {
			NewIndexArray := make([]*Index, len(table.Indexes) - 1)
			x := 0
			for i, v := range table.Indexes {
				if i == index {
					continue
				}
				NewIndexArray[x] = v
				x++
			}
			table.Indexes = NewIndexArray
			d.ArrayLock.Lock()
			for _, obj := range *d.Structure {
				if obj.Name == DatabaseName {
					for i, o := range obj.Tables {
						if o.Name == TableName {
							obj.Tables[i] = table
						}
					}
				}
			}
			d.ArrayLock.Unlock()
			d.SaveStructure()
			exists = true
			break
		}
	}
	if !exists {
		err := errors.New(`The index "` + TableName + `" does not exist.`)
		lock.Unlock()
		return &err
	}

	// Unlocks the table lock.
	lock.Unlock()

	// Do some filesystem garbage collection (this can be in the background).
	err := os.RemoveAll(path.Join(d.Base, "dbs", DatabaseName, TableName, "i", IndexName))
	if err != nil {
		panic(err)
	}

	// Yay, no errors!
	return nil
}

// Creates a index.
func (d *DBCore) CreateIndex(DatabaseName string, TableName string, IndexName string, Keys []string) *error {
	// Gets the table lock.
	lock := d.GetTableLock(DatabaseName, TableName)
	lock.Lock()

	// Lock the database slice.
	d.ArrayLock.Lock()

	// Go through all the databases.
	for _, db := range *d.Structure {
		if db.Name == DatabaseName {
			for _, table := range db.Tables {
				if table.Name == TableName {
					for _, index := range table.Indexes {
						if index.Name == IndexName {
							err := errors.New(`The index "` + IndexName + `" already exists.`)
							d.ArrayLock.Unlock()
							lock.Unlock()
							return &err
						}
					}

					i := Index{
						Name:            IndexName,
						Keys:            Keys,
						IndexLock:       nil,
						MapPreload:      nil,
						CurrentIndexDoc: 0,
					}
					table.Indexes = append(table.Indexes, &i)
					i.Init(d.Base, DatabaseName, TableName)

					d.ArrayLock.Unlock()
					lock.Unlock()
					d.SaveStructure()
					return nil
				}
			}
			d.ArrayLock.Unlock()
			lock.Unlock()
			err := errors.New(`The table "` + TableName + `" does not exist.`)
			return &err
		}
	}

	// Unlock the database slice.
	d.ArrayLock.Unlock()

	// Unlocks the table lock.
	lock.Unlock()

	// Throw an error.
	err := errors.New(`The database "` + DatabaseName + `" does not exist.`)
	return &err
}

// Deletes a table.
func (d *DBCore) DeleteTable(DatabaseName string, TableName string) *error {
	// Locks the array.
	d.ArrayLock.Lock()

	for _, db := range *d.Structure {
		if DatabaseName == db.Name {
			for index, table := range db.Tables {
				if table.Name == TableName {
					NewTableArray := make([]*Table, len(db.Tables) - 1)
					x := 0
					for i, v := range db.Tables {
						if i == index {
							continue
						}
						NewTableArray[x] = v
						x++
					}
					db.Tables = NewTableArray
					d.ArrayLock.Unlock()
					d.SaveStructure()
					err := os.RemoveAll(path.Join(d.Base, "dbs", DatabaseName, TableName))
					if err != nil {
						panic(err)
					}
					return nil
				}
			}
			d.ArrayLock.Unlock()
			err := errors.New(`The table "` + TableName + `" does not exist.`)
			return &err
		}
	}

	// Returns an error.
	d.ArrayLock.Unlock()
	err := errors.New(`The database "` + DatabaseName + `" does not exist.`)
	return &err
}

// Deletes a database.
func (d *DBCore) DeleteDatabase(DatabaseName string) *error {
	// Locks the array.
	d.ArrayLock.Lock()

	for index, db := range *d.Structure {
		if DatabaseName == db.Name {
			NewDBArray := make([]*DBStructure, len(*d.Structure) - 1)
			x := 0
			for i, v := range *d.Structure {
				if i == index {
					continue
				}
				NewDBArray[x] = v
				x++
			}
			d.Structure = &NewDBArray
			d.ArrayLock.Unlock()
			d.SaveStructure()
			err := os.RemoveAll(path.Join(d.Base, "dbs", DatabaseName))
			if err != nil {
				panic(err)
			}
			return nil
		}
	}

	// Returns an error.
	d.ArrayLock.Unlock()
	err := errors.New(`The database "` + DatabaseName + `" does not exist.`)
	return &err
}

// TODO: GetAllByIndex
// TODO: GetAll
// TODO: TableKeys
