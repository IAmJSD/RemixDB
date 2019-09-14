package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Does the shard calculations including replicas.
func HandleShardCalculation(Key string, Shards []string, Replicas int) []string {
	// If the replica count is the same as the shard count, return the shards array.
	ShardLen := len(Shards)
	if ShardLen == Replicas {
		return Shards
	}

	// Calculate the key as an int32.
	var KeyIntValue int32
	for _, v := range Key {
		KeyIntValue += v
	}

	// Get the replicas.
	ShardDiff := Shards
	ShardsContaining := make([]string, Replicas)
	x := 0
	for x != Replicas {
		ShardDiffLen := len(ShardDiff)
		ShardID := int(KeyIntValue % int32(ShardDiffLen))
		NewShardDiff := make([]string, ShardDiffLen-1)
		CurrentIndex := 0
		for i, v := range ShardDiff {
			if i == ShardID {
				ShardsContaining[x] = v
				continue
			}
			NewShardDiff[CurrentIndex] = v
			CurrentIndex++
		}
		ShardDiff = NewShardDiff
		x++
	}

	// Returns all the shards that contain the data.
	return ShardsContaining
}

// Defines the shard structure.
type Shard struct {
	Shards        []string                   `json:"s"`
	ActiveShards  []string                   `json:"as"`
	ShardURLS     map[string]string          `json:"su"`
	IAm           int                        `json:"iam"`
	ReplicaConfig map[string]*map[string]int `json:"r"`
}

// Defines all used variables.
var (
	ShardInstance     *Shard
	InnerClusterToken = os.Getenv("INNER_CLUSTER_TOKEN")
	OtherShardURL     = os.Getenv("OTHER_SHARD_URL")
	ThisShardURL      = os.Getenv("THIS_SHARD_URL")
	HTTPClient        = http.Client{}
	UptimeMap         = map[string]*int{}
	UptimeMutex 	  = sync.RWMutex{}
)

// Tries to get the latency of a shard.
// A null pointer means it is offline.
func GetShardLatency(ShardURL string) *int {
	u, err := url.Parse(ShardURL)
	if err != nil {
		panic(err)
	}
	Start := time.Now()
	u.Path = "/_shard/ping"
	resp, err := http.Get(u.String())
	if err != nil {
		return nil
	}
	if resp.StatusCode != 204 {
		return nil
	}
	ns := (time.Now().Nanosecond() - Start.Nanosecond()) / 1000000
	return &ns
}

// Joins this shard to a cluster.
func JoinCluster() {
	println("New config and cluster information detected. Attempting to join cluster!")
	u, err := url.Parse(OtherShardURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/_shard/config"
	client, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
	req, err := HTTPClient.Do(client)
	if err != nil {
		panic(err)
	}
	if req.StatusCode != 200 {
		panic("The other shard responded with a status " + string(req.StatusCode))
	}

	var OtherShard Shard
	defer req.Body.Close()
	var Data []byte
	_, err = req.Body.Read(Data)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(Data, &OtherShard)
	if err != nil {
		panic(err)
	}
	UUID := uuid.Must(uuid.NewV4()).String()
	ShardID := OtherShard.Shards[OtherShard.IAm]
	OtherShard.ShardURLS[ShardID] = OtherShardURL
	OtherShard.Shards = append(OtherShard.Shards, UUID)
	OtherShard.IAm = len(OtherShard.Shards) - 1
	ShardInstance = &OtherShard

	for _, x := range OtherShard.ShardURLS {
		ptr := GetShardLatency(x)
		if ptr == nil {
			panic("A shard is down in your cluster. Please fix this before adding a new shard.")
		}
	}

	err = Core.Insert("__internal", "sharding", "config", ToInterfacePtr(ShardInstance))
	if err != nil {
		panic(err)
	}

	u.Path = "/_shard/dbs"
	client, err = http.NewRequest("GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
	req, err = HTTPClient.Do(client)
	if err != nil {
		panic(err)
	}
	if req.StatusCode != 200 {
		panic("The other shard responded with a status " + string(req.StatusCode))
	}
	req, err = HTTPClient.Do(client)
	if err != nil {
		panic(err)
	}
	var Databases []*DBStructure
	err = json.Unmarshal(Data, &Databases)
	if err != nil {
		panic(err)
	}
	for _, v := range Databases {
		err := Core.CreateDatabase(v.Name)
		if err != nil {
			panic(err)
		}
		for _, t := range v.Tables {
			err = Core.CreateTable(v.Name, t.Name)
			if err != nil {
				panic(err)
			}
			for _, i := range t.Indexes {
				err = Core.CreateIndex(v.Name, t.Name, i.Name, i.Keys)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	println("Orchestrating reshard - Do NOT close the database while this runs. Like seriously, do NOT close it, you WILL likely lose data or have integrity issues with it.")
	for _, x := range ShardInstance.ShardURLS {
		u, err := url.Parse(x)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/new"
		ReqBody := map[string]interface{}{
			"ShardID":  UUID,
			"ShardURL": ThisShardURL,
		}
		b, err := json.Marshal(&ReqBody)
		if err != nil {
			panic(err)
		}
		client, err := http.NewRequest("POST", u.String(), bytes.NewReader(b))
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	for _, x := range ShardInstance.ShardURLS {
		u, err := url.Parse(x)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/ready/" + ShardInstance.Shards[ShardInstance.IAm]
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	println("Reshard orchestration complete. Welcome to the cluster!")
}

// Marks a shard as ready.
func MarkShardAsReady(ShardID string) {
	ShardInstance.ActiveShards = append(ShardInstance.ActiveShards, ShardID)
	err := Core.DeleteRecord("__internal", "sharding", "config")
	if err != nil {
		panic(err)
	}
	err = Core.Insert("__internal", "sharding", "config", ToInterfacePtr(ShardInstance))
	if err != nil {
		panic(err)
	}
}

// Inserts into a remote shard.
func InsertRemoteShardReshard(ShardID string, Item interface{}, Key string) {
	u, err := url.Parse(ShardInstance.ShardURLS[ShardID])
	if err != nil {
		panic(err)
	}
	u.Path = "/_shard/insert"
	I := map[string]interface{}{
		"data": Item,
		"Key":  Key,
	}
	b, err := json.Marshal(&I)
	if err != nil {
		panic(err)
	}
	client, err := http.NewRequest("POST", u.String(), bytes.NewReader(b))
	if err != nil {
		panic(err)
	}
	client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
	req, err := HTTPClient.Do(client)
	if err != nil {
		panic(err)
	}
	if req.StatusCode > 399 && 600 > req.StatusCode {
		if req.StatusCode == 409 {
			return
		}
		panic(req)
	}
}

// Get the replica count.
func GetReplicas(DatabaseName string, TableName string) int {
	DBInfo := ShardInstance.ReplicaConfig[DatabaseName]
	if DBInfo == nil {
		DBInfo = &map[string]int{}
		ShardInstance.ReplicaConfig[DatabaseName] = DBInfo
	}
	Replicas := (*DBInfo)[TableName]
	if Replicas == 0 {
		return 1
	}
	return Replicas
}

// Handles resharding.
func Reshard() {
	println("Resharding - Do NOT close the database while this runs. Like seriously, do NOT close it, you WILL likely lose data or have integrity issues with it.")
	Core.ArrayLock.Lock()
	var Databases []string
	for _, v := range *Core.Structure {
		Databases = append(Databases, v.Name)
	}
	Core.ArrayLock.Unlock()
	for _, v := range Databases {
		db := Core.Database(v)
		for _, x := range db.Tables {
			keys, err := Core.TableKeys(v, x.Name)
			if err != nil {
				panic(err)
			}
			for _, k := range keys {
				shards := HandleShardCalculation(k, ShardInstance.Shards, GetReplicas(v, x.Name))
				ContainsMe := false
				for _, v := range shards {
					if v == ShardInstance.Shards[ShardInstance.IAm] {
						ContainsMe = true
						break
					}
				}
				if !ContainsMe {
					for _, v := range shards {
						i, err := Core.Get(v, x.Name, k)
						if err != nil {
							panic(err)
						}
						InsertRemoteShardReshard(v, i, k)
					}
				}
			}
		}
	}
	println("Resharding complete.")
}

// Inserts a new shard and runs reshard.
func InsertShard(ShardID string, ShardURL string) {
	u, err := url.Parse(ShardURL)
	if err != nil {
		panic(err)
	}
	u.Path = "/"
	ShardInstance.Shards = append(ShardInstance.Shards, ShardID)
	ShardInstance.ShardURLS[ShardID] = ShardURL
	err = Core.DeleteRecord("__internal", "sharding", "config")
	if err != nil {
		panic(err)
	}
	err = Core.Insert("__internal", "sharding", "config", ToInterfacePtr(ShardInstance))
	if err != nil {
		panic(err)
	}
	Reshard()
}

// Initialises the shard.
func ShardInit() {
	if Core.Database("__internal") == nil {
		err := Core.CreateDatabase("__internal")
		if err != nil {
			panic(err)
		}
	}

	if Core.Table("__internal", "sharding") == nil {
		err := Core.CreateTable("__internal", "sharding")
		if err != nil {
			panic(err)
		}
		if InnerClusterToken == "" || OtherShardURL == "" {
			err = Core.Insert("__internal", "sharding", "config", ToInterfacePtr(Shard{
				Shards:        []string{uuid.Must(uuid.NewV4()).String()},
				ActiveShards:  []string{uuid.Must(uuid.NewV4()).String()},
				ShardURLS:     map[string]string{},
				IAm:           0,
				ReplicaConfig: map[string]*map[string]int{},
			}))
			if err != nil {
				panic(err)
			}
		} else {
			JoinCluster()
		}
	}

	r, err := Core.Get("__internal", "sharding", "config")
	if err != nil {
		panic(err)
	}

	// Idk why, converting to JSON and back solves this. It's fine, this runs once at boot. I can live with this.
	b, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	var s Shard
	err = json.Unmarshal(b, &s)
	if err != nil {
		panic(err)
	}

	ShardInstance = &s

	for _, v := range ShardInstance.ShardURLS {
		go ExecuteShardHeartbeat(v)
	}
}

// Executes a shard heartbeat.
func ExecuteShardHeartbeat(URL string) {
	Heartbeat := GetShardLatency(URL)
	if Heartbeat == nil {
		println("[" + URL + "] Shard is down!")
	}
	UptimeMutex.Lock()
	UptimeMap[URL] = Heartbeat
	UptimeMutex.Unlock()
	time.Sleep(time.Second)
	go ExecuteShardHeartbeat(URL)
}

// Defines the response from a remote shard.
type RemoteShardGetResponse struct {
	Err  *string      `json:"error"`
	Data *interface{} `json:"data"`
}

// Gets a item from a table.
func (s *Shard) Get(DatabaseName string, TableName string, Item string) (*interface{}, error) {
	Replicas := GetReplicas(DatabaseName, TableName)
	Shards := HandleShardCalculation(Item, s.Shards, Replicas)
	for _, v := range Shards {
		if s.ShardURLS[v] == "" {
			// Me!
			return Core.Get(DatabaseName, TableName, Item)
		}
	}
	var RemoteShard string
	var Ping *int
	for _, v := range Shards {
		UptimeMutex.RLock()
		if Ping == nil || (UptimeMap[v] != nil && *Ping > *UptimeMap[v]) {
			RemoteShard = v
		}
		UptimeMutex.RUnlock()
	}

	if Ping == nil {
		return nil, errors.New("All shards holding data are down!")
	}

	// This is specifically for a remote shard. Let the remote shard respond.
	u, err := url.Parse(RemoteShard)
	if err != nil {
		panic(err)
	}
	u.Path = "/_shard/get?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName) + "&item=" + url.QueryEscape(Item)
	client, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		panic(err)
	}
	client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
	req, err := HTTPClient.Do(client)
	if err != nil {
		panic(err)
	}
	if req.StatusCode != 200 {
		panic("The other shard responded with a status " + string(req.StatusCode))
	}
	var Response RemoteShardGetResponse
	var Data []byte
	_, err = req.Body.Read(Data)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(Data, &Response)
	if err != nil {
		panic(err)
	}
	if Response.Err != nil {
		return nil, errors.New(*Response.Err)
	}
	err = req.Body.Close()
	if err != nil {
		panic(err)
	}
	return Response.Data, nil
}

// Gets the DB structure if it exists. We can get this from the local instance.
func (s *Shard) Database(DatabaseName string) *DBStructure {
	return Core.Database(DatabaseName)
}

// Gets the table structure if it exists. We can get this from the local instance.
func (s *Shard) Table(DatabaseName string, TableName string) *Table {
	return Core.Table(DatabaseName, TableName)
}

// Creates a database on all shards.
func (s *Shard) CreateDatabase(DatabaseName string) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before creating a database.")
		}
	}
	UptimeMutex.RUnlock()
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/new_db?db=" + url.QueryEscape(DatabaseName)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return Core.CreateDatabase(DatabaseName)
}

// Creates a index on all shards.
func (s *Shard) CreateIndex(DatabaseName string, TableName string, IndexName string, Keys []string) error  {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before creating a index.")
		}
	}
	UptimeMutex.RUnlock()
	err := Core.CreateIndex(DatabaseName, TableName, IndexName, Keys)
	if err != nil {
		return err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		b, err := json.Marshal(&Keys)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/new_index?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName) + "&index=" + url.QueryEscape(IndexName) + "&keys=" + url.QueryEscape(string(b))
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return nil
}

// Creates a table on all shards.
func (s *Shard) CreateTable(DatabaseName string, TableName string) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before creating a table.")
		}
	}
	UptimeMutex.RUnlock()
	err := Core.CreateTable(DatabaseName, TableName)
	if err != nil {
		return err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/new_table?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return nil
}

// Delete a database on all shards.
func (s *Shard) DeleteDatabase(DatabaseName string) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before deleting a database.")
		}
	}
	UptimeMutex.RUnlock()
	err := Core.DeleteDatabase(DatabaseName)
	if err != nil {
		return err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/delete_db?db=" + url.QueryEscape(DatabaseName)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return nil
}

// Delete a index on all shards.
func (s *Shard) DeleteIndex(DatabaseName string, TableName string, IndexName string) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before deleting a index.")
		}
	}
	UptimeMutex.RUnlock()
	err := Core.DeleteIndex(DatabaseName, TableName, IndexName)
	if err != nil {
		return err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/delete_index?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName) + "&index=" + url.QueryEscape(IndexName)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return nil
}

// Deletes a record from all shards.
func (s *Shard) DeleteRecord(DatabaseName string, TableName string, Item string) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before deleting a record.")
		}
	}
	UptimeMutex.RUnlock()
	err := Core.DeleteRecord(DatabaseName, TableName, Item)
	if err != nil {
		return err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/delete_record?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName) + "&index=" + url.QueryEscape(Item)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return nil
}

// Deletes a table from all shards.
func (s *Shard) DeleteTable(DatabaseName string, TableName string) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before deleting a table.")
		}
	}
	UptimeMutex.RUnlock()
	err := Core.DeleteTable(DatabaseName, TableName)
	if err != nil {
		return err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/delete_table?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 204 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
	}
	return nil
}

// Gets all table keys.
func (s *Shard) TableKeys(DatabaseName string, TableName string) (*[]string, error) {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return nil, errors.New("A shard is down. Please fix this before getting table keys.")
		}
	}
	UptimeMutex.RUnlock()
	keys, err := Core.TableKeys(DatabaseName, TableName)
	if err != nil {
		return nil, err
	}
	for _, v := range s.ShardURLS {
		u, err := url.Parse(v)
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/table_keys?db=" + url.QueryEscape(DatabaseName) + "&table=" + url.QueryEscape(TableName)
		client, err := http.NewRequest("GET", u.String(), nil)
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 200 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
		var Data []byte
		_, err = req.Body.Read(Data)
		if err != nil {
			panic(err)
		}
		var StringArr []string
		err = json.Unmarshal(Data, &StringArr)
		if err != nil {
			panic(err)
		}
		for _, v := range StringArr {
			keys = append(keys, v)
		}
		err = req.Body.Close()
		if err != nil {
			panic(err)
		}
	}
	return &keys, nil
}

// Insert into all shards. *click, nice*
func (s *Shard) Insert(DatabaseName string, TableName string, Key string, Item *interface{}) error {
	UptimeMutex.RLock()
	for _, v := range UptimeMap {
		if v == nil {
			return errors.New("A shard is down. Please fix this before inserting.")
		}
	}
	UptimeMutex.RUnlock()

	Shards := HandleShardCalculation(Key, s.Shards, GetReplicas(DatabaseName, TableName))

	for _, k := range Shards {
		if s.ShardURLS[k] == "" {
			err := Core.Insert(DatabaseName, TableName, Key, Item)
			if err != nil {
				return err
			}
			continue
		}

		u, err := url.Parse(s.ShardURLS[k])
		if err != nil {
			panic(err)
		}
		u.Path = "/_shard/insert"
		POSTBody := map[string]interface{}{
			"DB": DatabaseName,
			"Table": TableName,
			"Key": Key,
			"Item": Item,
		}
		b, err := json.Marshal(&POSTBody)
		client, err := http.NewRequest("GET", u.String(), bytes.NewReader(b))
		if err != nil {
			panic(err)
		}
		client.Header.Set("Inner-Cluster-Token", InnerClusterToken)
		req, err := HTTPClient.Do(client)
		if err != nil {
			panic(err)
		}
		if req.StatusCode != 200 {
			panic("The other shard responded with a status " + string(req.StatusCode))
		}
		var Data []byte
		_, err = req.Body.Read(Data)
		if err != nil {
			panic(err)
		}
		var InsertResponse *string
		err = json.Unmarshal(Data, &InsertResponse)
		if err != nil {
			panic(err)
		}
		err = req.Body.Close()
		if err != nil {
			panic(err)
		}
		if InsertResponse != nil {
			return errors.New(*InsertResponse)
		}
	}
	return nil
}
