package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
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
}
