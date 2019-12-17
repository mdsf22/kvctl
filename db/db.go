package db

import (
	"fmt"
	"reflect"

	"github.com/magiconair/properties"
)

//KVDBCreator creates a database layer.
type KVDBCreator interface {
	Create(p *properties.Properties) KVDB
}

//KVDB is the layer to access the database
type KVDB interface {
	Get(key []byte) (val []byte, err error)
	Put(key []byte, val []byte) (err error)
	Del(key []byte) (err error)
	GetN(num int)
	PutN(num int)
	Test(total int, num int)
}

var dbCreators = map[string]KVDBCreator{}

// RegisterDBCreator registers a creator for the database
func RegisterDBCreator(name string, creator KVDBCreator) {
	_, ok := dbCreators[name]
	if ok {
		panic(fmt.Sprintf("duplicate register database %s", name))
	}

	dbCreators[name] = creator
}

// GetDBCreator gets the DBCreator for the database
func GetDBCreator(name string) KVDBCreator {
	return dbCreators[name]
}

//Display ...
func Display(i interface{}) {
	var kv = make(map[string]interface{})
	vValue := reflect.ValueOf(i)
	vType := reflect.TypeOf(i)
	for i := 0; i < vValue.NumField(); i++ {
		kv[vType.Field(i).Name] = vValue.Field(i)
	}
	fmt.Println("==========")
	for k, v := range kv {
		fmt.Print(k)
		fmt.Print(":")
		fmt.Print(v)
		fmt.Println()
	}
}
