package leveldb

import (
	"fmt"
	"github.com/magiconair/properties"
	"github.com/mdsf22/kvctl/db"
)

const (
	levelDbDir  = "level.dir"
	levelDbStat = "level.stat"
)

type leveldbCreator struct {
}

func (c leveldbCreator) Create(p *properties.Properties) db.KVDB {
	file := p.GetString(levelDbDir, "path/")
	keypre := p.GetString("keypre", "test")
	threads := p.GetInt("threads", 10)
	vallen := p.GetInt("vallen", 1024)

	fmt.Println("===============================")
	fmt.Println("db: level")
	fmt.Println("file: ", file)
	fmt.Println("threads: ", threads)
	fmt.Println("keypre: ", keypre)
	fmt.Println("===============================")

	return NewLevelDb(p, file, threads, keypre, vallen)
}

func init() {
	db.RegisterDBCreator("leveldb", leveldbCreator{})
}
