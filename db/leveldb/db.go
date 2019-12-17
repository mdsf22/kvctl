package leveldb

import (
	"fmt"
	"os"
	"sync"
	"github.com/mdsf22/kvctl/db"
	"github.com/mdsf22/kvctl/stat"
	"time"

	"github.com/magiconair/properties"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

//LevelDb ...
type LevelDb struct {
	p       *properties.Properties
	dataDir string
	threads int
	keypre  string
	vallen  int
}

//NewLevelDb ...
func NewLevelDb(p *properties.Properties, dataDir string, threads int, keypre string, vallen int) db.KVDB {
	db := &LevelDb{
		p:       p,
		dataDir: dataDir,
		threads: threads,
		keypre:  keypre,
		vallen:  vallen}
	return db
}

//Get ... get value
func (c *LevelDb) Get(key []byte) (val []byte, err error) {
	db, err := leveldb.OpenFile(c.dataDir, nil)
	if err != nil {
		return
	}
	defer db.Close()
	val, err = db.Get(key, nil)
	return
}

func (c *LevelDb) get2(key []byte, db *leveldb.DB) (val []byte, err error) {
	val, err = db.Get(key, nil)
	return
}

//Put ...
func (c *LevelDb) Put(key []byte, value []byte) (err error) {
	db, err := leveldb.OpenFile(c.dataDir, nil)
	if err != nil {
		return
	}
	defer db.Close()
	err = db.Put(key, value, nil)
	return
}

func (c *LevelDb) put2(key []byte, value []byte, db *leveldb.DB) (err error) {
	err = db.Put(key, value, nil)
	return
}

//Del ...
func (c *LevelDb) Del(key []byte) (err error) {
	db, err := leveldb.OpenFile(c.dataDir, nil)
	if err != nil {
		return
	}
	defer db.Close()
	err = db.Delete(key, nil)
	return
}

func (c *LevelDb) del2(key []byte, db *leveldb.DB) (err error) {
	err = db.Delete(key, nil)
	return
}

func (c *LevelDb) batchGet(result chan *stat.Histogram, index int, num int, wg *sync.WaitGroup, dbconn *leveldb.DB) (err error) {
	defer wg.Done()
	stat := stat.NewHistogram()
	for i := 0; i < num; i++ {
		key := db.K(index, c.keypre)
		index++
		start := time.Now()
		val, err := dbconn.Get([]byte(key), nil)
		lan := time.Now().Sub(start)
		if err != nil {
			fmt.Println("batchGet err:", err)
		}
		if len(val) == 0 {
			fmt.Printf("batchGet key: %s not exist\n", key)
		}
		stat.Measure(lan, int64(len(val)))
	}
	stat.Calc()
	result <- stat
	return
}

func (c *LevelDb) batchGet2(result chan *stat.Histogram, keys []string, wg *sync.WaitGroup, db *leveldb.DB) (err error) {
	defer wg.Done()
	stat := stat.NewHistogram()
	for _, key := range keys {
		start := time.Now()
		val, _ := c.get2([]byte(key), db)
		lan := time.Now().Sub(start)
		// if err != nil {
		// 	fmt.Println("get err:", err)
		// }

		stat.Measure(lan, int64(len(val)))
	}
	stat.Calc()
	result <- stat
	return
}

func (c *LevelDb) batchPut(result chan *stat.Histogram, id int, index int, num int, wg *sync.WaitGroup, dbconn *leveldb.DB) (err error) {
	defer wg.Done()
	stat := stat.NewHistogram()
	for i := 0; i < num; i++ {
		//fmt.Println("=============: ", index)
		if i%100000 == 0 {
			fmt.Println("progress: ", float32(i)/float32(num))
		}

		var val []byte
		if c.p.GetInt("valtype", 0) == 0 {
			val = db.V(c.vallen)
		} else {
			val = []byte(db.VRan(c.vallen))
		}
		key := db.K(index, c.keypre)
		index++
		start := time.Now()
		// err = db.Put(key, val, nil)
		err = dbconn.Put(key, val, nil)
		lan := time.Now().Sub(start)
		if err != nil {
			fmt.Println("put err:", err)
		}
		stat.Measure(lan, int64(len(val)))
	}
	stat.Calc()
	result <- stat
	return
}

func (c *LevelDb) batchPut2(result chan *stat.Histogram, data map[string]string, wg *sync.WaitGroup, db *leveldb.DB) (err error) {
	defer wg.Done()
	count := len(data)
	index := 0

	stat := stat.NewHistogram()
	for k, v := range data {
		index++
		if index%100000 == 0 {
			fmt.Println("progress: ", float32(index)/float32(count))
		}
		start := time.Now()
		err = c.put2([]byte(k), []byte(v), db)
		lan := time.Now().Sub(start)
		if err != nil {
			fmt.Println("put err:", err)
		}
		stat.Measure(lan, int64(len(v)))
	}
	stat.Calc()
	result <- stat
	return
}

func (c *LevelDb) batchDel2(result chan *stat.Histogram, keys []string, wg *sync.WaitGroup, db *leveldb.DB) (err error) {
	defer wg.Done()
	stat := stat.NewHistogram()
	for _, key := range keys {
		start := time.Now()
		err := c.del2([]byte(key), db)
		lan := time.Now().Sub(start)
		if err != nil {
			fmt.Println("del err:", err)
		}
		stat.Measure(lan, 0)
	}
	stat.Calc()
	result <- stat
	return
}

//GetN ...
func (c *LevelDb) GetN(num int) {
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	numPthread := num / threadCount
	result := make(chan *stat.Histogram, threadCount)
	dbconn, err := leveldb.OpenFile(c.dataDir, nil)
	if err != nil {
		return
	}
	defer dbconn.Close()
	for i := 0; i < threadCount; i++ {
		go c.batchGet(result, i*numPthread, numPthread, &wg, dbconn)
	}
	wg1.Add(1)
	go db.Statistics(result, &wg1)
	wg.Wait()
	close(result)
	wg1.Wait()
}

//PutN ...
func (c *LevelDb) PutN(num int) {
	os.RemoveAll(c.dataDir)
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	result := make(chan *stat.Histogram, threadCount)
	o := getOptions(c.p)
	db.Display(*o)
	dbconn, err := leveldb.OpenFile(c.dataDir, o)
	if err != nil {
		return
	}
	defer dbconn.Close()

	if c.p.GetInt("initdata", 0) == 0 {
		numPthread := num / threadCount
		for i := 0; i < threadCount; i++ {
			go c.batchPut(result, i, i*numPthread, numPthread, &wg, dbconn)
		}
	} else {
		data := db.NewTestData(c.p)
		fmt.Println("gen initdata begin")
		data.GenNewData(num)
		fmt.Println("gen initdata end")
		for i := 0; i < threadCount; i++ {
			go c.batchPut2(result, data.InitData[i], &wg, dbconn)
		}
	}
	wg1.Add(1)
	go db.Statistics(result, &wg1)
	wg.Wait()
	close(result)
	wg1.Wait()
}

//Test ...
func (c *LevelDb) Test(total int, num int) {
	os.RemoveAll(c.dataDir)
	fmt.Println("gen data begin")
	data := db.NewTestData(c.p)
	data.GenData(total, num)
	fmt.Println("gen data end")
	dbconn, err := leveldb.OpenFile(c.dataDir, nil)
	if err != nil {
		return
	}
	defer dbconn.Close()
	//init data
	fmt.Println("============init data begin==============")
	c.stage(1, data, dbconn)
	fmt.Println("============init data end================")
	//insert new data
	fmt.Println("============insert new data begin========")
	c.stage(2, data, dbconn)
	fmt.Println("============insert new data end==========")
	//query key exist
	fmt.Println("============query key exist begin========")
	c.stage(3, data, dbconn)
	fmt.Println("============query key exist end==========")
	//query key not exist
	fmt.Println("============query key not exist begin====")
	c.stage(4, data, dbconn)
	fmt.Println("============query key not exist end======")
	//update value of key which insert in step2
	fmt.Println("============update begin=================")
	c.stage(5, data, dbconn)
	fmt.Println("============update end===================")
	//del data which insert in step2
	fmt.Println("============del data begin===============")
	c.stage(6, data, dbconn)
	fmt.Println("============del data end=================")
}

func (c *LevelDb) stage(stage int, data *db.TestData, ldb *leveldb.DB) {
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	result := make(chan *stat.Histogram, threadCount)

	for i := 0; i < threadCount; i++ {
		switch stage {
		case 1:
			go c.batchPut2(result, data.InitData[i], &wg, ldb)
		case 2:
			go c.batchPut2(result, data.InsertData[i], &wg, ldb)
		case 3:
			go c.batchGet2(result, data.KeyExist[i], &wg, ldb)
		case 4:
			go c.batchGet2(result, data.KeyNotExit[i], &wg, ldb)
		case 5:
			go c.batchPut2(result, data.UpdateData[i], &wg, ldb)
		case 6:
			go c.batchDel2(result, data.KeyExist[i], &wg, ldb)
		}
	}
	wg1.Add(1)
	go db.Statistics(result, &wg1)
	wg.Wait()
	close(result)
	wg1.Wait()
}

func getOptions(p *properties.Properties) *opt.Options {
	o := &opt.Options{
		BlockCacheCapacity:            p.GetInt("BlockCacheCapacity", 8388608),
		BlockRestartInterval:          p.GetInt("BlockRestartInterval", 16),
		BlockSize:                     p.GetInt("BlockSize", 4096),
		CompactionExpandLimitFactor:   p.GetInt("CompactionExpandLimitFactor", 25),
		CompactionGPOverlapsFactor:    p.GetInt("CompactionGPOverlapsFactor", 10),
		CompactionL0Trigger:           p.GetInt("CompactionL0Trigger", 4),
		CompactionSourceLimitFactor:   p.GetInt("CompactionSourceLimitFactor", 1),
		CompactionTableSize:           p.GetInt("CompactionTableSize", 2097152),
		CompactionTableSizeMultiplier: p.GetFloat64("CompactionTableSizeMultiplier", 1.0),
		CompactionTotalSize:           p.GetInt("CompactionTotalSize", 10485760),
		CompactionTotalSizeMultiplier: p.GetFloat64("‬CompactionTotalSizeMultiplier", 10.0),
		DisableBlockCache:             p.GetBool("DisableBlockCache", false),
		DisableCompactionBackoff:      p.GetBool("DisableCompactionBackoff", false),
		DisableLargeBatchTransaction:  p.GetBool("DisableLargeBatchTransaction", false),
		ErrorIfExist:                  p.GetBool("ErrorIfExist", false),
		ErrorIfMissing:                p.GetBool("ErrorIfMissing", false),
		IteratorSamplingRate:          p.GetInt("IteratorSamplingRate", 1048576),
		NoSync:                        p.GetBool("NoSync", false),
		NoWriteMerge:                  p.GetBool("NoWriteMerge", false),
		OpenFilesCacheCapacity:        p.GetInt("OpenFilesCacheCapacity", 500),
		ReadOnly:                      p.GetBool("ReadOnly", false),
		WriteBuffer:                   p.GetInt("WriteBuffer", 4194304),
		WriteL0PauseTrigger:           p.GetInt("WriteL0PauseTrigger", 12),
		WriteL0SlowdownTrigger:        p.GetInt("WriteL0SlowdownTrigger", 8),
		Compression:                   opt.DefaultCompression,
	}

	if p.GetInt("Compression", 0) == 0 {
		o.Compression = opt.NoCompression
	}

	return o
}
