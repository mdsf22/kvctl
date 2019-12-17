package rocksdb

import (
	"fmt"
	"os"
	"sync"
	"github.com/mdsf22/kvctl/db"
	"github.com/mdsf22/kvctl/stat"
	"time"

	"github.com/magiconair/properties"
	"github.com/tecbot/gorocksdb"
)

//RocksDb ...
type RocksDb struct {
	p       *properties.Properties
	dataDir string
	threads int
	keypre  string
	vallen  int

	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions
}

//NewRocksDb ...
func NewRocksDb(p *properties.Properties, dataDir string, threads int, keypre string, vallen int) db.KVDB {
	db := &RocksDb{
		p:         p,
		dataDir:   dataDir,
		threads:   threads,
		keypre:    keypre,
		vallen:    vallen,
		readOpts:  gorocksdb.NewDefaultReadOptions(),
		writeOpts: gorocksdb.NewDefaultWriteOptions()}
	return db
}

//Get ...
func (c *RocksDb) Get(key []byte) (val []byte, err error) {
	opts := getOptions(c.p)
	db, err := gorocksdb.OpenDb(opts, c.dataDir)
	if err != nil {
		return
	}
	defer db.Close()

	data, err := db.Get(c.readOpts, key)
	if err != nil {
		return
	}
	defer data.Free()

	val = cloneValue(data)
	return
}

//Put ...
func (c *RocksDb) Put(key []byte, val []byte) (err error) {
	opts := getOptions(c.p)
	db, err := gorocksdb.OpenDb(opts, c.dataDir)
	if err != nil {
		fmt.Println("open failed :", err.Error())
		return
	}
	defer db.Close()

	err = db.Put(c.writeOpts, key, val)
	if err != nil {
		fmt.Println("put failed :", err.Error())
		return
	}
	return
}

func (c *RocksDb) put2(key []byte, value []byte, db *gorocksdb.DB) (err error) {
	err = db.Put(c.writeOpts, key, value)
	return
}

//Del ...
func (c *RocksDb) Del(key []byte) (err error) {
	opts := getOptions(c.p)
	db, err := gorocksdb.OpenDb(opts, c.dataDir)
	if err != nil {
		fmt.Println("open failed :", err.Error())
		return
	}
	defer db.Close()
	err = db.Delete(c.writeOpts, key)
	return
}

func (c *RocksDb) batchGet(result chan *stat.Histogram, index int, num int, wg *sync.WaitGroup, dbconn *gorocksdb.DB) (err error) {
	defer wg.Done()
	stat := stat.NewHistogram()
	for i := 0; i < num; i++ {
		key := db.K(index, c.keypre)
		index++
		start := time.Now()
		val, err := dbconn.Get(c.readOpts, []byte(key))
		lan := time.Now().Sub(start)
		if err != nil {
			fmt.Println("batchGet err:", err)
		}
		defer val.Free()
		data := cloneValue(val)

		if len(data) == 0 {
			fmt.Printf("batchGet key: %s not exist\n", key)
		}
		stat.Measure(lan, int64(len(data)))
	}
	stat.Calc()
	result <- stat
	return
}

func (c *RocksDb) batchPut(result chan *stat.Histogram, id int, index int, num int, wg *sync.WaitGroup, dbconn *gorocksdb.DB) (err error) {
	defer wg.Done()
	stat := stat.NewHistogram()
	for i := 0; i < num; i++ {
		//fmt.Println("=============: ", index)
		if i%100000 == 0 {
			fmt.Println("progress: ", float32(i)/float32(num))
		}

		key := db.K(index, c.keypre)
		var val []byte
		if c.p.GetInt("valtype", 0) == 0 {
			val = db.V(c.vallen)
		} else {
			val = []byte(db.VRan(c.vallen))
		}
		index++
		start := time.Now()
		// err = db.Put(key, val, nil)
		err = dbconn.Put(c.writeOpts, key, val)
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

func (c *RocksDb) batchPut2(result chan *stat.Histogram, data map[string]string, wg *sync.WaitGroup, db *gorocksdb.DB) (err error) {
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

//GetN ...
func (c *RocksDb) GetN(num int) {
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	numPthread := num / threadCount
	result := make(chan *stat.Histogram, threadCount)
	opts := getOptions(c.p)
	dbconn, err := gorocksdb.OpenDb(opts, c.dataDir)
	if err != nil {
		fmt.Println("open failed :", err.Error())
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
func (c *RocksDb) PutN(num int) {
	os.RemoveAll(c.dataDir)
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	result := make(chan *stat.Histogram, threadCount)
	opts := getOptions(c.p)
	dbconn, err := gorocksdb.OpenDb(opts, c.dataDir)
	if err != nil {
		fmt.Println("open failed :", err.Error())
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
func (c *RocksDb) Test(total int, num int) {

}

func getOptions(p *properties.Properties) *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetAllowConcurrentMemtableWrites(p.GetBool(rocksdbAllowConcurrentMemtableWrites, true))
	opts.SetAllowMmapReads(p.GetBool(rocsdbAllowMmapReads, false))
	opts.SetAllowMmapWrites(p.GetBool(rocksdbAllowMmapWrites, false))
	opts.SetArenaBlockSize(p.GetInt(rocksdbArenaBlockSize, 0))
	opts.SetDbWriteBufferSize(p.GetInt(rocksdbDBWriteBufferSize, 0))
	opts.SetHardPendingCompactionBytesLimit(p.GetUint64(rocksdbHardPendingCompactionBytesLimit, 256<<30))
	opts.SetLevel0FileNumCompactionTrigger(p.GetInt(rocksdbLevel0FileNumCompactionTrigger, 4))
	opts.SetLevel0SlowdownWritesTrigger(p.GetInt(rocksdbLevel0SlowdownWritesTrigger, 20))
	opts.SetLevel0StopWritesTrigger(p.GetInt(rocksdbLevel0StopWritesTrigger, 36))
	opts.SetMaxBytesForLevelBase(p.GetUint64(rocksdbMaxBytesForLevelBase, 256<<20))
	opts.SetMaxBytesForLevelMultiplier(p.GetFloat64(rocksdbMaxBytesForLevelMultiplier, 10))
	opts.SetMaxTotalWalSize(p.GetUint64(rocksdbMaxTotalWalSize, 0))
	opts.SetMemtableHugePageSize(p.GetInt(rocksdbMemtableHugePageSize, 0))
	opts.SetNumLevels(p.GetInt(rocksdbNumLevels, 7))
	opts.SetUseDirectReads(p.GetBool(rocksdbUseDirectReads, false))
	opts.SetUseFsync(p.GetBool(rocksdbUseFsync, false))
	opts.SetWriteBufferSize(p.GetInt(rocksdbWriteBufferSize, 64<<20))
	opts.SetMaxWriteBufferNumber(p.GetInt(rocksdbMaxWriteBufferNumber, 2))
	opts.SetMaxBackgroundFlushes(p.GetInt(rocksdbMaxBackgroundFlushes, 2))
	opts.IncreaseParallelism(p.GetInt(rocksdbThreadsCompaction, 2))
	if p.GetInt(rocksdbCompression, 0) == 0 {
		opts.SetCompression(0)
	} else {
		opts.SetCompression(1)
	}
	opts.SetBlockBasedTableFactory(getTableOptions(p))
	return opts
}

func getTableOptions(p *properties.Properties) *gorocksdb.BlockBasedTableOptions {
	tblOpts := gorocksdb.NewDefaultBlockBasedTableOptions()

	tblOpts.SetBlockSize(p.GetInt(rocksdbBlockSize, 4<<10))
	tblOpts.SetBlockSizeDeviation(p.GetInt(rocksdbBlockSizeDeviation, 10))
	tblOpts.SetCacheIndexAndFilterBlocks(p.GetBool(rocksdbCacheIndexAndFilterBlocks, false))
	tblOpts.SetNoBlockCache(p.GetBool(rocksdbNoBlockCache, false))
	tblOpts.SetPinL0FilterAndIndexBlocksInCache(p.GetBool(rocksdbPinL0FilterAndIndexBlocksInCache, false))
	tblOpts.SetWholeKeyFiltering(p.GetBool(rocksdbWholeKeyFiltering, true))
	tblOpts.SetBlockRestartInterval(p.GetInt(rocksdbBlockRestartInterval, 16))

	if b := p.GetString(rocksdbFilterPolicy, ""); len(b) > 0 {
		if b == "rocksdb.BuiltinBloomFilter" {
			const defaultBitsPerKey = 10
			tblOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(defaultBitsPerKey))
		}
	}

	indexType := p.GetString(rocksdbIndexType, "kBinarySearch")
	if indexType == "kBinarySearch" {
		tblOpts.SetIndexType(gorocksdb.KBinarySearchIndexType)
	} else if indexType == "kHashSearch" {
		tblOpts.SetIndexType(gorocksdb.KHashSearchIndexType)
	} else if indexType == "kTwoLevelIndexSearch" {
		tblOpts.SetIndexType(gorocksdb.KTwoLevelIndexSearchIndexType)
	}

	return tblOpts
}

func cloneValue(v *gorocksdb.Slice) []byte {
	return append([]byte(nil), v.Data()...)
}
