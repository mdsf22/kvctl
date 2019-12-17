package dbtikv

import (
	"fmt"
	"strings"
	"sync"
	"github.com/mdsf22/kvctl/db"
	"github.com/mdsf22/kvctl/stat"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
)

const (
	tikvPD = "tikv.pd"
	// raw, txn
	tikvType = "tikv.type"
)

//TikvDb ... type
type TikvDb struct {
	p       *properties.Properties
	pdAddr  []string
	threads int
	keypre  string
	vallen  int
}

// NewTikvDb ... new tikv connection
func NewTikvDb(p *properties.Properties, pdAddr []string, threads int, keypre string, vallen int) db.KVDB {
	conn := &TikvDb{
		p:       p,
		pdAddr:  pdAddr,
		threads: threads,
		keypre:  keypre,
		vallen:  vallen}
	return conn
}

// Get ... get value
func (c *TikvDb) Get(key []byte) (val []byte, err error) {
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()

	val, err = conn.Get(key)
	if err != nil {
		fmt.Println("get failed:", err)
		return
	}
	return
}

func (c *TikvDb) get2(key []byte, conn *tikv.RawKVClient) (val []byte, err error) {
	val, err = conn.Get(key)
	if err != nil {
		fmt.Println("get failed:", err)
		return
	}
	return
}

// Put ... Put
func (c *TikvDb) Put(key []byte, value []byte) (err error) {
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()
	err = conn.Put(key, value)
	return
}

func (c *TikvDb) put2(key []byte, value []byte, conn *tikv.RawKVClient) (err error) {
	err = conn.Put(key, value)
	return
}

// Del ...
func (c *TikvDb) Del(key []byte) (err error) {
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()
	err = conn.Delete(key)
	return
}

func (c *TikvDb) del2(key []byte, conn *tikv.RawKVClient) (err error) {
	err = conn.Delete(key)
	return
}

func (c *TikvDb) batchGet(result chan *stat.Histogram, index int, num int, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()

	stat := stat.NewHistogram()
	for i := 0; i < num; i++ {
		key := db.K(index, c.keypre)
		index++
		start := time.Now()
		val, err := conn.Get([]byte(key))
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

func (c *TikvDb) batchGet2(result chan *stat.Histogram, keys []string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()

	stat := stat.NewHistogram()
	for _, key := range keys {
		start := time.Now()
		val, err := c.get2([]byte(key), conn)
		lan := time.Now().Sub(start)
		if err != nil {
			fmt.Println("get err:", err)
		}
		stat.Measure(lan, int64(len(val)))
	}
	stat.Calc()
	result <- stat
	return
}

func (c *TikvDb) batchPut(result chan *stat.Histogram, id int, index int, num int, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()

	stat := stat.NewHistogram()
	for i := 0; i < num; i++ {
		key := db.K(index, c.keypre)
		val := db.V(c.vallen)
		index++
		start := time.Now()
		err = conn.Put(key, val)
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

func (c *TikvDb) batchPut2(result chan *stat.Histogram, data map[string]string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()

	stat := stat.NewHistogram()
	for k, v := range data {
		start := time.Now()
		err = c.put2([]byte(k), []byte(v), conn)
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

func (c *TikvDb) batchDel2(result chan *stat.Histogram, keys []string, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	conn, err := tikv.NewRawKVClient(c.pdAddr, config.Security{})
	if err != nil {
		return
	}
	defer conn.Close()

	stat := stat.NewHistogram()
	for _, key := range keys {
		start := time.Now()
		err := c.del2([]byte(key), conn)
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

// GetN ... get many data through many threads
func (c *TikvDb) GetN(num int) {
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	numPthread := num / threadCount
	result := make(chan *stat.Histogram, threadCount)

	for i := 0; i < threadCount; i++ {
		go c.batchGet(result, i*numPthread, numPthread, &wg)
	}
	wg1.Add(1)
	go db.Statistics(result, &wg1)
	wg.Wait()
	close(result)
	wg1.Wait()
}

//PutN ... put many data through many threads
func (c *TikvDb) PutN(num int) {
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	numPthread := num / threadCount
	result := make(chan *stat.Histogram, threadCount)

	for i := 0; i < threadCount; i++ {
		go c.batchPut(result, i, i*numPthread, numPthread, &wg)
	}
	wg1.Add(1)
	go db.Statistics(result, &wg1)
	wg.Wait()
	close(result)
	wg1.Wait()
}

//Test ...
func (c *TikvDb) Test(total int, num int) {
	data := db.NewTestData(c.p)
	data.GenData(total, num)

	//init data
	fmt.Println("============init data begin==============")
	c.stage(1, data)
	fmt.Println("============init data end================")
	//insert new data
	fmt.Println("============insert new data begin========")
	c.stage(2, data)
	fmt.Println("============insert new data end==========")
	//query key exist
	fmt.Println("============query key exist begin========")
	c.stage(3, data)
	fmt.Println("============query key exist end==========")
	//query key not exist
	fmt.Println("============query key not exist begin====")
	c.stage(4, data)
	fmt.Println("============query key not exist end======")
	//update value of key which insert in step2
	fmt.Println("============update begin=================")
	c.stage(5, data)
	fmt.Println("============update end===================")
	//del data which insert in step2
	fmt.Println("============del data begin===============")
	c.stage(6, data)
	fmt.Println("============del data end=================")
}

func (c *TikvDb) stage(stage int, data *db.TestData) {
	threadCount := c.threads
	var wg, wg1 sync.WaitGroup

	wg.Add(threadCount)
	result := make(chan *stat.Histogram, threadCount)

	for i := 0; i < threadCount; i++ {
		switch stage {
		case 1:
			go c.batchPut2(result, data.InitData[i], &wg)
		case 2:
			go c.batchPut2(result, data.InsertData[i], &wg)
		case 3:
			go c.batchGet2(result, data.KeyExist[i], &wg)
		case 4:
			go c.batchGet2(result, data.KeyNotExit[i], &wg)
		case 5:
			go c.batchPut2(result, data.UpdateData[i], &wg)
		case 6:
			go c.batchDel2(result, data.KeyExist[i], &wg)
		}
	}
	wg1.Add(1)
	go db.Statistics(result, &wg1)
	wg.Wait()
	close(result)
	wg1.Wait()
}

type tikvCreator struct {
}

func (c tikvCreator) Create(p *properties.Properties) db.KVDB {
	tp := p.GetString(tikvType, "raw")
	pd := p.GetString(tikvPD, "192.168.153.20:2379")
	pds := strings.Split(pd, ",")
	keypre := p.GetString("keypre", "test")
	threads := p.GetInt("threads", 10)
	vallen := p.GetInt("vallen", 1024)

	fmt.Println("===============================")
	fmt.Println("pdaddrs: ", pd)
	fmt.Println("threads: ", threads)
	fmt.Println("keypre: ", keypre)
	fmt.Println("===============================")

	switch tp {
	case "raw":
		return NewTikvDb(p, pds, threads, keypre, vallen)
	default:
		return nil
	}
}

func init() {
	db.RegisterDBCreator("tikv", tikvCreator{})
}
