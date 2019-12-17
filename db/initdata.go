package db

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"
	
	"github.com/mdsf22/kvctl/stat"

	"github.com/magiconair/properties"
)

//TestData ...
type TestData struct {
	p *properties.Properties
	// threads    int
	// keypre     string
	// vallen     int
	InitData   []map[string]string
	InsertData []map[string]string
	KeyExist   [][]string
	KeyNotExit [][]string
	UpdateData []map[string]string
}

// NewTestData ...
func NewTestData(p *properties.Properties) *TestData {
	data := &TestData{
		p: p,
	}
	return data
}

//GenNewData ...
func (t *TestData) GenNewData(count int) {
	threads := t.p.GetInt("threads", 10)
	keypre := t.p.GetString("keypre", "test")
	vallen := t.p.GetInt("vallen", 1024)

	itemsPthread := count / threads
	// gen init data
	t.InitData = make([]map[string]string, threads)
	index := 0
	for i := 0; i < threads; i++ {
		m := make(map[string]string)
		for j := 0; j < itemsPthread; j++ {
			key := fmt.Sprintf("%s_%d", keypre, index)
			index++
			if t.p.GetInt("valtype", 0) == 0 {
				m[key] = string(V(vallen))
			} else {
				m[key] = VRan(vallen)
			}
		}
		t.InitData[i] = m
	}
}

// GenData ...
func (t *TestData) GenData(total int, insert int) {
	threads := t.p.GetInt("threads", 10)
	keypre := t.p.GetString("keypre", "test")
	vallen := t.p.GetInt("vallen", 1024)

	totalItems := total * 1024 * 1024 / vallen
	itemsPthread := totalItems / threads
	newItems := insert * 1024 * 1024 / vallen
	newitemsPthread := newItems / threads
	fmt.Printf("total %d, itemsPt %d\n", totalItems, itemsPthread)
	// gen init data
	t.InitData = make([]map[string]string, threads)
	index := 0
	for i := 0; i < threads; i++ {
		m := make(map[string]string)
		for j := 0; j < itemsPthread; j++ {
			key := fmt.Sprintf("%s_%d", keypre, index)
			index++
			m[key] = VRan(vallen)
		}
		t.InitData[i] = m
	}
	// gen insert data and update data
	t.InsertData = make([]map[string]string, threads)
	t.UpdateData = make([]map[string]string, threads)
	t.KeyExist = make([][]string, threads)
	for i := 0; i < threads; i++ {
		m := make(map[string]string)
		m1 := make(map[string]string)
		keyExist := make([]string, newitemsPthread)
		for j := 0; j < newitemsPthread; j++ {
			key := fmt.Sprintf("%s_%d", keypre, index)
			index++
			m[key] = VRan(vallen)
			m1[key] = string(V(vallen))
			keyExist[j] = key
		}
		t.InsertData[i] = m
		t.UpdateData[i] = m1
		t.KeyExist[i] = keyExist
	}
	// gen key not exist
	t.KeyNotExit = make([][]string, threads)
	for i := 0; i < threads; i++ {
		keyNotExist := make([]string, newitemsPthread)
		for j := 0; j < newitemsPthread; j++ {
			key := fmt.Sprintf("%s_%d", keypre, index)
			index++
			keyNotExist[j] = key
		}
		t.KeyNotExit[i] = keyNotExist
	}
}

//K ...
func K(index int, keypre string) []byte {
	var t string
	t = fmt.Sprintf("%s_%d", keypre, index)
	return []byte(t)
}

//V ...
func V(n int) []byte {
	return bytes.Repeat([]byte{'0'}, n)
}

//VRan ...
func VRan(n int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	bytes := make([]byte, n)
	_, err := rand.Read(bytes)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	lettersLen := len(letters)
	for i, b := range bytes {
		bytes[i] = letters[b%byte(lettersLen)]
	}
	return string(bytes)
}

//Statistics ...
func Statistics(result chan *stat.Histogram, wg *sync.WaitGroup) {
	defer wg.Done()
	var rs []*stat.Histogram
	for r := range result {
		rs = append(rs, r)
	}
	stat.Result(rs)
}
