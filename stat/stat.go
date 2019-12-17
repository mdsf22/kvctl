package stat

import (
	"bytes"
	"fmt"
	"math"
	"time"
)

//Histogram ...
type Histogram struct {
	count      int64
	sum        int64
	min        int64
	max        int64
	totalBytes int64
	avg        int64
	elapsed    float64
	qps        int64
	kbs        int64
	startTime  time.Time
}

//NewHistogram ...
func NewHistogram() *Histogram {
	h := new(Histogram)
	h.min = math.MaxInt64
	h.max = math.MinInt64
	h.startTime = time.Now()
	return h
}

// Measure ... Measure
func (h *Histogram) Measure(latency time.Duration, len int64) {
	n := int64(latency / time.Microsecond)
	h.sum += n
	h.count++
	h.totalBytes += len
	if n < h.min {
		h.min = n
	}

	if n > h.max {
		h.max = n
	}
}

//Calc ...
func (h *Histogram) Calc() {
	h.elapsed = time.Now().Sub(h.startTime).Seconds()
	h.avg = int64(h.sum / h.count)
	if h.elapsed != 0.0 {
		h.kbs = int64(float64(h.totalBytes) / h.elapsed / 1024)
		h.qps = int64(float64(h.count) / h.elapsed)
	} else {
		h.kbs = 0.0
		h.qps = 0.0
	}

	// fmt.Println("elapsed:", h.elapsed)
	// fmt.Println("qps:", h.qps)
	// fmt.Println("kbs:", h.kbs)
}

//Summary ...
func (h *Histogram) Summary() string {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("Elapsed(s): %.1f", h.elapsed))
	buf.WriteString(fmt.Sprintf("Avg(us): %d, ", h.avg))
	buf.WriteString(fmt.Sprintf("Qps: %d, ", h.qps))
	buf.WriteString(fmt.Sprintf("Count: %d, ", h.count))
	buf.WriteString(fmt.Sprintf("Min(us): %d, ", h.min))
	buf.WriteString(fmt.Sprintf("Max(us): %d, ", h.max))
	buf.WriteString(fmt.Sprintf("kB/s: %d, ", h.kbs))
	return buf.String()
}

//Result ...
func Result(result []*Histogram) {
	sum := NewHistogram()
	num := 0
	for _, r := range result {
		num++
		sum.kbs += r.kbs
		sum.count += r.count
		sum.qps += r.qps
		sum.avg += r.avg
		if r.min < sum.min {
			sum.min = r.min
		}

		if r.max > sum.max {
			sum.max = r.max
		}

		if r.elapsed > sum.elapsed {
			sum.elapsed = r.elapsed
		}

		fmt.Println(r.Summary())
	}
	sum.avg = int64(sum.avg / int64(num))
	fmt.Println("sum: ", sum.Summary())
}
