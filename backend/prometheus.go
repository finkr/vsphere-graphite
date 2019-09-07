package backend

// InitPrometheus : Set some channels to notify other theads when using Prometheus
import (
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Describe : Implementation of Prometheus Collector.Describe
func (backend *Config) Describe(ch chan<- *prometheus.Desc) {
	prometheus.NewGauge(prometheus.GaugeOpts{Name: "Dummy", Help: "Dummy"}).Describe(ch)
}

// Collect : Implementation of Prometheus Collector.Collect
func (backend *Config) Collect(ch chan<- prometheus.Metric) {

	log.Println("prometheus: requesting metrics")

	request := make(chan Point, 100)
	done := make(chan bool)
	channels := Channels{Request: &request, Done: &done}

	select {
	case *queries <- channels:
		log.Println("prometheus: requested metrics")
	default:
		log.Println("prometheus: query buffer full. discarding request")
		return
	}

	// points recieved
	points := 0
	// handle timeout between point reception
	rectimer := time.NewTimer(100 * time.Millisecond)
	// check that the collection threads have finished
	recdone := false
	for {
		select {
		case point := <-*channels.Request:
			// reset timer
			if !rectimer.Stop() {
				select {
				case <-rectimer.C:
				default:
				}
			}
			rectimer.Reset(100 * time.Millisecond)
			// increase points
			points++
			// send point to prometheus
			backend.PrometheusSend(ch, point)
		case <-*channels.Done:
			recdone = true
		case <-rectimer.C:
			// only exit when done and timeout
			if recdone {
				log.Printf("prometheus: sent %d points", points)
				return
			}
		}
	}
}

//PrometheusSend sends a point to prometheus
func (backend *Config) PrometheusSend(ch chan<- prometheus.Metric, point Point) {
	tags := point.GetTags(backend.NoArray, ",")
	labelNames := make([]string, len(tags))
	labelValues := make([]string, len(tags))
	i := 0
	for key, value := range tags {
		labelNames[i] = key
		labelValues[i] = value
		i++
	}
	key := fmt.Sprintf("%s_%s_%s_%s", backend.Prefix, point.Group, point.Counter, point.Rollup)
	desc := prometheus.NewDesc(key, "vSphere collected metric", labelNames, nil)
	metric, err := prometheus.NewConstMetric(desc, prometheus.GaugeValue, float64(point.Value), labelValues...)
	if err != nil {
		log.Println("Error creating prometheus metric")
	}
	ch <- metric
}
