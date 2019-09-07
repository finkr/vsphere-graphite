package backend

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/cblomart/vsphere-graphite/utils"
	"github.com/valyala/fasthttp"
)

// ThinPrometheusClient tries to export metrics to prometheus as simply as possible
type ThinPrometheusClient struct {
	Hostname string
	Port     int
	address  string
}

const defaultPort = 9155

// NewThinPrometheusClient creates a new thin prometheus
func NewThinPrometheusClient(server string, port int) (ThinPrometheusClient, error) {
	//create the port
	if port == 0 {
		port = defaultPort
	} else if port < 1000 || port > 65535 {
		return ThinPrometheusClient{}, errors.New("port is not in a user range")
	}
	address := ""
	if len(server) > 0 {
		if server != "*" {
			address = server
		}
	}
	address = fmt.Sprintf("%s:%d", address, port)
	return ThinPrometheusClient{Hostname: server, Port: port, address: address}, nil
}

// ListenAndServe will start the listen thead for metric requests
func (client *ThinPrometheusClient) ListenAndServe() error {
	log.Printf("thinprom: start listening for metric request at %s\n", client.address)
	return fasthttp.ListenAndServe(client.address, fasthttp.CompressHandlerLevel(requestHandler, 9))
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	if string(ctx.Path()) != "/metrics" {
		ctx.Error("Unsupported path", fasthttp.StatusNotFound)
		return
	}
	// prepare the channels for the request
	request := make(chan Point, 100)
	done := make(chan bool)
	channels := Channels{Request: &request, Done: &done}
	// create a buffer to organise metrics per type
	buffer := map[string][]string{}
	log.Println("thinprom: sending query request")
	// start the queriess
	select {
	case *queries <- channels:
		log.Println("thinprom: sent query Request")
	default:
		ctx.Error("Query buffer full", fasthttp.StatusConflict)
		return
	}
	// start a timeout
	timeout := time.NewTimer(100 * time.Millisecond)
	// collected points
	points := 0
	// recieve done
	recdone := false
	log.Println("Tthinprom: waiting for query results")
L:
	for {
		select {
		case point := <-*channels.Request:
			// reset timer
			if !timeout.Stop() {
				select {
				case <-timeout.C:
				default:
				}
			}
			timeout.Reset(100 * time.Millisecond)
			// increased recieved points
			points++
			// add point to the buffer
			addToThinPrometheusBuffer(buffer, &point)
		case <-*channels.Done:
			// finish consuming metrics and break loop
			log.Println("thinprom: signaled the end of the collection")
			recdone = true
		case <-timeout.C:
			// stop timer
			if recdone {
				log.Printf("thinprom: sent %d points", points)
				break L
			}
		}
	}
	ctx.SetContentType("text/plain; charset=utf8")
	var outbuff bytes.Buffer
	for key, vals := range buffer {
		utils.MustWriteString(&outbuff, "#HELP ")
		utils.MustWriteString(&outbuff, key)
		utils.MustWriteString(&outbuff, " ")
		utils.MustWriteString(&outbuff, strings.Replace(key, "_", " ", -1))
		utils.MustWriteString(&outbuff, "\n")
		utils.MustWriteString(&outbuff, "#TYPE ")
		utils.MustWriteString(&outbuff, key)
		utils.MustWriteString(&outbuff, " gauge\n")
		for _, val := range vals {
			utils.MustWriteString(&outbuff, key)
			utils.MustWriteString(&outbuff, val)
			utils.MustWriteString(&outbuff, "\n")
		}
		_, err := ctx.Write(outbuff.Bytes())
		if err != nil {
			log.Printf("thinpro: error writing to buffer %s\n", err)
		}
		outbuff.Reset()
	}
	log.Println("thinprom: sended response to request")
}

func addToThinPrometheusBuffer(metrics map[string][]string, point *Point) {
	var buffer bytes.Buffer
	utils.MustWriteString(&buffer, prefix)
	utils.MustWriteString(&buffer, "_")
	utils.MustWriteString(&buffer, point.Group)
	utils.MustWriteString(&buffer, "_")
	utils.MustWriteString(&buffer, point.Counter)
	utils.MustWriteString(&buffer, "_")
	utils.MustWriteString(&buffer, point.Rollup)
	metric := buffer.String()
	buffer.Reset()
	tags := point.GetTags(false, ",")
	var keys = make([]string, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var tmp []string
	for _, key := range keys {
		if len(tags[key]) == 0 {
			continue
		}
		utils.MustWriteString(&buffer, key)
		utils.MustWriteString(&buffer, "=\"")
		utils.MustWriteString(&buffer, tags[key])
		utils.MustWriteString(&buffer, "\"")
		tmp = append(tmp, buffer.String())
		buffer.Reset()
	}
	strtags := strings.Join(tmp, ",")
	utils.MustWriteString(&buffer, "{")
	utils.MustWriteString(&buffer, strtags)
	utils.MustWriteString(&buffer, "} ")
	utils.MustWriteString(&buffer, utils.ValToString(point.Value, ",", false))

	if metrics[metric] == nil {
		metrics[metric] = []string{buffer.String()}
	} else {
		metrics[metric] = append(metrics[metric], buffer.String())
	}
	buffer.Reset()
}
