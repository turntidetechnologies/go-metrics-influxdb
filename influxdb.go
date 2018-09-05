package influxdb

import (
	"fmt"
	"log"
	uurl "net/url"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client"
	"github.com/rcrowley/go-metrics"
)

type reporter struct {
	reg      metrics.Registry
	interval time.Duration

	url      uurl.URL
	database string
	username string
	password string
	tags     map[string]string
	prefix   string

	client *client.Client
}

// InfluxDB starts a InfluxDB reporter which will post the metrics from the given registry at each d interval.
func InfluxDB(r metrics.Registry, d time.Duration, url, database, username, password, prefix string) {
	InfluxDBWithTags(r, d, url, database, username, password, prefix, nil)
}

// InfluxDBWithTags starts a InfluxDB reporter which will post the metrics from the given registry at each d interval with the specified tags
func InfluxDBWithTags(r metrics.Registry, d time.Duration, url, database, username, password, prefix string, tags map[string]string) {
	u, err := uurl.Parse(url)
	if err != nil {
		log.Printf("unable to parse InfluxDB url %s. err=%v", url, err)
		return
	}

	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix = prefix + "_"
	}

	rep := &reporter{
		reg:      r,
		interval: d,
		url:      *u,
		database: database,
		username: username,
		password: password,
		prefix:   prefix,
		tags:     tags,
	}
	if err := rep.makeClient(); err != nil {
		log.Printf("unable to make InfluxDB client. err=%v", err)
		return
	}

	rep.run()
}

func (r *reporter) makeClient() (err error) {
	r.client, err = client.NewClient(client.Config{
		URL:      r.url,
		Username: r.username,
		Password: r.password,
	})

	return
}

func (r *reporter) run() {
	intervalTicker := time.Tick(r.interval)
	pingTicker := time.Tick(time.Second * 5)

	for {
		select {
		case <-intervalTicker:
			if err := r.send(); err != nil {
				log.Printf("unable to send metrics to InfluxDB. err=%v", err)
			}
		case <-pingTicker:
			_, _, err := r.client.Ping()
			if err != nil {
				log.Printf("got error while sending a ping to InfluxDB, trying to recreate client. err=%v", err)

				if err = r.makeClient(); err != nil {
					log.Printf("unable to make InfluxDB client. err=%v", err)
				}
			}
		}
	}
}

func measurementName(pointName string) (measurementName string, extra string) {

	if dotCount := strings.Count(pointName, "."); dotCount == 0 {
		measurementName = pointName
		extra = ""
	} else if dotCount == 1 {
		parts := strings.SplitN(pointName, ".", 2)
		measurementName = parts[0]
		extra = parts[1]
	} else {
		parts := strings.SplitN(pointName, ".", dotCount)
		lenOffset := dotCount - 1
		measurementName = strings.Join(parts[:lenOffset], "_")
		extra = strings.Replace(parts[lenOffset], ".", "_", 1)
	}
}

func fieldName(prefix, name string) string {
	if prefix != "" {
		return prefix + "_" + name
	} else {
		return name
	}
}

func (r *reporter) send() error {

	measurementPoints := map[string]*client.Point
	// var pts []client.Point
	now := time.Now()
	r.reg.Each(func(name string, i interface{}) {

		measurement, fieldPrefix := measurementName(name)
		measurement = r.prefix + measurement

		point, ok := measurementPoints[measurement]
		if !ok {
			point = &client.Point{
				Measurement: measurement,
				Tags:        r.tags,
				Time:        now,
				Fields:      make(map[string]interface{}),
			}
			measurementPoints[measurement] = point
		}

		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			point.Fields[fieldPrefix + "_count"] = ms.Count()
		case metrics.Gauge:
			ms := metric.Snapshot()
			point.Fields[fieldName(fieldPrefix, "gauge")] = ms.Value()
		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			point.Fields[fieldName(fieldPrefix, "gauge")] = ms.Value()
		case metrics.Histogram:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fieldPrefix = fieldName(fieldPrefix, "histogram")
			point.Fields[fieldName(fieldPrefix, "count")] =    ms.Count()
			point.Fields[fieldName(fieldPrefix, "max")] =      ms.Max()
			point.Fields[fieldName(fieldPrefix, "mean")] =     ms.Mean()
			point.Fields[fieldName(fieldPrefix, "min")] =      ms.Min()
			point.Fields[fieldName(fieldPrefix, "stddev")] =   ms.StdDev()
			point.Fields[fieldName(fieldPrefix, "variance")] = ms.Variance()
			point.Fields[fieldName(fieldPrefix, "p50")] =      ps[0]
			point.Fields[fieldName(fieldPrefix, "p75")] =      ps[1]
			point.Fields[fieldName(fieldPrefix, "p95")] =      ps[2]
			point.Fields[fieldName(fieldPrefix, "p99")] =      ps[3]
			point.Fields[fieldName(fieldPrefix, "p999")] =     ps[4]
			point.Fields[fieldName(fieldPrefix, "p9999")] =    ps[5]
		case metrics.Meter:
			ms := metric.Snapshot()
			fieldPrefix = fieldName(fieldPrefix, "meter")
			point.Fields[fieldName(fieldPrefix, "count")] = ms.Count()
			point.Fields[fieldName(fieldPrefix, "m1")] =    ms.Rate1()
			point.Fields[fieldName(fieldPrefix, "m5")] =    ms.Rate5()
			point.Fields[fieldName(fieldPrefix, "m15")] =   ms.Rate15()
			point.Fields[fieldName(fieldPrefix, "mean")] =  ms.RateMean()
		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fieldPrefix = fieldName(fieldPrefix, "timer")
			point.Fields[fieldName(fieldPrefix, "count")] =    ms.Count()
			point.Fields[fieldName(fieldPrefix, "max")] =      ms.Max()
			point.Fields[fieldName(fieldPrefix, "mean")] =     ms.Mean()
			point.Fields[fieldName(fieldPrefix, "min")] =      ms.Min()
			point.Fields[fieldName(fieldPrefix, "stddev")] =   ms.StdDev()
			point.Fields[fieldName(fieldPrefix, "variance")] = ms.Variance()
			point.Fields[fieldName(fieldPrefix, "p50")] =      ps[0]
			point.Fields[fieldName(fieldPrefix, "p75")] =      ps[1]
			point.Fields[fieldName(fieldPrefix, "p95")] =      ps[2]
			point.Fields[fieldName(fieldPrefix, "p99")] =      ps[3]
			point.Fields[fieldName(fieldPrefix, "p999")] =     ps[4]
			point.Fields[fieldName(fieldPrefix, "p9999")] =    ps[5]
			point.Fields[fieldName(fieldPrefix, "m1")] =       ms.Rate1()
			point.Fields[fieldName(fieldPrefix, "m5")] =       ms.Rate5()
			point.Fields[fieldName(fieldPrefix, "m15")] =      ms.Rate15()
			point.Fields[fieldName(fieldPrefix, "meanrate")] = ms.RateMean()
		}
	})

	pts := make([]client.Point, len(measurementPoints))
	i := 0
	for _, point := range measurementPoints {
		pts[i] = &point
		i++
	}

	bps := client.BatchPoints{
		Points:   pts,
		Database: r.database,
	}

	_, err := r.client.Write(bps)
	return err
}
