package influxdb

import (
	"log"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
)

type reporter struct {
	reg      metrics.Registry
	interval time.Duration

	url      string
	database string
	username string
	password string
	tags     map[string]string
	prefix   string

	client client.Client
}

// InfluxDB starts a InfluxDB reporter which will post the metrics from the given registry at each d interval.
func InfluxDB(r metrics.Registry, d time.Duration, url, database, username, password, prefix string) {
	InfluxDBWithTags(r, d, url, database, username, password, prefix, nil)
}

// InfluxDBWithTags starts a InfluxDB reporter which will post the metrics from the given registry at each d interval with the specified tags
func InfluxDBWithTags(r metrics.Registry, d time.Duration, url, database, username, password, prefix string, tags map[string]string) {

	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix = prefix + "_"
	}

	rep := &reporter{
		reg:      r,
		interval: d,
		url:      url,
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
	r.client, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     r.url,
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
			_, _, err := r.client.Ping(time.Second * 5)
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
		parts := strings.SplitN(pointName, ".", dotCount+1)
		measurementName = strings.Join(parts[:dotCount], "_")
		extra = strings.Replace(parts[dotCount], ".", "_", 1)
	}
	return
}

func fieldName(prefix, name string) string {
	if prefix != "" {
		return prefix + "_" + name
	} else {
		return name
	}
}

func (r *reporter) send() error {
	measurementFields := make(map[string]map[string]interface{})
	now := time.Now()
	r.reg.Each(func(name string, i interface{}) {

		measurement, fieldPrefix := measurementName(name)
		measurement = r.prefix + measurement

		if _, ok := measurementFields[measurement]; !ok {
			measurementFields[measurement] = make(map[string]interface{})
		}
		fields := measurementFields[measurement]

		switch metric := i.(type) {
		case metrics.Counter:
			ms := metric.Snapshot()
			fields[fieldName(fieldPrefix, "count")] = ms.Count()
		case metrics.Gauge:
			ms := metric.Snapshot()
			fields[fieldName(fieldPrefix, "gauge")] = ms.Value()
		case metrics.GaugeFloat64:
			ms := metric.Snapshot()
			fields[fieldName(fieldPrefix, "gauge")] = ms.Value()
		case metrics.Histogram:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fieldPrefix = fieldName(fieldPrefix, "histogram")
			fields[fieldName(fieldPrefix, "count")] = ms.Count()
			fields[fieldName(fieldPrefix, "max")] = ms.Max()
			fields[fieldName(fieldPrefix, "mean")] = ms.Mean()
			fields[fieldName(fieldPrefix, "min")] = ms.Min()
			fields[fieldName(fieldPrefix, "stddev")] = ms.StdDev()
			fields[fieldName(fieldPrefix, "variance")] = ms.Variance()
			fields[fieldName(fieldPrefix, "p50")] = ps[0]
			fields[fieldName(fieldPrefix, "p75")] = ps[1]
			fields[fieldName(fieldPrefix, "p95")] = ps[2]
			fields[fieldName(fieldPrefix, "p99")] = ps[3]
			fields[fieldName(fieldPrefix, "p999")] = ps[4]
			fields[fieldName(fieldPrefix, "p9999")] = ps[5]
		case metrics.Meter:
			ms := metric.Snapshot()
			fieldPrefix = fieldName(fieldPrefix, "meter")
			fields[fieldName(fieldPrefix, "count")] = ms.Count()
			fields[fieldName(fieldPrefix, "m1")] = ms.Rate1()
			fields[fieldName(fieldPrefix, "m5")] = ms.Rate5()
			fields[fieldName(fieldPrefix, "m15")] = ms.Rate15()
			fields[fieldName(fieldPrefix, "mean")] = ms.RateMean()
		case metrics.Timer:
			ms := metric.Snapshot()
			ps := ms.Percentiles([]float64{0.5, 0.75, 0.95, 0.99, 0.999, 0.9999})
			fieldPrefix = fieldName(fieldPrefix, "timer")
			fields[fieldName(fieldPrefix, "count")] = ms.Count()
			fields[fieldName(fieldPrefix, "max")] = ms.Max()
			fields[fieldName(fieldPrefix, "mean")] = ms.Mean()
			fields[fieldName(fieldPrefix, "min")] = ms.Min()
			fields[fieldName(fieldPrefix, "stddev")] = ms.StdDev()
			fields[fieldName(fieldPrefix, "variance")] = ms.Variance()
			fields[fieldName(fieldPrefix, "p50")] = ps[0]
			fields[fieldName(fieldPrefix, "p75")] = ps[1]
			fields[fieldName(fieldPrefix, "p95")] = ps[2]
			fields[fieldName(fieldPrefix, "p99")] = ps[3]
			fields[fieldName(fieldPrefix, "p999")] = ps[4]
			fields[fieldName(fieldPrefix, "p9999")] = ps[5]
			fields[fieldName(fieldPrefix, "m1")] = ms.Rate1()
			fields[fieldName(fieldPrefix, "m5")] = ms.Rate5()
			fields[fieldName(fieldPrefix, "m15")] = ms.Rate15()
			fields[fieldName(fieldPrefix, "meanrate")] = ms.RateMean()
		}
	})

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  r.database,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	for measurement, fields := range measurementFields {
		if pnt, err := client.NewPoint(measurement, r.tags, fields, now); err != nil {
			return err
		} else {
			bp.AddPoint(pnt)
		}
	}

	if err := r.client.Write(bp); err != nil {
		return err
	}
	return nil
}
