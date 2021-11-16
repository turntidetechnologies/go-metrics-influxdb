package influxdb

import (
	"testing"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/rcrowley/go-metrics"
)

type mockClient struct {
	client.Client
	consumePoints func(points []*client.Point)
}

var _ client.Client = (*mockClient)(nil)

// Write passes any writtent points to the configured mc.consumePoints function
func (mc *mockClient) Write(bp client.BatchPoints) error {
	if bp != nil {
		mc.consumePoints(bp.Points())
	}
	return nil
}

func TestReporterSend(t *testing.T) {
	reporter := &reporter{
		reg:    metrics.DefaultRegistry,
		prefix: "namespace_",
		tags:   map[string]string{"service": "foo"},
		client: &mockClient{
			consumePoints: func(points []*client.Point) {
				if len(points) != 1 {
					t.Errorf("expected exactly 1 point for a meter, got %v", len(points))
				}
				point := points[0]
				if string(point.Name()) != "namespace_endpoint" {
					t.Errorf("bad measurement name %s", point.Name())
				}
				expectedTags := map[string]string{
					"service":  "foo",
					"method":   "GET",
					"protocol": "http",
				}
				actualTags := point.Tags()
				for name, value := range expectedTags {
					if actualTags[name] != value {
						t.Errorf("tag %q value doesn't match, got %q expected %q", name, value, actualTags[name])
					}
				}
				if len(expectedTags) != len(actualTags) {
					t.Errorf("unexpected number of tags: %v", len(actualTags))
				}
				expectedFieldNames := []string{
					"reqs_meter_count",
					"reqs_meter_m1",
					"reqs_meter_m5",
					"reqs_meter_m15",
					"reqs_meter_mean",
				}
				actualFields, _ := point.Fields()
				for _, fieldName := range expectedFieldNames {
					if actualFields[fieldName] == nil {
						t.Errorf("field %v expected but not found", fieldName)
					}
				}
			// 	fields, _ := point.Fields()
			// 	for name, field := range fields {
			// 		t.Errorf("field %v: %v", name, field)
			// 	}
			},
		},
	}

	reqsPerSec := metrics.GetOrRegisterMeter("endpoint.reqs[method:GET,ignored,protocol:http]", nil)
	reqsPerSec.Mark(3)

	reporter.send()
}
