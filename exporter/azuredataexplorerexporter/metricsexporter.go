package azuredataexplorerexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	metricTypeKey = "metric_type"
	countSuffix   = "_count"
	sumSuffix     = "_sum"
	bucketSuffix  = "_bucket"
	nanValue      = "NaN"
	plusInfValue  = "+Inf"
	minusInfValue = "-Inf"
)

func newMetricsExporter(set component.ExporterCreateSettings, cfg config.Exporter) (component.MetricsExporter, error) {
	config := cfg.(*Config)
	adxme := &AzureDataExplorerExporter{
		config:        config,
		tablesCreated: false,
	}
	adxme.Connect()
	return exporterhelper.NewMetricsExporter(cfg, set, adxme.metricPusher)
}

func (adxme *AzureDataExplorerExporter) pushMetricData(ctx context.Context, metricData []string, tableName string) {

	adxme.ingestData(ctx, metricData, tableName)

}

func (adxme *AzureDataExplorerExporter) metricPusher(ctx context.Context, metricData pmetric.Metrics) error {
	fmt.Println(" ---------------------- >>>>>>>>> ################# MetricPusher  : ")
	resourceMetric := metricData.ResourceMetrics()
	for i := 0; i < resourceMetric.Len(); i++ {
		resource := resourceMetric.At(i).Resource()
		commonFields := map[string]interface{}{}
		host := adxme.config.HostName
		metricFieldName := "unknown_metricName"

		resource.Attributes().Range(func(k string, v pcommon.Value) bool {
			switch k {
			case "host.name":
				host = v.StringVal()
			default:
				commonFields[k] = v.AsString()
			}
			return true
		})
		scopeMetrics := resourceMetric.At(i).ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			//scope := scopeMetrics.At(j).Scope()
			metrics := scopeMetrics.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {

				metric := metrics.At(k)
				metricFieldName = metric.Name()
				fmt.Println(" ---------------------- >>>>>>>>> ################# Name  : ", metric.Name())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Datatype: ", metric.DataType())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Desc : ", metric.Description())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Metric Attrib : ", resource.Attributes().AsRaw())

				switch metric.DataType() {
				case pmetric.MetricDataTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						dataPoint := dataPoints.At(dpi)
						timestamp := dataPoint.Timestamp().AsTime().Format(time.RFC3339)
						fields := cloneMap(commonFields)
						var metricValue float64
						fmt.Println(" ---------------------- >>>>>>>>> ################# TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# DataType: %v Vals: %v %v ValueType : %v", metric.DataType(), dataPoint.IntVal(), dataPoint.DoubleVal(), dataPoint.ValueType().String())
						fmt.Println(" ---------------------- >>>>>>>>> ################# Attribs: ", dataPoint.Attributes().AsRaw())
						switch dataPoint.ValueType() {
						case pmetric.NumberDataPointValueTypeInt:
							metricValue = float64(dataPoint.IntVal())
						case pmetric.NumberDataPointValueTypeDouble:
							metricValue = dataPoint.DoubleVal()
						}

						fieldsJson, _ := json.Marshal(fields)
						metricData := []string{timestamp, metricFieldName, pmetric.MetricDataTypeGauge.String(), strconv.FormatFloat(metricValue, 'E', -1, 64), host, string(fieldsJson)}
						adxme.pushMetricData(ctx, metricData, metricTable)
					}
				case pmetric.MetricDataTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						var metricValue float64
						dataPoint := dataPoints.At(dpi)
						bounds := dataPoint.ExplicitBounds() // ExplicitBounds : [0.005 0.01 0.025 0.05 0.1 0.25 0.5 1 2.5 5 10]
						counts := dataPoint.BucketCounts()   //  BuckeCounts : 	 [0     0    0     0    0   1    0   0 1   0  4 59]
						timestamp := dataPoint.Timestamp().AsTime().Format(time.RFC3339)
						fmt.Println(" ---------------------- >>>>>>>>> ################# TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String(), dataPoint.StartTimestamp().AsTime().UTC())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# DataType: %v | BuckeCounts : %v | Counts : %v | ExplicitBounds : %v | Flags: %v | HasSum : %v | Sum : %v | ExemplarCount :  %v \n", metric.DataType(), dataPoint.BucketCounts(), dataPoint.Count(), dataPoint.ExplicitBounds(), dataPoint.Flags().String(), dataPoint.HasSum(), dataPoint.Sum(), dataPoint.Exemplars().Len())
						fmt.Println(" ---------------------- >>>>>>>>> ################# Attribs: ", dataPoint.Attributes().AsRaw())
						exemplars := dataPoint.Exemplars()
						for ei := 0; ei < exemplars.Len(); ei++ {
							exemplar := exemplars.At(ei)
							fmt.Printf("---------------------- >>>>>>>>> ################# Exempler : spanId:  %v TraceId : %v  | ValueType: %v | DubleVal: %v | IntVal: %v | Timestamp: %v ", exemplar.SpanID(), exemplar.TraceID(), exemplar.ValueType(), exemplar.DoubleVal(), exemplar.IntVal(), exemplar.Timestamp().AsTime().UTC())
							fmt.Printf("---------------------- >>>>>>>>> ################# Exempler : FilteredAttribs :  %v ", exemplar.FilteredAttributes().AsRaw())
						}

						{
							fields := cloneMap(commonFields)
							populateAttributes(fields, dataPoint.Attributes())
							metricValue = dataPoint.Sum()

							fieldsJson, _ := json.Marshal(fields)
							metricData := []string{timestamp, metricFieldName + sumSuffix, pmetric.MetricDataTypeHistogram.String(), strconv.FormatFloat(metricValue, 'E', -1, 64), host, string(fieldsJson)}
							adxme.pushMetricData(ctx, metricData, metricTable)
						}
						{
							fields := cloneMap(commonFields)
							populateAttributes(fields, dataPoint.Attributes())
							metricValue = float64(dataPoint.Count())
							fields[metricTypeKey] = pmetric.MetricDataTypeHistogram.String()
							fieldsJson, _ := json.Marshal(fields)
							metricData := []string{timestamp, metricFieldName + countSuffix, pmetric.MetricDataTypeHistogram.String(), strconv.FormatFloat(metricValue, 'E', -1, 64), host, string(fieldsJson)}
							adxme.pushMetricData(ctx, metricData, metricTable)
						}
						// Spec says counts is optional but if present it must have one more
						// element than the bounds array.
						if len(counts) == 0 || len(counts) != len(bounds)+1 {
							continue
						}
						value := uint64(0)
						// now create buckets for each bound.
						for bi := 0; bi < len(bounds); bi++ {
							fields := cloneMap(commonFields)
							populateAttributes(fields, dataPoint.Attributes())
							fields["le"] = float64ToDimValue(bounds[bi])
							value += counts[bi]
							fmt.Printf(" ---------------------- >>>>>>>>> ################# Pushing bucket Timestamp: %v |  boundIndex: %v | DataType: %v | Count at bound : %v | Value : %v |  \n", timestamp, bi, metricFieldName+bucketSuffix, counts[bi], value)
							metricValue = float64(value)
							fields[metricTypeKey] = pmetric.MetricDataTypeHistogram.String()
							fieldsJson, _ := json.Marshal(fields)
							metricData := []string{timestamp, metricFieldName + bucketSuffix, pmetric.MetricDataTypeHistogram.String(), strconv.FormatFloat(metricValue, 'E', -1, 64), host, string(fieldsJson)}
							adxme.pushMetricData(ctx, metricData, metricTable)
						}
						// add an upper bound for +Inf
						{
							fields := cloneMap(commonFields)
							populateAttributes(fields, dataPoint.Attributes())
							fields["le"] = float64ToDimValue(math.Inf(1))
							value += counts[len(counts)-1]
							metricValue = float64(value)
							fields[metricTypeKey] = pmetric.MetricDataTypeHistogram.String()
							fieldsJson, _ := json.Marshal(fields)
							metricData := []string{timestamp, metricFieldName + bucketSuffix, pmetric.MetricDataTypeHistogram.String(), strconv.FormatFloat(metricValue, 'E', -1, 64), host, string(fieldsJson)}
							adxme.pushMetricData(ctx, metricData, metricTable)

						}

					}
				case pmetric.MetricDataTypeSum:
					dataPoints := metric.Sum().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						dataPoint := dataPoints.At(dpi)
						timestamp := dataPoint.Timestamp().AsTime().Format(time.RFC3339)
						var metricValue float64
						fmt.Println(" ---------------------- >>>>>>>>> ################# TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# DataType: %v Vals: %v %v ValueType : %v  | ExemplarsCount : %v\n", metric.DataType(), dataPoint.IntVal(), dataPoint.DoubleVal(), dataPoint.ValueType().String(), dataPoint.Exemplars().Len())
						fmt.Println(" ---------------------- >>>>>>>>> ################# Attribs: ", dataPoint.Attributes().AsRaw())
						fields := cloneMap(commonFields)
						populateAttributes(fields, dataPoint.Attributes())
						switch dataPoint.ValueType() {
						case pmetric.NumberDataPointValueTypeInt:
							metricValue = float64(dataPoint.IntVal())
						case pmetric.NumberDataPointValueTypeDouble:
							metricValue = dataPoint.DoubleVal()
						}
						fieldsJson, _ := json.Marshal(fields)
						metricData := []string{timestamp, metricFieldName, pmetric.MetricDataTypeSum.String(), strconv.FormatFloat(metricValue, 'E', -1, 64), host, string(fieldsJson)}
						adxme.pushMetricData(ctx, metricData, metricTable)
					}
				case pmetric.MetricDataTypeExponentialHistogram:
					dataPoints := metric.ExponentialHistogram().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						dataPoint := dataPoints.At(dpi)
						fmt.Println(" ---------------------- >>>>>>>>> ################# dataPoints TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String(), dataPoint.StartTimestamp().AsTime().UTC())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# dataPoints DataType: %v | Flags : %v | Count : %v | negative: %v | positive : %v | Scale: %v | ZeroCount : %v", metric.DataType(), dataPoint.Flags().String(), dataPoint.Count(), dataPoint.Negative(), dataPoint.Positive(), dataPoint.Scale(), dataPoint.ZeroCount())
						fmt.Println(" ---------------------- >>>>>>>>> ################# dataPoints Attribs: ", dataPoint.Attributes().AsRaw())
						exemplars := dataPoint.Exemplars()
						for ei := 0; ei < exemplars.Len(); ei++ {
							exemplar := exemplars.At(ei)
							fmt.Printf("---------------------- >>>>>>>>> ################# Exempler : spanId:  %v TraceId : %v  | ValueType: %v | DubleVal: %v | IntVal: %v | Timestamp: %v ", exemplar.SpanID(), exemplar.TraceID(), exemplar.ValueType(), exemplar.DoubleVal(), exemplar.IntVal(), exemplar.Timestamp().AsTime().UTC())
							fmt.Printf("---------------------- >>>>>>>>> ################# Exempler : FilteredAttribs :  %v ", exemplar.FilteredAttributes().AsRaw())
						}
					}
				case pmetric.MetricDataTypeNone:
					fmt.Printf("This is NONE metric data type: %v", metric.DataType())
				default:
					fmt.Printf("This is invalid metric data type: %v", metric.DataType())
				}

			}
		}
	}

	return nil
}

func populateAttributes(fields map[string]interface{}, attributeMap pcommon.Map) {
	attributeMap.Range(func(k string, v pcommon.Value) bool {
		fields[k] = v.AsString()
		return true
	})
}

func cloneMap(fields map[string]interface{}) map[string]interface{} {
	newFields := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		newFields[k] = v
	}
	return newFields
}

func float64ToDimValue(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
