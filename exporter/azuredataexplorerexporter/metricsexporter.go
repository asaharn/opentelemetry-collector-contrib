package azuredataexplorerexporter

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
		attribJSON, _ := json.Marshal(resource.Attributes().AsRaw())
		scopeMetrics := resourceMetric.At(i).ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			//scope := scopeMetrics.At(j).Scope()
			metrics := scopeMetrics.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metricData := []string{}
				metric := metrics.At(k)
				fmt.Println(" ---------------------- >>>>>>>>> ################# Name  : ", metric.Name())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Datatype: ", metric.DataType())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Desc : ", metric.Description())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Metric Attrib : ", resource.Attributes().AsRaw())

				switch metric.DataType() {
				case pmetric.MetricDataTypeGauge:
					dataPoints := metric.Gauge().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						dataPoint := dataPoints.At(dpi)
						fmt.Println(" ---------------------- >>>>>>>>> ################# TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# DataType: %v Vals: %v %v ValueType : %v", metric.DataType(), dataPoint.IntVal(), dataPoint.DoubleVal(), dataPoint.ValueType().String())
						fmt.Println(" ---------------------- >>>>>>>>> ################# Attribs: ", dataPoint.Attributes().AsRaw())
					}
				case pmetric.MetricDataTypeHistogram:
					dataPoints := metric.Histogram().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						dataPoint := dataPoints.At(dpi)
						fmt.Println(" ---------------------- >>>>>>>>> ################# TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String(), dataPoint.StartTimestamp().AsTime().UTC())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# DataType: %v | BuckeCounts : %v | Counts : %v | ExplicitBounds : %v | Flags: %v | HasSum : %v | Sum : %v | ExemplarCount :  %v \n", metric.DataType(), dataPoint.BucketCounts(), dataPoint.Count(), dataPoint.ExplicitBounds(), dataPoint.Flags().String(), dataPoint.HasSum(), dataPoint.Sum(), dataPoint.Exemplars().Len())
						fmt.Println(" ---------------------- >>>>>>>>> ################# Attribs: ", dataPoint.Attributes().AsRaw())
						exemplars := dataPoint.Exemplars()
						for ei := 0; ei < exemplars.Len(); ei++ {
							exemplar := exemplars.At(ei)
							fmt.Printf("---------------------- >>>>>>>>> ################# Exempler : spanId:  %v TraceId : %v  | ValueType: %v | DubleVal: %v | IntVal: %v | Timestamp: %v ", exemplar.SpanID(), exemplar.TraceID(), exemplar.ValueType(), exemplar.DoubleVal(), exemplar.IntVal(), exemplar.Timestamp().AsTime().UTC())
							fmt.Printf("---------------------- >>>>>>>>> ################# Exempler : FilteredAttribs :  %v ", exemplar.FilteredAttributes().AsRaw())
						}
					}
				case pmetric.MetricDataTypeSum:
					dataPoints := metric.Sum().DataPoints()
					for dpi := 0; dpi < dataPoints.Len(); dpi++ {
						dataPoint := dataPoints.At(dpi)
						fmt.Println(" ---------------------- >>>>>>>>> ################# TimeStamp: ", dataPoint.Timestamp().AsTime().UTC().String())
						fmt.Printf(" ---------------------- >>>>>>>>> ################# DataType: %v Vals: %v %v ValueType : %v  | ExemplarsCount : %v\n", metric.DataType(), dataPoint.IntVal(), dataPoint.DoubleVal(), dataPoint.ValueType().String(), dataPoint.Exemplars().Len())
						fmt.Println(" ---------------------- >>>>>>>>> ################# Attribs: ", dataPoint.Attributes().AsRaw())
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

				metricData := []string{metric.Name(), metric.DataType().String(), metric.Unit(), metric.Description(), string(attribJSON)}
				adxme.pushMetricData(ctx, metricData, metricTable)
			}
		}
	}

	return nil
}
