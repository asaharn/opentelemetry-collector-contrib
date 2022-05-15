package azuredataexplorerexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func newMetricsExporter(set component.ExporterCreateSettings, cfg config.Exporter) (component.MetricsExporter, error) {
	return exporterhelper.NewMetricsExporter(cfg, set, pusher)
}

func pusher(ctx context.Context, metricData pmetric.Metrics) error {

	resourceMetric := metricData.ResourceMetrics()
	for i := 0; i < resourceMetric.Len(); i++ {
		scopeMetrics := resourceMetric.At(i).ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			metrics := scopeMetrics.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				fmt.Println(" ---------------------- >>>>>>>>> ################# Name  : ", metrics.At(k).Name())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Datatype: ", metrics.At(k).DataType())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Unit: ", metrics.At(k).Unit())
				fmt.Println(" ---------------------- >>>>>>>>> ################# Desc : ", metrics.At(k).Description())

			}
		}
	}

	return nil
}
