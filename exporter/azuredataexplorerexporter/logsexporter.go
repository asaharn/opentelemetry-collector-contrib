package azuredataexplorerexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
)

func newLogsExporter(set component.ExporterCreateSettings, cfg config.Exporter) (component.LogsExporter, error) {
	return exporterhelper.NewLogsExporter(cfg, set, func(ctx context.Context, logData plog.Logs) error {
		fmt.Println("-------->>>>>>>>############### ", logData)
		resourceLogs := logData.ResourceLogs()
		for i := 0; i < resourceLogs.Len(); i++ {
			scopeLogs := resourceLogs.At(i).ScopeLogs()
			for j := 0; j < scopeLogs.Len(); j++ {
				logs := scopeLogs.At(j).LogRecords()
				for k := 0; k < logs.Len(); k++ {
					fmt.Println(" ---------------------- >>>>>>>>> #################", logs.At(k).Body().AsString())
				}
			}
		}
		return nil
	})
}
