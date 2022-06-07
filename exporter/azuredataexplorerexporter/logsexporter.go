package azuredataexplorerexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
)

func newLogsExporter(set component.ExporterCreateSettings, cfg config.Exporter) (component.LogsExporter, error) {

	config := cfg.(*Config)
	te := &AzureDataExplorerExporter{
		config:        config,
		tablesCreated: false,
	}
	te.Connect()

	return exporterhelper.NewLogsExporter(cfg, set, te.logPusher)
}

func (adxe *AzureDataExplorerExporter) logPusher(ctx context.Context, logData plog.Logs) error {
	fmt.Println("-------->>>>>>>>############### LOGS init ", logData)
	resourceLogs := logData.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		resource := resourceLogs.At(i)
		resourceAttribJSON, _ := json.Marshal(resource.Resource().Attributes().AsRaw())
		scopeLogs := resourceLogs.At(i).ScopeLogs()
		fmt.Println("-------->>>>>>>>############### scope LOGS ", scopeLogs)
		for j := 0; j < scopeLogs.Len(); j++ {
			scope := scopeLogs.At(j)
			logs := scopeLogs.At(j).LogRecords()
			fmt.Println("-------->>>>>>>>############### LOGS ", logs)

			for k := 0; k < logs.Len(); k++ {
				logData := logs.At(k)
				attribJSON, _ := json.Marshal(logData.Attributes().AsRaw())
				fmt.Printf(" ---------------------- >>>>>>>>> ################# Resource: %v | InsScope: %v  | Timestamp: %v | ObsTimeStamp : %v |  TraceId: %v | SpanId: %v | SeverityNum : %v | SeveretyText : %v | Body: %v | LogName: %v | Attributes : %v  ", resource.Resource().Attributes().AsRaw(), scope.Scope().Name(), logs.At(k).Timestamp().AsTime().Format(time.RFC3339Nano), logs.At(k).ObservedTimestamp().AsTime().Format(time.RFC3339Nano), logs.At(k).TraceID(), logs.At(k).SpanID(), logs.At(k).SeverityNumber(), logs.At(k).SeverityText(), logs.At(k).Body().AsString(), logs.At(k).Name(), logs.At(k).Attributes().AsRaw())
				dataToSendSlice := []string{logData.Timestamp().AsTime().Format(time.RFC3339Nano), logData.ObservedTimestamp().AsTime().Format(time.RFC3339Nano), logData.TraceID().HexString(), logData.SpanID().HexString(), logData.SeverityText(), logData.SeverityNumber().String(), logData.Body().AsString(), string(resourceAttribJSON), scope.Scope().Name(), string(attribJSON)}
				adxe.ingestData(ctx, dataToSendSlice, logTable)
			}
		}
	}
	return nil
}
