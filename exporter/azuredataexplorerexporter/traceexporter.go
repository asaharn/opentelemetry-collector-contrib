package azuredataexplorerexporter

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func newTraceExporter(set component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	config := cfg.(*Config)
	te := &AzureDataExplorerExporter{
		config:        config,
		tablesCreated: false,
	}
	te.Connect()

	return exporterhelper.NewTracesExporter(cfg, set, te.pusher)
}

func createJSONData(spanData ptrace.Span) string {
	attributes, _ := json.Marshal(spanData.Attributes().AsRaw())
	return `{"trace_id":"` + spanData.TraceID().HexString() + `","parent_id":"` + spanData.ParentSpanID().HexString() + `","span_id":"` + spanData.SpanID().HexString() + `","span_name":"` + spanData.Name() + `","span_status":"` + spanData.Status().Message() + `","span_kind":"` + spanData.Kind().String() + `","start_time":"` + spanData.StartTimestamp().AsTime().String() + `","end_time":"` + spanData.EndTimestamp().String() + `","Attributes":"` + string(attributes) + `"}`
}

func LinkedEventsString(spandData ptrace.Span) string {
	finalString := "{"
	for i := 0; i < spandData.Events().Len(); i++ {
		eventObj := `{ "eventName":"` + spandData.Events().At(i).Name() + `", eventTime: "` + spandData.Events().At(i).Timestamp().String() + `"},`
		finalString += eventObj
	}
	finalString += "}"
	fmt.Println("########", finalString)

	return finalString
}

func LinkedSpansString(spandData ptrace.Span) string {
	finalString := "{"
	for i := 0; i < spandData.Links().Len(); i++ {
		eventObj := `{ "spanId":"` + spandData.Links().At(i).SpanID().HexString() + `", traceId: "` + spandData.Links().At(i).TraceID().HexString() + `", traceState" : "` + string(spandData.Links().At(i).TraceState()) + `" },"`
		finalString += eventObj
	}
	finalString += "}"
	fmt.Println("########", finalString)

	return finalString
}

func (te *AzureDataExplorerExporter) pusher(ctx context.Context, traceData ptrace.Traces) error {
	resourceSpans := traceData.ResourceSpans()

	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)

		resource := resourceSpan.Resource()
		scopeSpansSlice := resourceSpan.ScopeSpans()

		for j := 0; j < scopeSpansSlice.Len(); j++ {

			scope := scopeSpansSlice.At(j).Scope()
			spanSlice := scopeSpansSlice.At(j).Spans()

			for k := 0; k < spanSlice.Len(); k++ {
				spanData := spanSlice.At(k)
				//jsondata, _ := json.Marshal(spanData.Attributes().AsRaw())
				// trace_id: string, span_id:string, span_name: string, span_status: string, span_kind:string , start_time: datetime, end_time: datetime,  Attributes: dynamic
				fmt.Println("-------->>>>>>>>###############  scope INfo ", scope.Name(), scope.Version())
				fmt.Println("-------->>>>>>>>###############  resource INfo ", resource.Attributes().AsRaw())
				attribJSON, _ := json.Marshal(spanData.Attributes().AsRaw())
				//dataToSend := createJSONData(spanData)
				//dataToSend := spanData.TraceID().HexString() + ", " + spanData.ParentSpanID().HexString() + "," + spanData.SpanID().HexString() + "," + spanData.Name() + "," + spanData.Status().Message() + "," + spanData.Kind().String() + "," + spanData.StartTimestamp().String() + "," + spanData.EndTimestamp().AsTime().String() // + "," + string(attribJSON)
				dataToSendSlice := []string{spanData.TraceID().HexString(), spanData.ParentSpanID().HexString(), spanData.SpanID().HexString(), spanData.Name(), spanData.Status().Code().String(), spanData.Kind().String(), spanData.StartTimestamp().String(), spanData.EndTimestamp().AsTime().String(), string(attribJSON), LinkedEventsString(spanData), LinkedSpansString(spanData)}

				te.ingestData(ctx, dataToSendSlice, traceTable)

				fmt.Println("##############>>>>>>>>>>>>>>>>>>>         ProcessEnded")
			}
		}

	}
	return nil
}
