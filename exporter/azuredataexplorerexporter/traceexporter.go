package azuredataexplorerexporter

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const createTableCommand = `.create-merge tables  %s ( trace_id: string, parent_id: string, span_id:string, span_name: string, span_status: string, span_kind:string , start_time: string, end_time: string,  Attributes: dynamic )`

// query need to be discussed
const createTableMapping = `.create table ['%s'] ingestion csv mapping '%s_mapping' '[{"Column": "trace_id", "Properties": {"Ordinal": "0"}},{"Column": "parent_id", "Properties": {"Ordinal": "1"}},{"Column": "span_id", "Properties": {"Ordinal": "2"}},{"Column": "span_name", "Properties": {"Ordinal": "3"}},{"Column": "span_status", "Properties": {"Ordinal": "4"}},{"Column": "span_kind", "Properties": {"Ordinal": "5"}},{"Column": "start_time", "Properties": {"Ordinal": "6"}},{"Column": "end_time", "Properties": {"Ordinal": "7"}},{"Column": "Attributes", "Properties": {"Ordinal": "8"}}]'`

//const createTableMapping = `.create table ['%s'] ingestion json mapping '%s_mapping' '[{"Column": "trace_id", "Properties": {"Path": "$[\'trace_id\']"}},{"Column": "parent_id", "Properties": {"Path": "$.parent_id"}},{"Column": "span_id", "Properties": {"Path": "$.span_id"}},{"Column": "span_name", "Properties": {"Path": "$.span_name"}},{"Column": "span_status", "Properties": {"Path": "$.span_status"}},{"Column": "span_kind", "Properties": {"Path": "$.span_kind"}},{"Column": "start_time", "Properties": {"Path": "$.start_time"}},{"Column": "end_time","Properties": {"Path": "$.end_time"}},{"Column": "Attributes","Properties": {"Path": "$.Attributes"}}]'`

func newTraceExporter(set component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	config := cfg.(*Config)

	//query := ".create-merge tables  TraceSpans2 ( HTTP_Flavour:string, HTTP_Host: string ,HTTP_Method: string , HTTP_Scheme: string , HTTP_ServerName: string ,HTTP_StatusCode: string , HTTP_target: string , HTTP_UserAgent:string , HTTP_WroteBytes: string ,NET_HOST_NAME: string , NET_HOST_PORT: string , NET_PEER_IP: string , NET_PEER_PORT: string ,NET_TRANSPORT: string, SERVER_ATTRIB: string )"
	// cmd := kusto.NewStmt("")
	// if _, err := client.Mgmt(context.Background(), config.DBName, cmd, kusto.AllowWrite()); err != nil {
	// 	panic(err)
	// }

	// if err != nil {
	// 	fmt.Println("----######## Conntection : Thats an error!!!  ", err)
	// 	os.Exit(0)
	// }

	fmt.Println("################################ >>>>>>>>>>>>>>>>>>>>> Client Connected: ")

	te := &AzureDataExplorerExporter{
		config:        config,
		tablesCreated: false,
	}
	te.Connect()

	return exporterhelper.NewTracesExporter(cfg, set, te.pusher)
}

// func (adxte *AzureDataExplorerExporter) Connect(config *Config) {

// 	authorizer := kusto.Authorization{
// 		Config: auth.NewClientCredentialsConfig(config.AppId,
// 			config.SecretId, config.TenentId),
// 	}
// 	client, err := kusto.New(config.Endpoint, authorizer)

// 	if err != nil {
// 		panic("Kusto client connection issue" + err.Error())
// 	}

// 	adxte.client = client
// 	adxte.authorizer = authorizer
// 	adxte.ingesters = make(map[string]localIngester)

// }

func (adxte *AzureDataExplorerExporter) getIngester(ctx context.Context, tableName string) (localIngester, error) {

	ingester := adxte.ingesters[tableName]

	if ingester == nil {
		if err := adxte.createADXTable(ctx, tableName); err != nil {
			return nil, fmt.Errorf("Table creation failed : %v, Error : %v", tableName, err)
		}

		tempIngester, err := ingest.New(adxte.client, adxte.config.DBName, tableName)

		if err != nil {
			return nil, fmt.Errorf("Ingestor creation failed : %v, Error : %v", tableName, err)
		}
		adxte.ingesters[tableName] = tempIngester
		ingester = tempIngester
	}
	return ingester, nil
}

func (adxte *AzureDataExplorerExporter) createADXTable(ctx context.Context, tableName string) error {

	if adxte.tablesCreated {
		return nil
	}

	//query := ".create-merge tables  TraceSpans2 ( trace_id: string, span_id:string, span_name: string, span_status: string, span_kind:string , start_time: datetime, end_time: datetime,  Attributes: dynamic )"
	createTableCmd := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(fmt.Sprintf(createTableCommand, tableName))
	if _, err := adxte.client.Mgmt(ctx, adxte.config.DBName, createTableCmd, kusto.AllowWrite()); err != nil {
		return err
	}

	createTableMappingCmd := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(fmt.Sprintf(createTableMapping, tableName, tableName))
	if _, err := adxte.client.Mgmt(ctx, adxte.config.DBName, createTableMappingCmd, kusto.AllowWrite()); err != nil {
		return err
	}

	adxte.tablesCreated = true
	return nil
}

func createJSONData(spanData ptrace.Span) string {
	attributes, _ := json.Marshal(spanData.Attributes().AsRaw())
	return `{"trace_id":"` + spanData.TraceID().HexString() + `","parent_id":"` + spanData.ParentSpanID().HexString() + `","span_id":"` + spanData.SpanID().HexString() + `","span_name":"` + spanData.Name() + `","span_status":"` + spanData.Status().Message() + `","span_kind":"` + spanData.Kind().String() + `","start_time":"` + spanData.StartTimestamp().AsTime().String() + `","end_time":"` + spanData.EndTimestamp().String() + `","Attributes":"` + string(attributes) + `"}`
}

func (te *AzureDataExplorerExporter) pusher(ctx context.Context, traceData ptrace.Traces) error {
	fmt.Println("################################ >>>>>>>>>> PUSHER IN ACTION")
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
				fmt.Println("-------->>>>>>>>###############  resource INfo ", resource.Attributes(), resource.Attributes().AsRaw())
				attribJSON, _ := json.Marshal(spanData.Attributes().AsRaw())

				//dataToSend := createJSONData(spanData)
				//dataToSend := spanData.TraceID().HexString() + ", " + spanData.ParentSpanID().HexString() + "," + spanData.SpanID().HexString() + "," + spanData.Name() + "," + spanData.Status().Message() + "," + spanData.Kind().String() + "," + spanData.StartTimestamp().String() + "," + spanData.EndTimestamp().AsTime().String() // + "," + string(attribJSON)
				dataToSendSlice := []string{spanData.TraceID().HexString(), spanData.ParentSpanID().HexString(), spanData.SpanID().HexString(), spanData.Name(), spanData.Status().Code().String(), spanData.Kind().String(), spanData.StartTimestamp().String(), spanData.EndTimestamp().AsTime().String(), string(attribJSON)}

				buf := new(bytes.Buffer)
				//wr is a writer that will write to buffer buf
				wr := csv.NewWriter(buf)
				wr.Write(dataToSendSlice)
				wr.Flush()
				fmt.Println("-------->>>>>>>>###############  data", buf.String())
				// send data to kusto
				// Ingesting the data

				ingester, err := te.getIngester(ctx, "TraceSpans2")
				format := ingest.FileFormat(ingest.CSV)
				//mapping := ingest.IngestionMappingRef("TraceSpans2_mapping", ingest.JSON)
				//
				mapping := ingest.IngestionMappingRef("TraceSpans2_mapping", ingest.CSV)

				if err != nil {
					fmt.Println("########## Ingester Error", err.Error())
					os.Exit(1)
				}

				r, w := io.Pipe()
				go func() {
					defer func(writer *io.PipeWriter) {
						err := writer.Close()
						if err != nil {
							fmt.Printf("Failed to close writer %v", err)
						}
					}(w)
					_, err := io.Copy(w, buf)
					if err != nil {
						fmt.Printf("Failed to copy io: %v", err)
					}
				}()

				if _, err := ingester.FromReader(context.Background(), r, format, mapping); err != nil {
					fmt.Println(">>>>>>>>>>>>>>", err)
					panic("############# >>>>>>>>>>>> ERRORRRR")
				}

				fmt.Println("##############>>>>>>>>>>>>>>>>>>>         ProcessEnded")
			}
		}

	}
	return nil
}
