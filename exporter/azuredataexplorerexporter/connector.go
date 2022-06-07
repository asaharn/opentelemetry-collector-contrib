package azuredataexplorerexporter

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

const traceTable = "TraceSpans2"
const metricTable = "RawMetricsData"
const logTable = "LogsData"

const createTraceTableCommand = `.create-merge tables  %s ( trace_id: string, parent_id: string, span_id:string, span_name: string, span_status: string, span_kind:string , start_time: string, end_time: string,  Attributes: dynamic,  events: dynamic,  links: dynamic   )`

// query need to be discussed
const createTraceTableMappingCommand = `.create-or-alter table ['%s'] ingestion csv mapping '%s_mapping' '[{"Column": "trace_id", "Properties": {"Ordinal": "0"}},{"Column": "parent_id", "Properties": {"Ordinal": "1"}},{"Column": "span_id", "Properties": {"Ordinal": "2"}},{"Column": "span_name", "Properties": {"Ordinal": "3"}},{"Column": "span_status", "Properties": {"Ordinal": "4"}},{"Column": "span_kind", "Properties": {"Ordinal": "5"}},{"Column": "start_time", "Properties": {"Ordinal": "6"}},{"Column": "end_time", "Properties": {"Ordinal": "7"}},{"Column": "Attributes", "Properties": {"Ordinal": "8"}},{"Column": "events", "Properties": {"Ordinal": "9"}}, {"Column": "links", "Properties": {"Ordinal": "10"}}]'`

//const createTableMapping = `.create table ['%s'] ingestion json mapping '%s_mapping' '[{"Column": "trace_id", "Properties": {"Path": "$[\'trace_id\']"}},{"Column": "parent_id", "Properties": {"Path": "$.parent_id"}},{"Column": "span_id", "Properties": {"Path": "$.span_id"}},{"Column": "span_name", "Properties": {"Path": "$.span_name"}},{"Column": "span_status", "Properties": {"Path": "$.span_status"}},{"Column": "span_kind", "Properties": {"Path": "$.span_kind"}},{"Column": "start_time", "Properties": {"Path": "$.start_time"}},{"Column": "end_time","Properties": {"Path": "$.end_time"}},{"Column": "Attributes","Properties": {"Path": "$.Attributes"}}]'`
//const createMetricTableCommand = `.create-merge tables  %s ( name: string, data_type: string, unit:string, description: string, resc_attributes: dynamic )`
//const createMetricTableMappingCommand = `.create-or-alter table ['%s'] ingestion csv mapping '%s_mapping' '[{"Column": "name", "Properties": {"Ordinal": "0"}},{"Column": "data_type", "Properties": {"Ordinal": "1"}},{"Column": "unit", "Properties": {"Ordinal": "2"}},{"Column": "description", "Properties": {"Ordinal": "3"}}, {"Column": "resc_attributes", "Properties": {"Ordinal": "4"}}]'`
const createMetricTableCommand = `.create-merge tables  %s ( Timestamp: datetime, MetricName: string, MetricType:string, Value: double, Host: string, Fields: dynamic )`
const createMetricTableMappingCommand = `.create-or-alter table ['%s'] ingestion csv mapping '%s_mapping' '[{"Column": "Timestamp", "Properties": {"Ordinal": "0"}},{"Column": "MetricName", "Properties": {"Ordinal": "1"}},{"Column": "MetricType", "Properties": {"Ordinal": "2"}}, {"Column": "Value", "Properties": {"Ordinal": "3"}}, {"Column": "Host", "Properties": {"Ordinal": "4"}}, {"Column": "Fields", "Properties": {"Ordinal": "5"}}]'`
const createLogTableCommand = `.create-merge tables  %s ( Timestamp: datetime, ObservedTimestamp: datetime, TraceId: string, SpanId:string, SeverityText: string, SeverityNumber: string, Body: string, ResourceData: dynamic, InstrumentationScope: string, Attributes: dynamic )`
const createLogTableMappingCommand = `.create-or-alter table ['%s'] ingestion csv mapping '%s_mapping' '[{"Column": "Timestamp", "Properties": {"Ordinal": "0"}}, {"Column": "ObservedTimestamp", "Properties": {"Ordinal": "1"}},{"Column": "TraceId", "Properties": {"Ordinal": "2"}},{"Column": "SpanId", "Properties": {"Ordinal": "3"}}, {"Column": "SeverityText", "Properties": {"Ordinal": "4"}}, {"Column": "SeverityNumber", "Properties": {"Ordinal": "5"}}, {"Column": "Body", "Properties": {"Ordinal": "6"}}, {"Column": "ResourceData", "Properties": {"Ordinal": "7"}}, {"Column": "InstrumentationScope", "Properties": {"Ordinal": "8"}}, {"Column": "Attributes", "Properties": {"Ordinal": "9"}}]'`

var createTableCmd = map[string]string{
	traceTable:  fmt.Sprintf(createTraceTableCommand, traceTable),
	metricTable: fmt.Sprintf(createMetricTableCommand, metricTable),
	logTable:    fmt.Sprintf(createLogTableCommand, logTable),
}

var createTableMappingCmd = map[string]string{
	traceTable:  fmt.Sprintf(createTraceTableMappingCommand, traceTable, traceTable),
	metricTable: fmt.Sprintf(createMetricTableMappingCommand, metricTable, metricTable),
	logTable:    fmt.Sprintf(createLogTableMappingCommand, logTable, logTable),
}

func (adxe *AzureDataExplorerExporter) Connect() {

	authorizer := kusto.Authorization{
		Config: auth.NewClientCredentialsConfig(adxe.config.AppId,
			adxe.config.SecretId, adxe.config.TenentId),
	}
	client, err := kusto.New(adxe.config.Endpoint, authorizer)

	if err != nil {
		panic("Kusto client connection issue" + err.Error())
	}

	adxe.client = client
	adxe.authorizer = authorizer
	adxe.ingesters = make(map[string]localIngester)

}

func (adxte *AzureDataExplorerExporter) createADXTable(ctx context.Context, tableName string) error {

	if adxte.tablesCreated {
		return nil
	}

	//query := ".create-merge tables  TraceSpans2 ( trace_id: string, span_id:string, span_name: string, span_status: string, span_kind:string , start_time: datetime, end_time: datetime,  Attributes: dynamic )"
	createTableCmd := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(createTableCmd[tableName])
	if _, err := adxte.client.Mgmt(ctx, adxte.config.DBName, createTableCmd, kusto.AllowWrite()); err != nil {
		return err
	}

	createTableMappingCmd := kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true, SuppressWarning: true})).UnsafeAdd(createTableMappingCmd[tableName])
	if _, err := adxte.client.Mgmt(ctx, adxte.config.DBName, createTableMappingCmd, kusto.AllowWrite()); err != nil {
		return err
	}

	adxte.tablesCreated = true
	return nil
}

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

func (adxe *AzureDataExplorerExporter) ingestData(ctx context.Context, dataToIngestSlice []string, tableName string) {
	buf := new(bytes.Buffer)
	//wr is a writer that will write to buffer buf
	wr := csv.NewWriter(buf)
	wr.Write(dataToIngestSlice)
	wr.Flush()
	fmt.Println("############# >>>>>>>>>>>>  Data : ", buf.String())
	// send data to kusto
	// Ingesting the data

	ingester, err := adxe.getIngester(ctx, tableName)
	format := ingest.FileFormat(ingest.CSV)
	//mapping := ingest.IngestionMappingRef("TraceSpans2_mapping", ingest.JSON)
	//
	mapping := ingest.IngestionMappingRef(fmt.Sprintf("%s_mapping", tableName), ingest.CSV)

	if err != nil {
		fmt.Println("############# >>>>>>>>>>>> Ingester Error", err.Error())
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
		fmt.Println("############# >>>>>>>>>>>>", err)
		panic("############# >>>>>>>>>>>> ERRORRRR")
	}

}
