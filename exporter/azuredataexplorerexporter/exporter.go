package azuredataexplorerexporter

import (
	"context"
	"io"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/ingest"
)

type AzureDataExplorerExporter struct {
	config        *Config
	client        *kusto.Client
	ingesters     map[string]localIngester
	authorizer    kusto.Authorization
	tablesCreated bool
}

type localIngester interface {
	FromReader(ctx context.Context, reader io.Reader, options ...ingest.FileOption) (*ingest.Result, error)
}
