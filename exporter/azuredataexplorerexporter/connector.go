package azuredataexplorerexporter

import (
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

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
