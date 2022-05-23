package azuredataexplorerexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
)

type Config struct {
	config.ExporterSettings `mapstructure:",squash"`
	Endpoint                string `mapstructure:"endpoint"`
	AppId                   string `mapstructure:"appid"`
	SecretId                string `mapstructure:"secretid"`
	TenentId                string `mapstructure:"tenentid"`
	DBName                  string `mapstructure:"dbname"`
}

func NewFactory() component.ExporterFactory {

	return component.NewExporterFactory(
		"azuredataexplorer",
		createDefaultConfig,
		component.WithLogsExporter(createLogsExporter),
		component.WithTracesExporter(createTraceExporter),
		component.WithMetricsExporter(createMetricsExporter),
	)

}

func createDefaultConfig() config.Exporter {
	cfg := &Config{
		ExporterSettings: config.NewExporterSettings(config.NewComponentID("azuredataexplorer")),
		Endpoint:         "",
	}

	return cfg
}

func createTraceExporter(
	_ context.Context,
	set component.ExporterCreateSettings,
	cfg config.Exporter,
) (exp component.TracesExporter, err error) {
	return newTraceExporter(set, cfg)
}

func createLogsExporter(
	_ context.Context,
	set component.ExporterCreateSettings,
	cfg config.Exporter,
) (exp component.LogsExporter, err error) {
	return newLogsExporter(set, cfg)
}

func createMetricsExporter(
	_ context.Context,
	set component.ExporterCreateSettings,
	cfg config.Exporter,
) (exp component.MetricsExporter, err error) {
	return newMetricsExporter(set, cfg)
}
