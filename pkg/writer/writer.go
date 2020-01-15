package writer

import (
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
)

type Writer interface {
	Write(*labels.Labels, []int64, []float64) error
	Close()
}

func NewWriter(format string) (Writer, error) {
	switch format {
	case "victoriametrics":
		return NewVictoriaMetricsWriter()
	case "parquet":
		return NewParquetWriter()
	}
	return nil, fmt.Errorf("invalid format: %s", format)
}
