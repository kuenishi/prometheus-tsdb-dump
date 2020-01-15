package writer

import (

	"github.com/prometheus/prometheus/pkg/labels"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
	//	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetWriter struct {
	Filename string
	FileWriter local.FileWriter
	// 	Writer writer.ParquetWriter
}

func NewParquetWriter() (*ParquetWriter, error) {
	filename := "hoge.parquet"
	return &ParquetWriter{}, nil
}

type parquetLine struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (w *ParquetWriter) Write(labels *labels.Labels, timestamps []int64, values []float64) error {
	metric := map[string]string{}
	for _, l := range *labels {
		metric[l.Name] = l.Value
	}

	enc := json.NewEncoder(os.Stdout)
	err := enc.Encode(victoriaMetricsLine{
		Metric:     metric,
		Values:     values,
		Timestamps: timestamps,
	})
	if err != nil {
		return err
	}
	return nil
}
