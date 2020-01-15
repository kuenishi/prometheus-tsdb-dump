package writer

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"log"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetWriter struct {
	Filename string
	file     source.ParquetFile
	Writer   *writer.ParquetWriter
}

type Entry struct {
	Value     float64 `parquet:"name=value, type=DOUBLE"`
	Timestamp int64   `parquet:"name=value, type=TIMESTAMP_MILLIS"`
	// ↓Can this be a pointer?↓
	Labels *map[string]string `parquet:"name=metric, type=MAP, keytype=UTF8, valuetype=UTF8"`
}

func NewParquetWriter() (*ParquetWriter, error) {
	filename := "hoge.parquet"
	fw, err := local.NewLocalFileWriter(filename)
	if err != nil {
		log.Println("Can't create file", err)
		return nil, err
	}
	pw, err := writer.NewParquetWriter(fw, new(Entry), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return nil, err
	}
	return &ParquetWriter{Writer: pw, file: fw,
		Filename: filename}, nil
}

func (w *ParquetWriter) Write(labels *labels.Labels, timestamps []int64, values []float64) error {
	metric := map[string]string{}
	for _, l := range *labels {
		metric[l.Name] = l.Value
	}

	for i, t := range timestamps {
		e := Entry{
			Value:     values[i],
			Timestamp: t,
			Labels:    &metric,
		}
		if err := w.Writer.Write(e); err != nil {
			log.Println("Write error", err)
			return err
		}
	}
	return nil
}

func (w *ParquetWriter) Close() {
	w.Writer.WriteStop()
	w.file.Close()
}
