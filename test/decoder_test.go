package test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/obgnail/binlog-parser"
)

func TestDecoder(t *testing.T) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	decoder, err := binlog.NewBinFileDecoder("./testdata/mysql-bin.000004")

	if err != nil {
		t.Error(err)
		return
	}

	f, _ := decoder.BinFile.Stat()
	fmt.Println("Binlog file size:", f.Size()>>10>>10, "MB")
	starTime := time.Now()

	count := 0
	maxCount := 0
	err = decoder.WalkEvent(func(event *binlog.BinEvent) (isContinue bool, err error) {
		fmt.Println(event.Header)
		count++
		return maxCount > count || maxCount == 0, nil
	})

	duration := time.Since(starTime)
	fmt.Println("Time total:", duration.String())

	speed := float64(f.Size()>>10>>10) / duration.Seconds()
	fmt.Printf("Speed: %.2f MB/s\n", speed)

	if err != nil {
		t.Error(err)
	}

	runtime.ReadMemStats(memStats)
	fmt.Println("GC times:", memStats.NumGC)
	pauseTotal := time.Duration(int64(memStats.PauseTotalNs))
	fmt.Println("Pause total:", pauseTotal.String())
}

func TestBinLog(t *testing.T) {
	decoder, err := binlog.NewBinFileDecoder("./testdata/mysql-bin.000004")
	if err != nil {
		panic(err)
	}

	err = decoder.WalkEvent(func(event *binlog.BinEvent) (isContinue bool, err error) {
		eventType, _ := event.GetType()
		fmt.Printf("Got %s\n", eventType)
		fmt.Println(event.Header)
		if event.Body != nil {
			fmt.Println(event.Body)
		}
		fmt.Println(strings.Repeat("=", 100))
		return true, nil
	})

	if err != nil {
		panic(err)
	}
}
