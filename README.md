# binlog-parser

mysql binlog 文件解析SDK(只解析了部分event)

## Usage
```go
func main() {
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
```
