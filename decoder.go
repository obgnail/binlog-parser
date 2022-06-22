package binlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// binFileHeader : A binlog file starts with a Binlog File Header [ fe 'bin' ]
// https://dev.mysql.com/doc/internals/en/binlog-file-header.html
var binFileHeader = []byte{254, 98, 105, 110}

// BinReaderOption will describe the details to tell decoders when it should start and when stop.
// with time [start, end)
type BinReaderOption struct {
	StartPos  int64
	EndPos    int64
	StartTime time.Time
	EndTime   time.Time
}

// Start return bool of if start decoding
func (option *BinReaderOption) Start(header *BinEventHeader) bool {
	if option == nil {
		return true
	} else if option.StartPos != 0 && option.StartPos <= header.LogPos-header.EventSize {
		return true
	} else if option.StartTime.Unix() <= time.Unix(header.Timestamp, 0).Unix() {
		return true
	}
	return false
}

// Stop return bool of if stop decoding
func (option *BinReaderOption) Stop(header *BinEventHeader) bool {
	if option == nil {
		return false
	} else if option.EndPos != 0 && option.EndPos < header.LogPos {
		return true
	} else if !option.EndTime.IsZero() && option.EndTime.Unix() <= time.Unix(header.Timestamp, 0).Unix() {
		return true
	}
	return false
}

// BinaryLogInfo is the base decoder of all types
type BinaryLogInfo struct {
	// cause different version mapping different payload
	// every binary log event analysis depend on descriptions
	description *BinFmtDescEvent
	tableInfo   map[uint64]*BinTableMapEvent
}

// BinFileDecoder will mapping a binary log file, decode binary log event
type BinFileDecoder struct {
	Path string          // binary log path
	prev *BinFileDecoder // prev binary log
	next *BinFileDecoder // next binary log

	// binary log reading options
	Option *BinReaderOption

	// file object
	BinFile *os.File

	// buffer
	buf *bufio.Reader

	*BinaryLogInfo
}

// NewBinFileDecoder return a BinFileDecoder with binary log file path
func NewBinFileDecoder(path string, options ...*BinReaderOption) (*BinFileDecoder, error) {
	decoder := &BinFileDecoder{
		Path: path,
	}
	// set options
	if len(options) > 0 {
		decoder.Option = options[0]
	}

	// decoder init
	err := decoder.init()
	if err != nil {
		return nil, err
	}
	return decoder, nil
}

// Init BinFileDecoder, binary log file validate
func (decoder *BinFileDecoder) init() error {
	// open binary log
	if decoder.BinFile == nil {
		binFile, err := os.Open(decoder.Path)
		if err != nil {
			return err
		}
		decoder.BinFile = binFile
		decoder.buf = bufio.NewReader(decoder.BinFile)
	}

	// binary log header validate
	header := make([]byte, 4)
	if _, err := decoder.BinFile.Read(header); err != nil {
		return err
	}

	if !bytes.Equal(header, binFileHeader) {
		return fmt.Errorf("invalid binary log header {%x}", header)
	}

	decoder.BinaryLogInfo = &BinaryLogInfo{
		tableInfo: make(map[uint64]*BinTableMapEvent),
	}
	return nil
}

// DecodeEvent will decode a single event from binary log
func (decoder *BinFileDecoder) DecodeEvent() (*BinEvent, error) {
	event := &BinEvent{}
	rd := decoder.buf

	// event header固定为19字节
	// 这里是为了兼容不同的binlog版本
	eventHeaderLength := defaultEventHeaderSize
	if decoder.description != nil {
		eventHeaderLength = decoder.description.EventHeaderLength
	}

	// read binlog event header
	headerData, err := ReadNBytes(rd, eventHeaderLength)
	if err != nil {
		return nil, err
	}

	// decode binlog event header
	event.Header, err = decodeEventHeader(headerData, eventHeaderLength)
	if err != nil {
		return nil, err
	}

	if _, ok := EventType2Str[event.Header.EventType]; !ok {
		return nil, fmt.Errorf("got unknown event type {%x}", event.Header.EventType)
	}

	readDataLength := event.Header.EventSize - eventHeaderLength
	// read binlog event body
	var data []byte
	data, err = ReadNBytes(rd, readDataLength)
	if err != nil {
		return nil, err
	}

	// skip data if not start
	// 如果没有跳过,第一个event必须是FormatDescriptionEvent
	if event.Header.EventType != FormatDescriptionEvent && !decoder.Option.Start(event.Header) {
		return nil, err
	}

	data, err = event.Validation(decoder.BinaryLogInfo, headerData, data)
	if err != nil {
		return event, err
	}

	// decode binlog event body
	var eventBody BinEventBody
	switch event.Header.EventType {
	case FormatDescriptionEvent:
		decoder.description, err = decodeFmtDescEvent(data)
		eventBody = decoder.description

	case QueryEvent:
		eventBody, err = decodeQueryEvent(data, decoder.description.BinlogVersion)

	case XIDEvent:
		eventBody, err = decodeXIDEvent(data)

	case IntvarEvent:
		eventBody, err = decodeIntvarEvent(data)

	case RotateEvent:
		eventBody, err = decodeRotateEvent(data, decoder.description.BinlogVersion)

	case TableMapEvent:
		eventBody, err = decodeTableMapEvent(data, decoder.description)
		if err != nil {
			return nil, err
		}
		decoder.tableInfo[eventBody.(*BinTableMapEvent).TableID] = eventBody.(*BinTableMapEvent)

	case WriteRowsEventV0, UpdateRowsEventV0, DeleteRowsEventV0,
		WriteRowsEventV1, UpdateRowsEventV1, DeleteRowsEventV1,
		WriteRowsEventV2, UpdateRowsEventV2, DeleteRowsEventV2:
		// ROWS_EVENT
		eventBody, err = decodeRowsEvent(data, decoder.description, event.Header.EventType)

	case PreviousGTIDEvent, AnonymousGTIDEvent:
		// decode ignore event.
		// TODO: decode AnonymousGTIDEvent
		eventBody, err = decodeUnSupportEvent(data)

	case UnknownEvent:
		return nil, fmt.Errorf("got unknown event")

	default:
		// TODO more decoders for more events
		err = errors.New("not support event: " + event.Header.Type())
	}

	if err != nil {
		return nil, err
	}

	// set event body
	event.Body = eventBody

	return event, nil
}

// WalkEvent will walk all events for binary log which in io.Reader
// This function will return isFinish bool and err error.
func (decoder *BinFileDecoder) WalkEvent(f func(event *BinEvent) (isContinue bool, err error)) error {
	for {
		// if rd is nil, BinFileDecoder.DecodeEvent() will set rd to BinFileDecoder.BinFile
		event, err := decoder.DecodeEvent()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// will receive a nil event if decoding not start yet
		if event == nil {
			continue
		}

		// if stop decoding
		if decoder.Option.Stop(event.Header) {
			return nil
		}

		isContinue, err := f(event)
		if !isContinue || err != nil {
			return err
		}
	}
}
