package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// BinEventBody describe event body
type BinEventBody interface {
	isEventBody()
}

// BinEvent binary log event definition
type BinEvent struct {
	Header       *BinEventHeader
	Body         BinEventBody
	ChecksumType byte
	ChecksumVal  []byte
}

func (event *BinEvent) GetType() (string, bool) {
	eventType, ok := EventType2Str[event.Header.EventType]
	return eventType, ok
}

// Validation event validity check
func (event *BinEvent) Validation(bin *BinaryLogInfo, header, body []byte) ([]byte, error) {
	if bin == nil {
		return body, fmt.Errorf("empty BinaryLogInfo")
	}

	if event.Header == nil {
		return body, fmt.Errorf("empty event header")
	}

	if l := int64(len(body) + len(header)); l != event.Header.EventSize {
		return body, fmt.Errorf("event size got %d need %d", l, event.Header.EventSize)
	}

	if bin.description != nil && bin.description.hasCheckSum {
		index := len(body) - binlogChecksumLength - 1
		event.ChecksumType = body[index]
		event.ChecksumVal = body[index+1:]
		body = body[:index+1]

		if !ChecksumValidate(event.ChecksumType, event.ChecksumVal, append(header, body...)) || len(event.ChecksumVal) != 4 {
			return body, fmt.Errorf("binlog checksum validation failed")
		}
	}

	return body, nil
}

// BaseEventBody is base off all events
type BaseEventBody struct{}

func (event *BaseEventBody) isEventBody() {}

// BinEventUnParsed save the event data when the event type not supported yet.
type BinEventUnParsed struct {
	BaseEventBody
	Data []byte
}

func decodeUnSupportEvent(data []byte) (*BinEventUnParsed, error) {
	return &BinEventUnParsed{
		Data: data,
	}, nil
}

// mysql binlog version > 1 (version > mysql 4.0.0), size = 19
var defaultEventHeaderSize int64 = 19

// BinEventHeader binary log header definition
// https://dev.mysql.com/doc/internals/en/binlog-event-header.html
type BinEventHeader struct {
	Timestamp int64
	EventType uint8
	ServerID  int64
	EventSize int64
	LogPos    int64
	Flag      uint16
}

// Type function will translate event type into string
func (header *BinEventHeader) Type() string {
	return EventType2Str[header.EventType]
}

// String interface implement
func (header *BinEventHeader) String() string {
	return fmt.Sprintf("Type:%s, Time:%s, ServerID:%d, EventSize:%d, EventEndPos:%d, Flag:0x%x",
		header.Type(),
		time.Unix(header.Timestamp, 0),
		header.Timestamp,
		header.EventSize,
		header.LogPos,
		header.Flag,
	)
}

func decodeEventHeader(data []byte, size int64) (*BinEventHeader, error) {
	if l := len(data); int64(l) < size {
		return nil, fmt.Errorf("invalid event header size %d, should be %d", l, size)
	}

	var pos int
	eventHeader := &BinEventHeader{}

	// timestamp
	// 注意这里的Uint32,只会截取前4个
	eventHeader.Timestamp = int64(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// event_type
	eventHeader.EventType = data[pos]
	pos++

	// serverId
	eventHeader.ServerID = int64(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// event_size
	eventHeader.EventSize = int64(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// version > 2
	if size > 13 {
		// log_pos
		eventHeader.LogPos = int64(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		// flags
		eventHeader.Flag = binary.LittleEndian.Uint16(data[pos:])
	}

	return eventHeader, nil
}

// BinFmtDescEvent is the definition of FORMAT_DESCRIPTION_EVENT
// https://dev.mysql.com/doc/internals/en/format-description-event.html
type BinFmtDescEvent struct {
	BaseEventBody
	BinlogVersion     int
	MySQLVersion      string
	CreateTime        int64
	EventHeaderLength int64
	EventTypeHeader   []byte

	// cache the result of hasCheckSum()
	hasCheckSum bool
}

func decodeFmtDescEvent(data []byte) (*BinFmtDescEvent, error) {
	var pos int
	desc := &BinFmtDescEvent{}

	// binlog-version
	desc.BinlogVersion = int(binary.LittleEndian.Uint16(data))
	pos += 2

	// mysql-server version
	desc.MySQLVersion = string(bytes.Trim(data[pos:pos+50], strconv.Itoa(0x00)))
	desc.hasCheckSum = hasChecksum(desc.MySQLVersion)
	pos += 50

	// create timestamp
	desc.CreateTime = int64(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// event header length
	desc.EventHeaderLength = int64(data[pos])
	pos++

	// event type header lengths
	desc.EventTypeHeader = data[pos:]

	return desc, nil
}

// BinQueryEvent is the definition of QUERY_EVENT
// https://dev.mysql.com/doc/internals/en/query-event.html
type BinQueryEvent struct {
	BaseEventBody
	SlaveProxyID     int64
	ExecutionTime    int64
	ErrorCode        uint16
	statusVarsLength int
	StatusVars       []byte
	Schema           string
	Query            string
}

func decodeQueryEvent(data []byte, binlogVersion int) (*BinQueryEvent, error) {
	var pos int
	event := &BinQueryEvent{}

	// slave_proxy_id
	event.SlaveProxyID = int64(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// execution time
	event.ExecutionTime = int64(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// schema length
	schemaLength := int(data[pos])
	pos++

	// error-code
	event.ErrorCode = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if binlogVersion >= 4 {
		// status-vars length
		event.statusVarsLength = int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		// status-vars
		event.StatusVars = data[pos : pos+event.statusVarsLength]
		pos += event.statusVarsLength
	}

	// schema
	event.Schema = string(data[pos : pos+schemaLength])
	pos += schemaLength

	// ignore 0x00
	pos++

	// query
	event.Query = string(data[pos:])
	return event, nil
}

// Statue will format status_vars of QUERY_EVENT
// TODO decode QUERY_EVENT status_var
func (event *BinQueryEvent) Statue() error {
	fmt.Println(event.statusVarsLength)
	for i := 0; i < event.statusVarsLength; {
		// got status_vars key
		k := event.StatusVars[i]
		i++

		// decode values
		switch k {
		case QFlags2Code:
			v := event.StatusVars[i : i+4]
			i += 4

			// TODO
			fmt.Println("QFlags2Code", v)
		case QSQLModeCode:
			v := event.StatusVars[i : i+8]
			i += 8

			// TODO
			fmt.Println("QSQLModeCode", v)
		case QCatalog:
			n := int(event.StatusVars[i])
			v := string(event.StatusVars[i+1 : i+1+n])
			i += 1 + n + 1

			// TODO
			fmt.Println("QCatalog", v)
		case QAutoIncrement:
			increment := binary.LittleEndian.Uint32(event.StatusVars[i:])
			offset := binary.LittleEndian.Uint32(event.StatusVars[i+2:])

			// TODO
			fmt.Printf("QAutoIncrement %d, %d\n", increment, offset)
		case QCharsetCode:
			clientCharSet := event.StatusVars[i : i+2]
			i += 2
			collationConnection := event.StatusVars[i : i+2]
			i += 2
			collationServer := event.StatusVars[i : i+2]
			i += 2

			// TODO
			fmt.Println("QCharsetCode", clientCharSet, collationConnection, collationServer)
		case QTimeZoneCode:
			n := int(event.StatusVars[i])
			v := string(event.StatusVars[i+1 : i+1+n])
			i += 1 + n

			// TODO
			fmt.Println("QTimeZoneCode", v)
		case QCatalogNZCode:
			n := int(event.StatusVars[i])
			v := string(event.StatusVars[i+1 : i+1+n])
			i += 1 + n

			// TODO
			fmt.Printf("QCatalogNZCode %s\n", v)
		case QLCTimeNamesCode:
			// TODO
			fmt.Println("QLCTimeNamesCode")
		case QCharsetDatabaseCode:
			// TODO
			fmt.Println("QCharsetDatabaseCode")
		case QTableMapForUpdateCode:
			// TODO
			fmt.Println("QTableMapForUpdateCode")
		case QMasterDataWrittenCode:
			// TODO
			fmt.Println("QMasterDataWrittenCode")
		case QInvokers:
			// TODO
			fmt.Println("QInvokers")
		case QUpdatedDBNames:
			// TODO
			fmt.Println("QUpdatedDBNames")
		case QMicroseconds:
			microseconds := binary.LittleEndian.Uint32(event.StatusVars[i:])
			i += 3

			// TODO
			fmt.Printf("QMicroseconds %d\n", microseconds)
		default:
			return fmt.Errorf("unknown status var %x", k)
		}
	}

	return nil
}

// BinXIDEvent is the definition of XID_EVENT
// https://dev.mysql.com/doc/internals/en/xid-event.html
// Transaction ID for 2PC, written whenever a COMMIT is expected.
type BinXIDEvent struct {
	BaseEventBody
	XID uint64
}

func decodeXIDEvent(data []byte) (*BinXIDEvent, error) {
	e := &BinXIDEvent{XID: binary.LittleEndian.Uint64(data)}
	return e, nil
}

// BinIntvarEvent is the definition of INTVAR_EVENT
// https://dev.mysql.com/doc/internals/en/xid-event.html
// Transaction ID for 2PC, written whenever a COMMIT is expected.
type BinIntvarEvent struct {
	BaseEventBody
	Type  uint8
	Value uint64
}

func decodeIntvarEvent(data []byte) (*BinIntvarEvent, error) {
	return &BinIntvarEvent{
		Type:  data[0],
		Value: binary.LittleEndian.Uint64(data[1:]),
	}, nil
}

// TODO: BinIntvarEvent.Type format

// BinRotateEvent is the definition of ROTATE_EVENT
// https://dev.mysql.com/doc/internals/en/rotate-event.html
// The rotate event is added to the binlog as last event to tell the reader what binlog to request next.
type BinRotateEvent struct {
	BaseEventBody
	Position uint64
	FileName string
}

func decodeRotateEvent(data []byte, binlogVersion int) (*BinRotateEvent, error) {
	event := &BinRotateEvent{}
	var pos int
	if binlogVersion > 1 {
		// next_binlog_po
		event.Position = binary.LittleEndian.Uint64(data)
		pos += 8
	}

	// next_binlog_filename
	event.FileName = strings.TrimSpace(string(data[pos:]))
	return event, nil
}

// BinPreGTIDsEvent is the definition of PREVIOUS_GTIDS_EVENT
// TODO: PREVIOUS_GTIDS_EVENT
type BinPreGTIDsEvent struct{ BaseEventBody }
