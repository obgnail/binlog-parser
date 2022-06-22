package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
)

func bitmapByteSize(columnCount int) int {
	return (columnCount + 7) / 8
}

// BinTableMapEvent is the definition of TABLE_MAP_EVENT
// https://dev.mysql.com/doc/internals/en/table-map-event.html
type BinTableMapEvent struct {
	BaseEventBody
	TableID       uint64
	tableIDLen    int
	Flags         uint16
	Schema        string       // database_name
	Table         string       // table_name
	ColumnCount   uint64       // 对应表中的字段数量
	ColumnTypeDef []FieldType  // 字段类型
	ColumnMetaDef []ColumnType // 每个字段的元数据信息，比如 varchar 字段需要记录最长长度
	NullBitmap    Bitfield     // 一个 bit 表示一个字段是否可以为 NULL，顺序是：第一个字节的最低位开始向最高位增长，之后第二个字节的最低位开始向最高位增长，以此类推
}

type Bitfield []byte

func (bits Bitfield) isSet(index uint) bool {
	return bits[index/8]&(1<<(index%8)) != 0
}

type FieldType byte

type ColumnType struct {
	columnType FieldType
	name       string
	unsigned   bool
	maxLength  uint16
	lengthSize uint8
	precision  int
	decimals   int
	size       uint16
	bytes      int
	bits       byte
	fsp        uint8
}

// Init BinTableMapEvent tableIDLen
func (e *BinTableMapEvent) Init(h *BinFmtDescEvent) *BinTableMapEvent {
	if int(h.EventTypeHeader[TableMapEvent-1]) == 6 {
		e.tableIDLen = 4
	} else {
		e.tableIDLen = 6
	}
	return e
}

func decodeTableMapEvent(data []byte, h *BinFmtDescEvent) (*BinTableMapEvent, error) {
	event := &BinTableMapEvent{}

	// set table id
	event = event.Init(h)
	pos := event.tableIDLen
	event.TableID = FixedLengthInt(data[:pos])

	// set flags
	event.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	// set schema && skip 0x00
	schemaLength := int(data[pos])
	pos++
	event.Schema = string(data[pos : pos+schemaLength])
	pos += schemaLength + 1

	// set table && skip 0x00
	tableLength := int(data[pos])
	pos++
	event.Table = string(data[pos : pos+tableLength])
	pos += tableLength + 1

	// set column count
	var n int
	event.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	// column_type_def (string.var_len)
	// array of column definitions, one byte per field type
	event.ColumnTypeDef = make([]FieldType, event.ColumnCount)
	for i := 0; i < int(event.ColumnCount); i++ {
		event.ColumnTypeDef[i] = FieldType(data[pos+i])
	}
	pos += int(event.ColumnCount)

	// decode column meta
	var err error
	var metaData []byte
	if metaData, _, n, err = LengthEncodedString(data[pos:]); err != nil {
		return nil, err
	}

	if err := event.decodeMeta(metaData); err != nil {
		return nil, err
	}

	pos += n

	// null_bitmap (string.var_len) [len=(column_count + 7) / 8]
	if len(data[pos:]) == bitmapByteSize(int(event.ColumnCount)) {
		event.NullBitmap = data[pos:]
		return event, nil
	}

	return event, io.EOF
}

func (e *BinTableMapEvent) decodeMeta(data []byte) error {
	pos := 0
	e.ColumnMetaDef = make([]ColumnType, e.ColumnCount)
	for i, t := range e.ColumnTypeDef {
		switch t {
		case MySQLTypeString:
			var fieldType, fieldLength uint8
			fieldType = data[pos]
			pos += 1
			fieldLength = data[pos]
			pos += 1
			metadata := (uint16(fieldType) << 8) + uint16(fieldLength)
			if FieldType(fieldType) == MySQLTypeEnum || FieldType(fieldType) == MySQLTypeSet {
				e.ColumnMetaDef[i].columnType = FieldType(fieldType)
				e.ColumnMetaDef[i].size = metadata & 0x00ff
			} else {
				e.ColumnMetaDef[i].maxLength = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff)
			}
		case MySQLTypeVarString, MySQLTypeVarchar, MySQLTypeDecimal:
			e.ColumnMetaDef[i].maxLength = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case MySQLTypeBit:
			bits := data[pos]
			pos += 1
			bytes := data[pos]
			pos += 1
			e.ColumnMetaDef[i].bits = (bytes * 8) + bits
			e.ColumnMetaDef[i].bytes = int((e.ColumnMetaDef[i].bits + 7) / 8)
		case MySQLTypeBlob, MySQLTypeGeometry, MySQLTypeDouble, MySQLTypeFloat,
			MySQLTypeMediumBlob, MySQLTypeTinyBlob, MySQLTypeLongBlob, MySQLTypeJSON:
			e.ColumnMetaDef[i].lengthSize = data[pos]
			pos++
		case MySQLTypeNewDecimal:
			e.ColumnMetaDef[i].precision = int(data[pos])
			pos += 1
			e.ColumnMetaDef[i].decimals = int(data[pos])
			pos += 1
		case MySQLTypeTime2, MySQLTypeDatetime2, MySQLTypeTimestamp2:
			e.ColumnMetaDef[i].fsp = data[pos]
			pos += 1
		case MySQLTypeDate, MySQLTypeDatetime, MySQLTypeTimestamp, MySQLTypeTime,
			MySQLTypeTiny, MySQLTypeShort, MySQLTypeInt24, MySQLTypeLong,
			MySQLTypeLonglong, MySQLTypeNull, MySQLTypeYear, MySQLTypeNewDate:
			e.ColumnMetaDef[i].maxLength = 0
		default:
			return fmt.Errorf("unknown FieldType %s", fmt.Sprint(t))
		}
	}
	return nil
}

// BinRowsEvent describe MySQL ROWS_EVENT
// https://dev.mysql.com/doc/internals/en/rows-event.html
type BinRowsEvent struct {
	BaseEventBody
	// header
	Version    int
	TableID    uint64
	tableIDLen int
	Flags      uint16
	ExtraData  []byte // if version == 2

	// body
	ColumnCount    uint64
	ColumnsBitmap1 Bitfield
	ColumnsBitmap2 Bitfield // if UPDATE_ROWS_EVENTv1 or v2

	rows []map[string]interface{}

	tableMap *BinTableMapEvent // 该event所属的tableMap
}

// Init BinRowsEvent, adding version and table_id length
func (e *BinRowsEvent) Init(h *BinFmtDescEvent, eventType uint8) *BinRowsEvent {
	if int(h.EventTypeHeader[eventType-1]) == 6 {
		e.tableIDLen = 4
	} else {
		e.tableIDLen = 6
	}

	switch eventType {
	case WriteRowsEventV0, UpdateRowsEventV0, DeleteRowsEventV0:
		e.Version = 0
	case WriteRowsEventV1, UpdateRowsEventV1, DeleteRowsEventV1:
		e.Version = 1
	case WriteRowsEventV2, UpdateRowsEventV2, DeleteRowsEventV2:
		e.Version = 2
	}

	return e
}

func decodeRowsEvent(data []byte, h *BinFmtDescEvent, typ uint8) (*BinRowsEvent, error) {
	event := &BinRowsEvent{}
	event = event.Init(h, typ)

	// set table id
	pos := event.tableIDLen
	event.TableID = FixedLengthInt(data[:pos])

	// set flags
	event.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	// set extraDataLength
	if event.Version == 2 {
		extraDataLen := binary.LittleEndian.Uint16(data[pos:])
		pos += 2

		event.ExtraData = data[pos : pos+int(extraDataLen-2)]
		pos += int(extraDataLen - 2)
	}

	// body
	var n int
	event.ColumnCount, _, n = LengthEncodedInt(data[pos:])
	pos += n

	// columns-present-bitmap1
	bitCount := bitmapByteSize(int(event.ColumnCount))
	event.ColumnsBitmap1 = data[pos : pos+bitCount]
	pos += bitCount

	// columns-present-bitmap2
	if typ == UpdateRowsEventV1 || typ == UpdateRowsEventV2 {
		event.ColumnsBitmap2 = data[pos : pos+bitCount]
		pos += bitCount
	}

	// TODO Unfinished

	return event, nil
}
