package redis

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var errRESPProtocol = errors.New("redis resp: protocol error")

type respArg []byte

type respValue struct {
	kind   respKind
	text   string
	bytes  []byte
	number int64
	items  []respValue
	fields []respMapField
}

type respMapField struct {
	key   respValue
	value respValue
}

type respKind uint8

const (
	respSimpleString respKind = iota + 1
	respError
	respInteger
	respBulkString
	respNullBulkString
	respArray
	respMap
)

func simpleString(text string) respValue {
	return respValue{kind: respSimpleString, text: text}
}

func errorString(text string) respValue {
	return respValue{kind: respError, text: text}
}

func integerValue(number int64) respValue {
	return respValue{kind: respInteger, number: number}
}

func bulkString(value []byte) respValue {
	return respValue{kind: respBulkString, bytes: append([]byte(nil), value...)}
}

func bulkText(text string) respValue {
	return respValue{kind: respBulkString, bytes: []byte(text)}
}

func nullBulkString() respValue {
	return respValue{kind: respNullBulkString}
}

func arrayValue(items ...respValue) respValue {
	return respValue{kind: respArray, items: items}
}

func mapValue(fields ...respMapField) respValue {
	return respValue{kind: respMap, fields: fields}
}

func mapField(key, value string) respMapField {
	return respMapField{key: bulkText(key), value: bulkText(value)}
}

func readRESPCommand(reader *bufio.Reader) ([]respArg, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read redis command prefix: %w", err)
	}

	switch prefix {
	case '*':
		return readRESPArrayCommand(reader)
	case '\r', '\n':
		return nil, errRESPProtocol
	default:
		line, err := readLineSuffix(reader)
		if err != nil {
			return nil, err
		}
		return inlineCommand(append([]byte{prefix}, line...)), nil
	}
}

func readRESPArrayCommand(reader *bufio.Reader) ([]respArg, error) {
	count, err := readRESPInt(reader)
	if err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, errRESPProtocol
	}

	args := make([]respArg, 0, count)
	for range count {
		arg, readErr := readRESPArg(reader)
		if readErr != nil {
			return nil, readErr
		}
		args = append(args, arg)
	}
	return args, nil
}

func readRESPArg(reader *bufio.Reader) (respArg, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("read redis arg prefix: %w", err)
	}

	switch prefix {
	case '$':
		return readRESPBulkArg(reader)
	case '+', ':':
		line, readErr := readLineSuffix(reader)
		if readErr != nil {
			return nil, readErr
		}
		return respArg(line), nil
	default:
		return nil, errRESPProtocol
	}
}

func readRESPBulkArg(reader *bufio.Reader) (respArg, error) {
	size, err := readRESPInt(reader)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, nil
	}
	raw := make([]byte, size+2)
	if _, err := io.ReadFull(reader, raw); err != nil {
		return nil, fmt.Errorf("read redis bulk argument: %w", err)
	}
	if raw[size] != '\r' || raw[size+1] != '\n' {
		return nil, errRESPProtocol
	}
	return respArg(raw[:size]), nil
}

func readRESPInt(reader *bufio.Reader) (int, error) {
	line, err := readLineSuffix(reader)
	if err != nil {
		return 0, err
	}
	value, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, fmt.Errorf("%w: invalid integer", errRESPProtocol)
	}
	return value, nil
}

func readLineSuffix(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read redis line: %w", err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errRESPProtocol
	}
	return line[:len(line)-2], nil
}

func inlineCommand(line []byte) []respArg {
	parts := strings.Fields(string(line))
	args := make([]respArg, 0, len(parts))
	for index := range parts {
		args = append(args, respArg(parts[index]))
	}
	return args
}

func writeRESP(writer *bufio.Writer, value respValue) error {
	if err := encodeRESP(writer, value); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush redis response: %w", err)
	}
	return nil
}

func encodeRESP(writer *bufio.Writer, value respValue) error {
	switch value.kind {
	case respSimpleString:
		return writeRESPTextLine(writer, '+', value.text)
	case respError:
		return writeRESPTextLine(writer, '-', value.text)
	case respInteger:
		return writeRESPTextLine(writer, ':', strconv.FormatInt(value.number, 10))
	case respBulkString:
		return encodeRESPBulk(writer, value.bytes)
	case respNullBulkString:
		return writeRESPRawString(writer, "$-1\r\n")
	case respArray:
		return encodeRESPArray(writer, value.items)
	case respMap:
		return encodeRESPMap(writer, value.fields)
	default:
		return fmt.Errorf("%w: unknown response kind", errRESPProtocol)
	}
}

func encodeRESPBulk(writer *bufio.Writer, value []byte) error {
	if err := writeRESPTextLine(writer, '$', strconv.Itoa(len(value))); err != nil {
		return err
	}
	if _, err := writer.Write(value); err != nil {
		return fmt.Errorf("write redis bulk response: %w", err)
	}
	return writeRESPRawString(writer, "\r\n")
}

func encodeRESPArray(writer *bufio.Writer, items []respValue) error {
	if err := writeRESPTextLine(writer, '*', strconv.Itoa(len(items))); err != nil {
		return err
	}
	for index := range items {
		if err := encodeRESP(writer, items[index]); err != nil {
			return err
		}
	}
	return nil
}

func encodeRESPMap(writer *bufio.Writer, fields []respMapField) error {
	if err := writeRESPTextLine(writer, '%', strconv.Itoa(len(fields))); err != nil {
		return err
	}
	for index := range fields {
		if err := encodeRESP(writer, fields[index].key); err != nil {
			return err
		}
		if err := encodeRESP(writer, fields[index].value); err != nil {
			return err
		}
	}
	return nil
}

func writeRESPTextLine(writer *bufio.Writer, prefix byte, text string) error {
	if err := writer.WriteByte(prefix); err != nil {
		return fmt.Errorf("write redis line prefix: %w", err)
	}
	if _, err := writer.WriteString(text); err != nil {
		return fmt.Errorf("write redis line text: %w", err)
	}
	return writeRESPRawString(writer, "\r\n")
}

func writeRESPRawString(writer *bufio.Writer, value string) error {
	if _, err := writer.WriteString(value); err != nil {
		return fmt.Errorf("write redis response: %w", err)
	}
	return nil
}
