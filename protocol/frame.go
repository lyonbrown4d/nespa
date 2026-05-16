// Package protocol implements the Nespa TCP frame codec.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	Magic           uint32 = 0x4e535041 // "NSPA"
	Version         uint8  = 1
	FixedHeaderSize        = 32

	DefaultMaxMetadataBytes uint32 = 1 << 20  // 1 MiB
	DefaultMaxPayloadBytes  uint32 = 64 << 20 // 64 MiB
)

var (
	ErrInvalidMagic       = errors.New("protocol: invalid frame magic")
	ErrUnsupportedVersion = errors.New("protocol: unsupported frame version")
	ErrFrameTooLarge      = errors.New("protocol: frame too large")
	ErrInvalidFrame       = errors.New("protocol: invalid frame")
)

type ErrorCode uint16

const (
	ErrorUnknown ErrorCode = iota + 1
	ErrorBadFrame
	ErrorUnsupportedVersion
	ErrorTooLarge
	ErrorNoRoute
	ErrorTimeout
	ErrorUnavailable
	ErrorInternal
	ErrorInvalidArgument
)

type Op uint16

const (
	OpCacheGet Op = iota + 1
	OpCacheSet
	OpCacheDelete
	OpCacheBatchGet
	OpCacheBatchSet
	OpNodeHeartbeat
	OpControlSnapshot
	OpControlWatch
	OpCacheExists
	OpCacheTouch
	OpCacheAdjust
)

type Flags uint8

const (
	FlagResponse Flags = 1 << iota
	FlagError
	FlagMore
)

type Frame struct {
	Flags      Flags
	Op         Op
	RequestID  uint64
	RouteEpoch uint64
	Metadata   []byte
	Payload    []byte
}

type Codec struct {
	MaxMetadataBytes uint32
	MaxPayloadBytes  uint32
}

type CodecOption func(*Codec)

func WithMaxMetadataBytes(limit uint32) CodecOption {
	return func(c *Codec) {
		c.MaxMetadataBytes = limit
	}
}

func WithMaxPayloadBytes(limit uint32) CodecOption {
	return func(c *Codec) {
		c.MaxPayloadBytes = limit
	}
}

func NewCodec(opts ...CodecOption) *Codec {
	codec := &Codec{
		MaxMetadataBytes: DefaultMaxMetadataBytes,
		MaxPayloadBytes:  DefaultMaxPayloadBytes,
	}
	for _, opt := range opts {
		opt(codec)
	}
	return codec
}

func Encode(w io.Writer, frame Frame) error {
	return NewCodec().Encode(w, frame)
}

func Decode(r io.Reader) (Frame, error) {
	return NewCodec().Decode(r)
}

func (c *Codec) Encode(w io.Writer, frame Frame) error {
	if frame.Op == 0 {
		return fmt.Errorf("%w: op is required", ErrInvalidFrame)
	}
	metaLen, payloadLen, err := checkedLengths(len(frame.Metadata), len(frame.Payload))
	if err != nil {
		return err
	}
	if err := c.validateLengths(metaLen, payloadLen); err != nil {
		return err
	}

	var header [FixedHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], Magic)
	header[4] = Version
	header[5] = byte(frame.Flags)
	binary.BigEndian.PutUint16(header[6:8], uint16(frame.Op))
	binary.BigEndian.PutUint64(header[8:16], frame.RequestID)
	binary.BigEndian.PutUint64(header[16:24], frame.RouteEpoch)
	binary.BigEndian.PutUint32(header[24:28], metaLen)
	binary.BigEndian.PutUint32(header[28:32], payloadLen)

	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	if len(frame.Metadata) > 0 {
		if err := writeFull(w, frame.Metadata); err != nil {
			return err
		}
	}
	if len(frame.Payload) > 0 {
		if err := writeFull(w, frame.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *Codec) Decode(r io.Reader) (Frame, error) {
	header, err := readHeader(r)
	if err != nil {
		return Frame{}, err
	}

	if magic := binary.BigEndian.Uint32(header[0:4]); magic != Magic {
		return Frame{}, ErrInvalidMagic
	}
	if version := header[4]; version != Version {
		return Frame{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	metaLen := binary.BigEndian.Uint32(header[24:28])
	payloadLen := binary.BigEndian.Uint32(header[28:32])
	if err := c.validateLengths(metaLen, payloadLen); err != nil {
		return Frame{}, err
	}

	frame := Frame{
		Flags:      Flags(header[5]),
		Op:         Op(binary.BigEndian.Uint16(header[6:8])),
		RequestID:  binary.BigEndian.Uint64(header[8:16]),
		RouteEpoch: binary.BigEndian.Uint64(header[16:24]),
	}
	if frame.Op == 0 {
		return Frame{}, fmt.Errorf("%w: op is required", ErrInvalidFrame)
	}
	if err := readFrameBody(r, &frame, metaLen, payloadLen); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func readHeader(r io.Reader) ([FixedHeaderSize]byte, error) {
	var header [FixedHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return header, fmt.Errorf("read frame header: %w", err)
	}
	return header, nil
}

func readFrameBody(r io.Reader, frame *Frame, metaLen, payloadLen uint32) error {
	if metaLen > 0 {
		frame.Metadata = make([]byte, metaLen)
		if _, err := io.ReadFull(r, frame.Metadata); err != nil {
			return fmt.Errorf("read frame metadata: %w", err)
		}
	}
	if payloadLen > 0 {
		frame.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, frame.Payload); err != nil {
			return fmt.Errorf("read frame payload: %w", err)
		}
	}
	return nil
}

func (c *Codec) validateLengths(metaLen, payloadLen uint32) error {
	maxMetadataBytes, maxPayloadBytes := c.limits()
	if metaLen > maxMetadataBytes {
		return fmt.Errorf("%w: metadata %d > %d", ErrFrameTooLarge, metaLen, maxMetadataBytes)
	}
	if payloadLen > maxPayloadBytes {
		return fmt.Errorf("%w: payload %d > %d", ErrFrameTooLarge, payloadLen, maxPayloadBytes)
	}
	return nil
}

func (c *Codec) limits() (uint32, uint32) {
	maxMetadataBytes := c.MaxMetadataBytes
	if maxMetadataBytes == 0 {
		maxMetadataBytes = DefaultMaxMetadataBytes
	}
	maxPayloadBytes := c.MaxPayloadBytes
	if maxPayloadBytes == 0 {
		maxPayloadBytes = DefaultMaxPayloadBytes
	}
	return maxMetadataBytes, maxPayloadBytes
}

func checkedLengths(metaLen, payloadLen int) (uint32, uint32, error) {
	const maxUint32 = uint64(^uint32(0))
	if metaLen < 0 || uint64(metaLen) > maxUint32 {
		return 0, 0, fmt.Errorf("%w: metadata %d > %d", ErrFrameTooLarge, metaLen, maxUint32)
	}
	if payloadLen < 0 || uint64(payloadLen) > maxUint32 {
		return 0, 0, fmt.Errorf("%w: payload %d > %d", ErrFrameTooLarge, payloadLen, maxUint32)
	}
	return uint32(metaLen), uint32(payloadLen), nil
}

func writeFull(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err != nil {
		return fmt.Errorf("write frame bytes: %w", err)
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}
