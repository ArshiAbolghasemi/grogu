// Package zapconsole provides utilities for working with zap logging library.
package zapconsole

import (
	"encoding/base64"
	"math"
	"time"
	"unicode/utf8"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	_hex            = "0123456789abcdef"
	_sizeFloat32    = 32
	_sizeFloat64    = 64
	_sizeSliceArray = 2
)

var _sliceEncoderPool = NewPool(func() *sliceArrayEncoder {
	return &sliceArrayEncoder{
		elems: make([]any, 0, _sizeSliceArray),
	}
})

func getSliceEncoder() *sliceArrayEncoder {
	return _sliceEncoderPool.Get()
}

// safeAppendStringLike is a generic implementation of safeAddString and safeAddByteString.
// It appends a string or byte slice to the buffer, escaping all special characters.
// It works by skipping over characters
// that can be safely copied as-is,
// until a character is found that needs special handling.
func safeAppendStringLike[S []byte | string](
	appendTo func(*buffer.Buffer, S),
	decodeRune func(S) (rune, int),
	buf *buffer.Buffer,
	inputVar S,
) {
	last := 0

	for indx := 0; indx < len(inputVar); {
		if inputVar[indx] >= utf8.RuneSelf {
			// Character >= RuneSelf may be part of a multi-byte rune.
			r, size := decodeRune(inputVar[indx:])
			if r != utf8.RuneError || size != 1 {
				// No special handling required.
				indx += size
				continue
			}

			// Invalid UTF-8 sequence. Replace it with the Unicode replacement character.
			appendTo(buf, inputVar[last:indx])
			buf.AppendString(`\ufffd`)

			indx++
			last = indx
		} else {
			// Character < RuneSelf is a single-byte UTF-8 rune.
			if inputVar[indx] >= 0x20 && inputVar[indx] != '\\' && inputVar[indx] != '"' {
				// No escaping necessary.
				indx++
				continue
			}

			appendTo(buf, inputVar[last:indx])

			switch inputVar[indx] {
			case '\\', '"':
				buf.AppendByte('\\')
				buf.AppendByte(inputVar[indx])
			case '\n':
				buf.AppendByte('\\')
				buf.AppendByte('n')
			case '\r':
				buf.AppendByte('\\')
				buf.AppendByte('r')
			case '\t':
				buf.AppendByte('\\')
				buf.AppendByte('t')
			default:
				// Encode bytes < 0x20, except for the escape sequences above.
				buf.AppendString(`\u00`)
				buf.AppendByte(_hex[inputVar[indx]>>4])
				buf.AppendByte(_hex[inputVar[indx]&0xF])
			}

			indx++
			last = indx
		}
	}

	// add remaining
	appendTo(buf, inputVar[last:])
}

func putSliceEncoder(e *sliceArrayEncoder) {
	e.elems = e.elems[:0]
	_sliceEncoderPool.Put(e)
}

type ConsoleEncoder struct {
	zapcore.Encoder
	*zapcore.EncoderConfig

	buf            *buffer.Buffer
	openNamespaces int
}

func NewConsoleEncoder(encConfig *zapcore.EncoderConfig) *ConsoleEncoder {
	return &ConsoleEncoder{
		EncoderConfig:  encConfig,
		buf:            buffer.NewPool().Get(),
		openNamespaces: 0,
	}
}

func (enc *ConsoleEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	enc.addKey(key)
	return enc.AppendArray(arr)
}

func (enc *ConsoleEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	enc.addKey(key)
	return enc.AppendObject(obj)
}

func (enc *ConsoleEncoder) AddBinary(key string, val []byte) {
	enc.AddString(key, base64.StdEncoding.EncodeToString(val))
}

func (enc *ConsoleEncoder) AddByteString(key string, val []byte) {
	enc.addKey(key)
	enc.AppendByteString(val)
}

func (enc *ConsoleEncoder) AddBool(key string, val bool) {
	enc.addKey(key)
	enc.AppendBool(val)
}

func (enc *ConsoleEncoder) AddComplex128(key string, val complex128) {
	enc.addKey(key)
	enc.AppendComplex128(val)
}

func (enc *ConsoleEncoder) AddComplex64(key string, val complex64) {
	enc.addKey(key)
	enc.AppendComplex64(val)
}

func (enc *ConsoleEncoder) AddDuration(key string, val time.Duration) {
	enc.addKey(key)
	enc.AppendDuration(val)
}

func (enc *ConsoleEncoder) AddFloat64(key string, val float64) {
	enc.addKey(key)
	enc.AppendFloat64(val)
}

func (enc *ConsoleEncoder) AddFloat32(key string, val float32) {
	enc.addKey(key)
	enc.AppendFloat32(val)
}

func (enc *ConsoleEncoder) AddInt64(key string, val int64) {
	enc.addKey(key)
	enc.AppendInt64(val)
}

func (enc *ConsoleEncoder) AddReflected(key string, obj any) error { return nil }

func (enc *ConsoleEncoder) OpenNamespace(key string) {
	enc.buf.AppendByte('{')

	enc.openNamespaces++
}

func (enc *ConsoleEncoder) AddString(key, val string) {
	enc.addKey(key)
	enc.AppendString(val)
}

func (enc *ConsoleEncoder) AddTime(key string, val time.Time) {
	enc.addKey(key)
	enc.AppendTime(val)
}

func (enc *ConsoleEncoder) AddUint64(key string, val uint64) {
	enc.addKey(key)
	enc.AppendUint64(val)
}

func (enc *ConsoleEncoder) AppendArray(arr zapcore.ArrayMarshaler) error {
	enc.addElementSeparator()
	enc.buf.AppendByte('[')
	err := arr.MarshalLogArray(enc)
	enc.buf.AppendByte(']')

	return err
}

func (enc *ConsoleEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	old := enc.openNamespaces
	enc.openNamespaces = 0
	enc.addElementSeparator()
	enc.buf.AppendByte('{')
	err := obj.MarshalLogObject(enc)
	enc.buf.AppendByte('}')
	enc.closeOpenNamespaces()
	enc.openNamespaces = old

	return err
}

func (enc *ConsoleEncoder) AppendBool(val bool) {
	enc.addElementSeparator()
	enc.buf.AppendBool(val)
}

func (enc *ConsoleEncoder) AppendByteString(val []byte) {
	enc.addElementSeparator()
	enc.buf.AppendByte('"')
	enc.safeAddByteString(val)
	enc.buf.AppendByte('"')
}

func (enc *ConsoleEncoder) AppendDuration(val time.Duration) {
	cur := enc.buf.Len()

	if e := enc.EncodeDuration; e != nil {
		e(val, enc)
	}

	if cur == enc.buf.Len() {
		// User-supplied EncodeDuration is a no-op. Fall back to nanoseconds to keep
		// JSON valid.
		enc.AppendInt64(int64(val))
	}
}

func (enc *ConsoleEncoder) AppendInt64(val int64) {
	enc.addElementSeparator()
	enc.buf.AppendInt(val)
}

func (enc *ConsoleEncoder) AppendReflected(val interface{}) error {
	valueBytes, err := enc.encodeReflected(val)
	if err != nil {
		return err
	}

	enc.addElementSeparator()
	_, err = enc.buf.Write(valueBytes)

	return err
}

func (enc *ConsoleEncoder) AppendString(val string) {
	enc.addElementSeparator()
	enc.buf.AppendByte('"')
	enc.safeAddString(val)
	enc.buf.AppendByte('"')
}

func (enc *ConsoleEncoder) AppendTimeLayout(timeVar time.Time, layout string) {
	enc.addElementSeparator()
	enc.buf.AppendByte('"')
	enc.buf.AppendTime(timeVar, layout)
	enc.buf.AppendByte('"')
}

func (enc *ConsoleEncoder) AppendTime(val time.Time) {
	cur := enc.buf.Len()

	if e := enc.EncodeTime; e != nil {
		e(val, enc)
	}

	if cur == enc.buf.Len() {
		enc.AppendInt64(val.UnixNano())
	}
}

func (enc *ConsoleEncoder) AppendUint64(val uint64) {
	enc.addElementSeparator()
	enc.buf.AppendUint(val)
}

func (enc *ConsoleEncoder) AddInt(k string, v int)         { enc.AddInt64(k, int64(v)) }
func (enc *ConsoleEncoder) AddInt32(k string, v int32)     { enc.AddInt64(k, int64(v)) }
func (enc *ConsoleEncoder) AddInt16(k string, v int16)     { enc.AddInt64(k, int64(v)) }
func (enc *ConsoleEncoder) AddInt8(k string, v int8)       { enc.AddInt64(k, int64(v)) }
func (enc *ConsoleEncoder) AddUint(k string, v uint)       { enc.AddUint64(k, uint64(v)) }
func (enc *ConsoleEncoder) AddUint32(k string, v uint32)   { enc.AddUint64(k, uint64(v)) }
func (enc *ConsoleEncoder) AddUint16(k string, v uint16)   { enc.AddUint64(k, uint64(v)) }
func (enc *ConsoleEncoder) AddUint8(k string, v uint8)     { enc.AddUint64(k, uint64(v)) }
func (enc *ConsoleEncoder) AddUintptr(k string, v uintptr) { enc.AddUint64(k, uint64(v)) }
func (enc *ConsoleEncoder) AppendComplex64(v complex64) {
	enc.appendComplex(complex128(v), _sizeFloat32)
}

func (enc *ConsoleEncoder) AppendComplex128(v complex128) {
	enc.appendComplex(complex128(v), _sizeFloat64)
}
func (enc *ConsoleEncoder) AppendFloat64(v float64) { enc.appendFloat(v, _sizeFloat64) }
func (enc *ConsoleEncoder) AppendFloat32(v float32) { enc.appendFloat(float64(v), _sizeFloat32) }
func (enc *ConsoleEncoder) AppendInt(v int)         { enc.AppendInt64(int64(v)) }
func (enc *ConsoleEncoder) AppendInt32(v int32)     { enc.AppendInt64(int64(v)) }
func (enc *ConsoleEncoder) AppendInt16(v int16)     { enc.AppendInt64(int64(v)) }
func (enc *ConsoleEncoder) AppendInt8(v int8)       { enc.AppendInt64(int64(v)) }
func (enc *ConsoleEncoder) AppendUint(v uint)       { enc.AppendUint64(uint64(v)) }
func (enc *ConsoleEncoder) AppendUint32(v uint32)   { enc.AppendUint64(uint64(v)) }
func (enc *ConsoleEncoder) AppendUint16(v uint16)   { enc.AppendUint64(uint64(v)) }
func (enc *ConsoleEncoder) AppendUint8(v uint8)     { enc.AppendUint64(uint64(v)) }
func (enc *ConsoleEncoder) AppendUintptr(v uintptr) { enc.AppendUint64(uint64(v)) }

func (enc *ConsoleEncoder) Clone() zapcore.Encoder {
	clone := enc.clone()
	_, _ = clone.buf.Write(enc.buf.Bytes())

	return clone
}

// appendComplex appends the encoded form of the provided complex128 value.
// precision specifies the encoding precision for the real and imaginary
// components of the complex number.
func (enc *ConsoleEncoder) appendComplex(val complex128, precision int) {
	enc.addElementSeparator()
	// Cast to a platform-independent, fixed-size type.
	r, floatValue := float64(real(val)), float64(imag(val))

	enc.buf.AppendByte('"')
	// Because we're always in a quoted string, we can use strconv without
	// special-casing NaN and +/-Inf.
	enc.buf.AppendFloat(r, precision)
	// If imaginary part is less than 0, minus (-) sign is added by default
	// by AppendFloat.
	if floatValue >= 0 {
		enc.buf.AppendByte('+')
	}

	enc.buf.AppendFloat(floatValue, precision)
	enc.buf.AppendByte('i')
	enc.buf.AppendByte('"')
}

func (enc *ConsoleEncoder) encodeReflected(interface{}) ([]byte, error) { return nil, nil }

func (enc *ConsoleEncoder) clone() *ConsoleEncoder {
	clone := NewConsoleEncoder(enc.EncoderConfig)
	clone.openNamespaces = enc.openNamespaces
	clone.buf = buffer.NewPool().Get()

	return clone
}

func (enc *ConsoleEncoder) closeOpenNamespaces() {
	for range make([]struct{}, enc.openNamespaces) {
		enc.buf.AppendByte('}')
	}

	enc.openNamespaces = 0
}

func (enc *ConsoleEncoder) addKey(key string) {
	enc.addElementSeparator()
	enc.buf.AppendByte('"')
	enc.safeAddString(key)
	enc.buf.AppendByte('"')
	enc.buf.AppendByte(':')
}

func (enc *ConsoleEncoder) addElementSeparator() {
	last := enc.buf.Len() - 1
	if last < 0 {
		return
	}

	switch enc.buf.Bytes()[last] {
	case '{', '[', ':', ',', ' ':
		return
	default:
		enc.buf.AppendByte(',')
	}
}

func (enc *ConsoleEncoder) appendFloat(val float64, bitSize int) {
	enc.addElementSeparator()

	switch {
	case math.IsNaN(val):
		enc.buf.AppendString(`"NaN"`)
	case math.IsInf(val, 1):
		enc.buf.AppendString(`"+Inf"`)
	case math.IsInf(val, -1):
		enc.buf.AppendString(`"-Inf"`)
	default:
		enc.buf.AppendFloat(val, bitSize)
	}
}

// safeAddString JSON-escapes a string and appends it to the internal buffer.
// Unlike the standard library's encoder, it doesn't attempt to protect the
// user from browser vulnerabilities or JSONP-related problems.
func (enc *ConsoleEncoder) safeAddString(s string) {
	safeAppendStringLike(
		(*buffer.Buffer).AppendString,
		utf8.DecodeRuneInString,
		enc.buf,
		s,
	)
}

// safeAddByteString is no-alloc equivalent of safeAddString(string(s)) for s []byte.
func (enc *ConsoleEncoder) safeAddByteString(s []byte) {
	safeAppendStringLike(
		(*buffer.Buffer).AppendBytes,
		utf8.DecodeRune,
		enc.buf,
		s,
	)
}
