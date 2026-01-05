package zapconsole

import (
	"fmt"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func (enc *ConsoleEncoder) EncodeResponse(arr *sliceArrayEncoder, fields []zapcore.Field) {
	var status int64

	var duration time.Duration

	var method, path, sessionID string

	for fieldIndx := range fields {
		switch fields[fieldIndx].Key {
		case "status":
			status = fields[fieldIndx].Integer
		case "duration":
			duration = time.Duration(fields[fieldIndx].Integer)
		case "method":
			method = fields[fieldIndx].String
		case "path":
			path = fields[fieldIndx].String
		case "session_id":
			sessionID = fields[fieldIndx].String
		default:
		}
	}

	arr.AppendInt64(status)
	arr.AppendString(method)
	arr.AppendString(path)
	arr.AppendString(sessionID)
	arr.AppendString(duration.String())
}

func (enc *ConsoleEncoder) EncodeCustomLog(
	arr *sliceArrayEncoder,
	ent *zapcore.Entry,
	line *buffer.Buffer,
	fields []zapcore.Field,
) {
	if ent.Caller.Defined {
		if enc.CallerKey != "" && enc.EncodeCaller != nil {
			enc.EncodeCaller(ent.Caller, arr)
		}

		if enc.FunctionKey != "" {
			arr.AppendString(ent.Caller.Function)
		}
	}

	if enc.MessageKey != "" {
		arr.AppendString(ent.Message)
	}

	appendFields(arr, fields)
}

func appendFields(arr *sliceArrayEncoder,
	fields []zapcore.Field,
) {
	for fieldIndx := range fields {
		if fields[fieldIndx].Key == "error" {
			arr.AppendString(fields[fieldIndx].String)
		}
	}
}

func (enc *ConsoleEncoder) encodeFixedFields(arr *sliceArrayEncoder, ent *zapcore.Entry) {
	if enc.TimeKey != "" && enc.EncodeTime != nil && !ent.Time.IsZero() {
		enc.EncodeTime(ent.Time, arr)
	}

	if enc.LevelKey != "" && enc.EncodeLevel != nil {
		enc.EncodeLevel(ent.Level, arr)
	}
}

func (enc *ConsoleEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	line := buffer.NewPool().Get()

	arr := getSliceEncoder()
	enc.encodeFixedFields(arr, &ent)

	if ent.LoggerName == "responseLogger" {
		enc.EncodeResponse(arr, fields)
	} else {
		enc.EncodeCustomLog(arr, &ent, line, fields)
	}

	for indx := range arr.elems {
		if indx > 0 {
			line.AppendString(enc.ConsoleSeparator)
		}

		_, _ = fmt.Fprint(line, arr.elems[indx])
	}

	putSliceEncoder(arr)

	// If there's no stacktrace key, honor that; this allows users to force
	// single-line output.
	if ent.Stack != "" && enc.StacktraceKey != "" {
		line.AppendByte('\n')
		line.AppendString(ent.Stack)
	}

	line.AppendString(enc.LineEnding)

	return line, nil
}
