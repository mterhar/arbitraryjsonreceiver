package arbitraryjsonreceiver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/arbitraryjsonreceiver/internal/errors"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/arbitraryjsonreceiver/internal/httphelper"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/arbitraryjsonreceiver/internal/trace"
)

// Pre-computed status with code=Internal to be used in case of a marshaling error.
var fallbackMsg = []byte(`{"code": 13, "message": "failed to marshal error message"}`)

const fallbackContentType = "application/json"

func handleTraces(resp http.ResponseWriter, req *http.Request, tracesReceiver *trace.Receiver) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	simpleSpans, err := readInput(resp, req, enc) // enc.unmarshalTracesRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpTraces, err := toTraces(simpleSpans)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpReq := ptraceotlp.NewExportRequestFromTraces(otlpTraces)

	otlpResp, err := tracesReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalTracesResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func readInput(resp http.ResponseWriter, req *http.Request, enc encoder) (map[string][]simpleSpan, error) {
	simpleSpans := make(map[string][]simpleSpan)
	lineNum := 0
	defer func() {
		panic := recover()
		err, ok := panic.(error)
		// recover from panic if one occurred. Set err to nil otherwise.
		if !ok && err != nil {
			writeError(resp, enc, err, http.StatusBadRequest)
			return
		}
	}()

	scanner := bufio.NewScanner(req.Body)

	for scanner.Scan() {
		lineNum += 1
		var ss simpleSpan
		thisLine := scanner.Bytes()
		err := json.Unmarshal(thisLine, &ss)
		if err != nil {
			writeError(resp, enc, err, http.StatusBadRequest)
			//fmt.Fprintf(os.Stderr, "encountered json error on line %d:\nError: %v\nJSON line: %v\n", lineNum, err.Error(), string(thisLine))
			continue
		}
		thisServiceName := ss.Data["service.name"].(string)
		simpleSpans[thisServiceName] = append(simpleSpans[thisServiceName], ss)
	}
	return simpleSpans, nil
}

// writeError encodes the HTTP error inside a rpc.Status message as required by the OTLP protocol.
func writeError(w http.ResponseWriter, encoder encoder, err error, statusCode int) {
	s, ok := status.FromError(err)
	if ok {
		statusCode = errors.GetHTTPStatusCodeFromStatus(s)
	} else {
		s = httphelper.NewStatusFromMsgAndHTTPCode(err.Error(), statusCode)
	}
	writeStatusResponse(w, encoder, statusCode, s.Proto())
}

// errorHandler encodes the HTTP error message inside a rpc.Status message as required
// by the OTLP protocol.
func errorHandler(w http.ResponseWriter, r *http.Request, errMsg string, statusCode int) {
	s := httphelper.NewStatusFromMsgAndHTTPCode(errMsg, statusCode)
	switch getMimeTypeFromContentType(r.Header.Get("Content-Type")) {
	case jsonContentType:
		writeStatusResponse(w, jsEncoder, statusCode, s.Proto())
		return
	}
	writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
}

func writeStatusResponse(w http.ResponseWriter, enc encoder, statusCode int, rsp *spb.Status) {
	msg, err := enc.marshalStatus(rsp)
	if err != nil {
		writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
		return
	}

	writeResponse(w, enc.contentType(), statusCode, msg)
}

func readContentType(resp http.ResponseWriter, req *http.Request) (encoder, bool) {
	if req.Method != http.MethodPost {
		handleUnmatchedMethod(resp)
		return nil, false
	}

	switch getMimeTypeFromContentType(req.Header.Get("Content-Type")) {
	case jsonContentType:
		return jsEncoder, true
	default:
		handleUnmatchedContentType(resp)
		return nil, false
	}
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// Nothing we can do with the error if we cannot write to the response.
	_, _ = w.Write(msg)
}

func getMimeTypeFromContentType(contentType string) string {
	mediatype, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return mediatype
}

func handleUnmatchedMethod(resp http.ResponseWriter) {
	status := http.StatusMethodNotAllowed
	writeResponse(resp, "text/plain", status, []byte(fmt.Sprintf("%v method not allowed, supported: [POST]", status)))
}

func handleUnmatchedContentType(resp http.ResponseWriter) {
	status := http.StatusUnsupportedMediaType
	writeResponse(resp, "text/plain", status, []byte(fmt.Sprintf("%v unsupported media type, supported: [%s, %s]", status, jsonContentType, pbContentType)))
}
