package arbitraryjsonreceiver

import (
	"crypto/rand"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"

	trc "go.opentelemetry.io/otel/trace"
)

type simpleSpan struct {
	Samplerate int                    `json:"samplerate"`
	Time       string                 `json:"time"` // epoch miliseconds.nanoseconds
	Data       map[string]interface{} `json:"data"`
}

func toTraces(chunks map[string][]simpleSpan) (ptrace.Traces, error) {
	// Creating a map of service spans to slices
	// since the expectation is that `service.name`
	// is added as a resource attribute in most systems
	// now instead of being a span level attribute.
	groupByService := make(map[string]ptrace.SpanSlice)
	count := 0
	for thisServiceName, serviceNameChunks := range chunks {

		for _, span := range serviceNameChunks {
			count += 1
			slice, exist := groupByService[thisServiceName]
			if !exist {
				slice = ptrace.NewSpanSlice()
				groupByService[thisServiceName] = slice
			}
			newSpan := slice.AppendEmpty()

			// turn duraiton seconds with decimals into a nanoseconds without decimal field
			time_s, err := strconv.ParseFloat(span.Time, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing timestamp %v error %s", span.Time, err.Error())
			}
			time_ns := uint64(time_s * 1000000000)
			// fmt.Fprintf(os.Stderr, "Old time (s): %d \nOffset (s): %d\nNew time (ns): %d\n", uint64(time_s*1000000000), secondsOffset, time_ns)

			durationfields := []string{"duration_ms", "timing.duration_ms", "timing.sysctl_ms"}
			duration_ms := 0.0
			for _, df := range durationfields {
				if duration, okay := span.Data[df]; okay {
					duration_ms = duration.(float64)
					break
				}
			}
			end_timestamp := time_ns + (uint64(duration_ms) * 1000000)

			if tid, ok := span.Data["trace.trace_id"]; ok {
				tid := strings.Replace(tid.(string), "-", "", -1)
				if len(tid) > 32 {
					// fmt.Println("SpanID was too long now it's shortened to: %v", sid)
					tid = tid[0:32]
				}
				newTraceId := pcommon.TraceID(stringToTraceID(tid))
				newSpan.SetTraceID(newTraceId)
			} else {
				newSpan.SetTraceID(pcommon.TraceID(fakeMeAnId(32)))
			}

			if sid, ok := span.Data["trace.span_id"]; ok {
				sid := strings.Replace(sid.(string), "-", "", -1)
				if len(sid) == 32 {
					// take the middle
					sid = sid[8:24]
				} else if len(sid) > 16 {
					// fmt.Println("SpanID was too long now it's shortened to: %v", sid)
					sid = sid[0:16]
				}
				newTraceId := pcommon.SpanID(stringToSpanID(sid))
				newSpan.SetSpanID(newTraceId)
			} else {
				newSpan.SetSpanID(pcommon.SpanID(fakeMeAnId(16)))
			}

			newSpan.SetStartTimestamp(pcommon.Timestamp(time_ns))
			newSpan.SetEndTimestamp(pcommon.Timestamp(end_timestamp))
			if pid, ok := span.Data["trace.parent_id"]; ok {
				newSpan.SetParentSpanID(pcommon.SpanID(stringToSpanID(pid.(string))))
			}
			newSpan.SetName(span.Data["name"].(string))
			newSpan.Status().SetCode(ptrace.StatusCodeOk)

			if _, ok := span.Data["error"]; ok {
				newSpan.Status().SetCode(ptrace.StatusCodeError)
			}

			switch span.Data["span.kind"].(string) {
			case "server":
				newSpan.SetKind(ptrace.SpanKindServer)
			case "client":
				newSpan.SetKind(ptrace.SpanKindClient)
			case "producer":
				newSpan.SetKind(ptrace.SpanKindProducer)
			case "consumer":
				newSpan.SetKind(ptrace.SpanKindConsumer)
			case "internal":
				newSpan.SetKind(ptrace.SpanKindInternal)
			default:
				newSpan.SetKind(ptrace.SpanKindUnspecified)
			}

			newSpan.Attributes().PutInt("SampleRate", int64(span.Samplerate))

			already_used_fields := []string{
				"time",
				"samplerate",
				"span.kind",
				"error",
				"name",
				"trace.parent_id",
				"duration_ms",
				"trace.span_id",
				"trace.trace_id",
				"service.name",
			}

			for k, v := range span.Data {
				if slices.Contains(already_used_fields, k) {
					continue
				}
				switch v.(type) {
				case string:
					newSpan.Attributes().PutStr(k, v.(string))
				case int:
					newSpan.Attributes().PutInt(k, v.(int64))
				case float64:
					newSpan.Attributes().PutDouble(k, v.(float64))
				case bool:
					newSpan.Attributes().PutBool(k, v.(bool))
				default:
					fmt.Fprintf(os.Stderr, "data type issue: %v is the key for type %t where value is %v", k, v, v)
				}
			}
		}
	}

	results := ptrace.NewTraces()
	for service, spans := range groupByService {
		rs := results.ResourceSpans().AppendEmpty()
		rs.SetSchemaUrl(semconv.SchemaURL)
		// sharedAttributes.CopyTo(rs.Resource().Attributes())
		rs.Resource().Attributes().PutStr(semconv.AttributeServiceName, service)

		in := rs.ScopeSpans().AppendEmpty()
		in.Scope().SetName("otelcol-jsonhttp")
		in.Scope().SetVersion("1.0.0")
		spans.CopyTo(in.Spans())
	}

	return results, nil
}

// read the json body into an array of batches by service.name

/*
func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)

	ctx := context.Background()
	go func() {
		for {
			sig := <-sigChan
			signalHandler(ctx, sig)
		}
	}()

	for idx, readFile := range filesToCheck {
		readFile := readFile // make a local copy?
		idx := idx
		ctxFile, spanFile := tracer.Start(ctx, "reading file")
		spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("file.name"), Value: attribute.StringValue(readFile.Name())})
		g.Go(func() error {
			// skip some stuff
			if readFile.IsDir() {
				fmt.Printf("Skip subdirectories: %v\n", readFile.Name())
				spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("action"), Value: attribute.StringValue("skipped")})
				spanFile.End()
				return nil
			}
			if strings.HasPrefix(readFile.Name(), ".") {
				fmt.Printf("Skip hidden file: %v\n", readFile.Name())
				spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("action"), Value: attribute.StringValue("skipped")})
				spanFile.End()
				return nil
			}
			if strings.Contains(readFile.Name(), "otel") {
				fmt.Printf("%d filename %v already otel formatted\n", idx, readFile.Name())
				spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("action"), Value: attribute.StringValue("skipped")})
				spanFile.End()
				return nil
			}
			fh, err := os.Open(filepath.Join(sourcePath, readFile.Name()))
			if err != nil {
				spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("action"), Value: attribute.StringValue("skipped")})
				spanFile.End()
				fmt.Printf("%d filename %v could not be opened.\n", idx, readFile.Name())
				return nil
			}

			if strings.HasSuffix(readFile.Name(), ".gz") {
				gzf, err := gzip.NewReader(fh)
				if err != nil {
					fmt.Printf("%d filename %v was a bad gz. %v\n", idx, readFile.Name(), err.Error())
				}
				scanner = bufio.NewScanner(gzf)
			} else {
				fmt.Printf("%d filename %v not *.gz.\n", idx, readFile.Name())
				spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("action"), Value: attribute.StringValue("skipped")})
				return nil
				// scanner = bufio.NewScanner(fh)
			}
			spanFile.SetAttributes(attribute.KeyValue{Key: attribute.Key("action"), Value: attribute.StringValue("parsing")})

			parserStartTime := time.Now()
			simpleSpans, err := readInput(scanner)
			fh.Close()
			if(err)

			traceShaped, err := toTraces(simpleSpans)

			marshaler := &ptrace.JSONMarshaler{}
			// write multiple files
			for key, tss := range traceShaped {
				_, mashspan := tracer.Start(ctxFile, "Marshal the new spans to JSON")

				jsontxt, err := marshaler.MarshalTraces(tss)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error marshaling JSON from simpleSpans in file %v\n", readFile.Name())
					mashspan.SetAttributes(attribute.KeyValue{Key: attribute.Key("err"), Value: attribute.StringValue(err.Error())})
					mashspan.SetStatus(codes.Error, "Marshaling JSON failed")
					return err
				}
				mashspan.End()
				_, writespan := tracer.Start(ctxFile, "Write output file")

				writeFileName := fmt.Sprintf("%v/%d-%v.otel.gz", runDir, key, readFile.Name())
				writespan.SetAttributes(attribute.KeyValue{Key: attribute.Key("output.file.name"), Value: attribute.StringValue(writeFileName)})

				writeFile, err := os.Create(writeFileName)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error creating file %s: %v\n", writeFileName, err.Error())
					writespan.SetStatus(codes.Error, "error creating file")
					return err
				}
				gz := gzip.NewWriter(writeFile)
				_, err = gz.Write(jsontxt)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error writing to file %s, %v\n", writeFileName, err.Error())
					writespan.SetStatus(codes.Error, "error with gz.Write()")
					return err
				}
				if err := gz.Close(); err != nil {
					fmt.Fprintf(os.Stderr, "Error writing to file %s, %v\n", writeFileName, err.Error())
					writespan.SetStatus(codes.Error, "error with gz.Close()")
					return err
				}
				writespan.SetAttributes(attribute.Int("file.span_count", tss.SpanCount()))
				fmt.Fprintf(os.Stderr, "Converted %v to otel at %v\n", readFile.Name(), writeFileName)
				fmt.Fprintf(os.Stderr, "number of spans: %d in %d milliseconds\n", tss.SpanCount(), time.Since(parserStartTime).Milliseconds())
				writespan.End()
			}
			spanFile.End()
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		fmt.Printf("Wrapping up error: %v", err)
	}

	span2.End()
	span.End()
	fmt.Println("All done")
	tp.ForceFlush(ctx)
	tp.Shutdown(ctx)
	time.Sleep(5000 * time.Millisecond)
	os.Exit(0)
}
*/

func stringToTraceID(id string) trc.TraceID {
	traceID, err := trc.TraceIDFromHex(id)
	if err != nil {
		return trc.TraceID(fakeMeAnId(32))
	}
	return traceID
}

func stringToSpanID(id string) trc.SpanID {
	spanID, err := trc.SpanIDFromHex(id)
	if err != nil {
		return trc.SpanID(fakeMeAnId(16))
	}
	return spanID
}

func fakeMeAnId(length int) []byte {
	token := make([]byte, length)
	rand.Read(token)
	return token
}
