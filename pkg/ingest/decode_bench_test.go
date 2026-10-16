package ingest

import (
	"bytes"
	"fmt"
	"testing"
)

// benchEvent is a Sysmon-style process event with a dozen numbers, one a nanosecond
// epoch past 2^53, so every decode keeps exact digits.
func benchEvent(i int) string {
	return fmt.Sprintf(`{"@timestamp":"2026-10-16T12:00:%02d.123Z","event_id":1,"computer_name":"WS-%04d.corp.example.com","user":"CORP\\user%d","image":"C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe","parent_image":"C:\\Windows\\explorer.exe","command_line":"powershell.exe -NoProfile -ExecutionPolicy Bypass -File C:\\scripts\\job%d.ps1","process_id":%d,"parent_process_id":%d,"process_guid":"{4a1b2c3d-%04x-6543-0000-0010abcdef01}","logon_id":%d,"src_ip":"10.1.%d.%d","src_port":%d,"dst_ip":"93.184.216.34","dst_port":443,"bytes_sent":%d,"duration_ms":%d.%03d,"cpu":0.%02d,"record_id":%d,"event_ns":%d,"integrity_level":"High","hashes":"SHA256=9F86D081884C7D659A2FEAE0C55AD015A3BF4F1B2B0B822CD15D6C15B0F00A08","agent":{"name":"sysmon","version":"15.15","pid":%d,"ok":true,"tags":["edr","windows"]}}`,
		i%60, i%5000, i%300, i, 4000+i, 600+i%50, i, 0x3e7000+int64(i)*17, i%250, i%250, 49152+i%16000,
		int64(i)*1337, i%900, i%1000, i%100, 100000000+i, int64(1760616000123456789)+int64(i), 2000+i%50)
}

func benchBodies() (ndjson, array, bulk []byte) {
	var nd, arr, bk bytes.Buffer
	arr.WriteByte('[')
	for i := 0; i < 1000; i++ {
		ev := benchEvent(i)
		nd.WriteString(ev)
		nd.WriteByte('\n')
		if i > 0 {
			arr.WriteByte(',')
		}
		arr.WriteString(ev)
		bk.WriteString(`{"index":{"_index":"logs"}}` + "\n" + ev + "\n")
	}
	arr.WriteByte(']')
	return nd.Bytes(), arr.Bytes(), bk.Bytes()
}

func BenchmarkIngestDecodeNDJSON(b *testing.B) {
	nd, _, _ := benchBodies()
	h := &IngestHandler{}
	b.SetBytes(int64(len(nd)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, err := h.parseJSONLogsWithConfig(nd, nil, nil)
		if err != nil || len(logs) != 1000 {
			b.Fatalf("parsed %d: %v", len(logs), err)
		}
	}
}

func BenchmarkIngestDecodeArray(b *testing.B) {
	_, arr, _ := benchBodies()
	h := &IngestHandler{}
	b.SetBytes(int64(len(arr)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, err := h.parseJSONLogsWithConfig(arr, nil, nil)
		if err != nil || len(logs) != 1000 {
			b.Fatalf("parsed %d: %v", len(logs), err)
		}
	}
}

func BenchmarkIngestDecodeBulk(b *testing.B) {
	_, _, bk := benchBodies()
	h := &ElasticBulkHandler{}
	b.SetBytes(int64(len(bk)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, _, err := h.parseBulkRequest(bk, nil, nil)
		if err != nil || len(logs) != 1000 {
			b.Fatalf("parsed %d: %v", len(logs), err)
		}
	}
}
