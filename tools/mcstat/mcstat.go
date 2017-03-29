package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func PrintUsageAndExit() {
	fmt.Printf("%s addr [interval]\n", os.Args[0])
	os.Exit(1)
}

type Stats struct {
	CmdSet  int64
	CmdGet  int64
	GetHits int64
	Rx      int64
	Tx      int64
	Keys    int64

	Bytes    int64
	MaxBytes int64
}

func (s Stats) Sub(o Stats) Stats {
	s.CmdSet -= o.CmdSet
	s.CmdGet -= o.CmdGet
	s.GetHits -= o.GetHits
	s.Rx -= o.Rx
	s.Tx -= o.Tx
	// st.Bytes
	// st.MaxBytes
	return s
}

func ParseStats(r *bufio.Reader) (Stats, error) {
	var st Stats
	mapping := map[string]interface{}{
		"cmd_set":        &st.CmdSet,
		"cmd_get":        &st.CmdGet,
		"get_hits":       &st.GetHits,
		"bytes_read":     &st.Rx,
		"bytes_written":  &st.Tx,
		"curr_items":     &st.Keys,
		"bytes":          &st.Bytes,
		"limit_maxbytes": &st.MaxBytes,
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return st, err
		}
		line = line[:len(line)-2] // remove \r\n
		if line == "END" {
			break
		}
		for name, v := range mapping {
			if !strings.Contains(line, name) {
				continue
			}
			fmt.Sscanf(line, "STAT "+name+" %v", v)
		}
	}
	return st, nil
}

func main() {
	if len(os.Args) != 2 && len(os.Args) != 3 {
		PrintUsageAndExit()
	}

	addr := os.Args[1]

	var interval int
	if len(os.Args) == 3 {
		interval, _ = strconv.Atoi(os.Args[2])
	}
	if interval <= 0 {
		interval = 1
	}

	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, "11211")
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	r := bufio.NewReader(conn)

	var st0 Stats
	for i := 0; ; i++ {
		conn.SetDeadline(time.Now().Add(10 * time.Second))
		_, err := conn.Write([]byte("stats\r\n"))
		if err != nil {
			log.Fatal(err)
		}
		st1, err := ParseStats(r)
		if err != nil {
			log.Fatal(err)
		}
		if i == 0 {
			st0 = st1
			time.Sleep(time.Duration(interval) * time.Second)
			continue
		}
		diff := st1.Sub(st0)
		if (i-1)%50 == 0 {
			fmt.Printf("%8s %8s %8s %6s %10s %10s %10s %18s\n",
				"SET", "GET", "HITS", "HITR", "KEYS", "RX", "TX", "USAGE")
		}
		fmt.Printf("%8s %8s %8s %6.2f %10d %10s %10s %18s\n",
			readableNum(diff.CmdSet), readableNum(diff.CmdGet),
			readableNum(diff.GetHits), float32(diff.GetHits)/float32(diff.CmdGet), st1.Keys,
			readableSize(diff.Rx), readableSize(diff.Tx),
			readableSize(st1.Bytes)+"/"+readableSize(st1.MaxBytes))
		st0 = st1
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func readableNum(i int64) string {
	if i < (1 << 10) {
		return fmt.Sprintf("%d", i)
	}
	if i < (1 << 20) {
		return fmt.Sprintf("%.2fK", float32(i)/(1<<10))
	}
	if i < (1 << 30) {
		return fmt.Sprintf("%.2fM", float32(i)/(1<<20))
	}
	return fmt.Sprintf("%.2fG", float32(i)/(1<<30))
}

func readableSize(i int64) string {
	return readableNum(i) + "B"
}
