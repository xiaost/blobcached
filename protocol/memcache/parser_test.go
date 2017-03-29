package memcache

import "testing"

func TestParseSet(t *testing.T) {
	b := []byte("set k1 1 2 3 noreply\r\nxxx\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "set" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if cmd.Key != "k1" {
		t.Fatal("key err", cmd.Key)
	}
	if cmd.Flags != 1 {
		t.Fatal("flags err", cmd.Flags)
	}
	if cmd.Exptime != 2 {
		t.Fatal("exptime err", cmd.Exptime)
	}
	if cmd.PayloadLen != 3 {
		t.Fatal("payload len err", cmd.PayloadLen)
	}
	if cmd.NoReply != true {
		t.Fatal("norepl err")
	}
	if string(b[advance:]) != "xxx\r\n" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseCAS(t *testing.T) {
	b := []byte("cas k1 1 2 3 4 noreply\r\nxxx\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "cas" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if cmd.Key != "k1" {
		t.Fatal("key err", cmd.Key)
	}
	if cmd.Flags != 1 {
		t.Fatal("flags err", cmd.Flags)
	}
	if cmd.Exptime != 2 {
		t.Fatal("exptime err", cmd.Exptime)
	}
	if cmd.PayloadLen != 3 {
		t.Fatal("payload len err", cmd.PayloadLen)
	}
	if cmd.CasUnique != 4 {
		t.Fatal("cas uniq err", cmd.CasUnique)
	}
	if cmd.NoReply != true {
		t.Fatal("norepl err")
	}
	if string(b[advance:]) != "xxx\r\n" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseGet(t *testing.T) {
	b := []byte("get k1 k2\r\nxxx\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "get" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if cmd.Key != "k1" {
		t.Fatal("key err", cmd.Key)
	}
	if len(cmd.Keys) != 2 {
		t.Fatal("keys number err", len(cmd.Keys))
	}
	if cmd.Keys[0] != "k1" {
		t.Fatal("keys[0] err", cmd.Keys[0])
	}
	if cmd.Keys[1] != "k2" {
		t.Fatal("keys[1] err", cmd.Keys[1])

	}
	if string(b[advance:]) != "xxx\r\n" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseDelete(t *testing.T) {
	b := []byte("delete k1 noreply\r\nxxx\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "delete" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if cmd.Key != "k1" {
		t.Fatal("key err", cmd.Key)
	}
	if cmd.NoReply != true {
		t.Fatal("norepl err")
	}
	if string(b[advance:]) != "xxx\r\n" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseIncr(t *testing.T) {
	b := []byte("incr k1 7 noreply\r\nxxx\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "incr" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if cmd.Key != "k1" {
		t.Fatal("key err", cmd.Key)
	}
	if cmd.Delta != 7 {
		t.Fatal("delta err", cmd.Delta)
	}
	if cmd.NoReply != true {
		t.Fatal("norepl err")
	}
	if string(b[advance:]) != "xxx\r\n" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseTouch(t *testing.T) {
	b := []byte("touch k1 7 noreply\r\nxxx\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "touch" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if cmd.Key != "k1" {
		t.Fatal("key err", cmd.Key)
	}
	if cmd.Exptime != 7 {
		t.Fatal("delta err", cmd.Delta)
	}
	if cmd.NoReply != true {
		t.Fatal("norepl err")
	}
	if string(b[advance:]) != "xxx\r\n" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseStats(t *testing.T) {
	b := []byte("stats\r\n")
	advance, cmd, err := ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "stats" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if string(b[advance:]) != "" {
		t.Fatal("left buf err", string(b[advance:]))
	}

	b = []byte("stats settings\r\n")
	advance, cmd, err = ParseCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cmd != "stats" {
		t.Fatal("cmd err", cmd.Cmd)
	}
	if len(cmd.Keys) != 1 || cmd.Keys[0] != "settings" {
		t.Fatal("stats args err")
	}
	if string(b[advance:]) != "" {
		t.Fatal("left buf err", string(b[advance:]))
	}
}

func TestParseErr(t *testing.T) {
	b := []byte("xxx k1 7 noreply\r\nxxx\r\n")
	advance, _, err := ParseCommand(b)
	if err != errCommand {
		t.Fatal("err != errCommand", err)
	}
	if advance != len("xxx k1 7 noreply\r\n") {
		t.Fatal("advance err", advance)
	}
}
