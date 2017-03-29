package memcache

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrNeedMoreData = errors.New("need more data")

	errCommand = errors.New("command err")
)

// ParseCommand parses cmd from `data` and return CommandInfo
// return ErrNeedMoreData if data not contains '\n'
// implements: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
func ParseCommand(data []byte) (advance int, cmdinfo *CommandInfo, err error) {
	idx := bytes.IndexByte(data, '\n')
	if idx < 0 {
		return 0, nil, ErrNeedMoreData
	}
	advance = idx + 1
	left := bytes.TrimSpace(data[:advance])
	if len(left) == 0 {
		return advance, nil, errCommand
	}

	var cmd string
	idx = bytes.IndexByte(left, ' ')
	if idx < 0 { // single cmd
		cmd = string(left)
		left = left[0:0]
	} else {
		cmd = string(left[:idx])
		left = left[idx+1:] // remove cmd
	}

	var parser func(cmd string, left []byte) (*CommandInfo, error)
	switch cmd {
	case "set", "add", "replace", "append", "prepend", "cas":
		parser = parseStorageCommands
	case "get", "gets":
		parser = parseRetrievalCommands
	case "delete":
		parser = parseDeleteCommand
	case "incr", "decr":
		parser = parseIncrDecrCommands
	case "touch":
		parser = parseTouchCommand
	default:
		parser = parseOtherCommands
	}
	cmdinfo, err = parser(cmd, left)
	return advance, cmdinfo, err
}

var (
	norepl = []byte("noreply")
)

// parse:
// <command name> <key> <flags> <exptime> <bytes> [noreply]
// cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]
func parseStorageCommands(cmd string, line []byte) (*CommandInfo, error) {
	c := CommandInfo{Cmd: cmd}

	if cmd != "cas" {
		n, _ := fmt.Sscanf(string(line), "%s %d %d %d",
			&c.Key, &c.Flags, &c.Exptime, &c.PayloadLen)
		if n != 4 {
			return nil, errCommand
		}
	} else {
		n, _ := fmt.Sscanf(string(line), "%s %d %d %d %d",
			&c.Key, &c.Flags, &c.Exptime, &c.PayloadLen, &c.CasUnique)
		if n != 5 {
			return nil, errCommand
		}
	}
	if bytes.HasSuffix(line, norepl) {
		c.NoReply = true
	}
	return &c, nil
}

// parse:
// get <key>*
// gets <key>*
func parseRetrievalCommands(cmd string, line []byte) (*CommandInfo, error) {
	if len(line) == 0 {
		return nil, errCommand
	}
	bb := bytes.Split(line, []byte(" "))
	c := CommandInfo{Cmd: cmd}
	c.Keys = make([]string, 0, len(bb))
	for _, b := range bb {
		c.Keys = append(c.Keys, string(b))
	}
	c.Key = c.Keys[0]
	return &c, nil
}

// parse:
// delete <key> [noreply]
func parseDeleteCommand(cmd string, line []byte) (*CommandInfo, error) {
	c := CommandInfo{Cmd: cmd}
	bb := bytes.Split(line, []byte(" "))
	c.Key = string(bb[0])
	if len(bb) == 1 {
		return &c, nil
	}
	if len(bb) != 2 || !bytes.Equal(bb[1], norepl) {
		return nil, errCommand
	}
	c.NoReply = true
	return &c, nil
}

// parse:
// incr <key> <value> [noreply]
// decr <key> <value> [noreply]
func parseIncrDecrCommands(cmd string, line []byte) (*CommandInfo, error) {
	c := CommandInfo{Cmd: cmd}
	n, _ := fmt.Sscanf(string(line), "%s %d", &c.Key, &c.Delta)
	if n != 2 {
		return nil, errCommand
	}
	if bytes.HasSuffix(line, norepl) {
		c.NoReply = true
	}
	return &c, nil
}

// parse:
// touch <key> <exptime> [noreply]
func parseTouchCommand(cmd string, line []byte) (*CommandInfo, error) {
	c := CommandInfo{Cmd: cmd}
	n, _ := fmt.Sscanf(string(line), "%s %d", &c.Key, &c.Exptime)
	if n != 2 {
		return nil, errCommand
	}
	if bytes.HasSuffix(line, norepl) {
		c.NoReply = true
	}
	return &c, nil
}

func parseOtherCommands(cmd string, line []byte) (*CommandInfo, error) {
	c := CommandInfo{Cmd: cmd}
	if cmd == "stats" {
		bb := bytes.Split(line, []byte(" "))
		c.Keys = make([]string, len(bb), len(bb))
		for i, b := range bb {
			c.Keys[i] = string(b)
		}
		return &c, nil
	}
	return nil, errCommand
}
