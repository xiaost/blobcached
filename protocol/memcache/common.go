package memcache

var (
	RspErr       = []byte("ERROR\r\n")
	RspStored    = []byte("STORED\r\n")
	RspNotStored = []byte("NOT_STORED\r\n")
	RspExists    = []byte("EXISTS\r\n")
	RspNotFound  = []byte("NOT_FOUND\r\n")
	RspDeleted   = []byte("DELETED\r\n")
	RspEnd       = []byte("END\r\n")
	RspTouched   = []byte("TOUCHED\r\n")

	EOL = []byte("\r\n")
)

func MakeRspClientErr(err error) []byte {
	return []byte("CLIENT_ERROR " + err.Error() + "\r\n")
}

func MakeRspServerErr(err error) []byte {
	return []byte("SERVER_ERROR " + err.Error() + "\r\n")
}

type CommandInfo struct {
	Cmd        string
	Key        string
	Keys       []string // for retrieval commands
	Delta      uint64   // for incr/decr
	Flags      uint32
	Exptime    uint32
	PayloadLen int64
	CasUnique  int64
	NoReply    bool
}
