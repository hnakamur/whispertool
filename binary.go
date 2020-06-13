package whispertool

import "unsafe"

var one = int(0x1)
var isBigEndian = *(*byte)(unsafe.Pointer(&one)) == 0

func htonl(netlong uint32) uint32 {
	if isBigEndian {
		return netlong
	}
	return (netlong << 24) |
		((netlong << 8) & 0x00FF0000) |
		((netlong >> 8) & 0x0000FF00) |
		(netlong >> 24)
}

func ntohl(hostlong uint32) uint32 {
	if isBigEndian {
		return hostlong
	}
	return (hostlong << 24) |
		((hostlong << 8) & 0x00FF0000) |
		((hostlong >> 8) & 0x0000FF00) |
		(hostlong >> 24)
}

func htonll(netlonglong uint64) uint64 {
	if isBigEndian {
		return netlonglong
	}
	return (netlonglong << 56) |
		((netlonglong << 40) & 0x00FF000000000000) |
		((netlonglong << 24) & 0x0000FF0000000000) |
		((netlonglong << 8) & 0x000000FF00000000) |
		((netlonglong >> 8) & 0x00000000FF000000) |
		((netlonglong >> 24) & 0x0000000000FF0000) |
		((netlonglong >> 40) & 0x000000000000FF00) |
		(netlonglong >> 56)
}

func ntohll(hostlonglong uint64) uint64 {
	if isBigEndian {
		return hostlonglong
	}
	return (hostlonglong << 56) |
		((hostlonglong << 40) & 0x00FF000000000000) |
		((hostlonglong << 24) & 0x0000FF0000000000) |
		((hostlonglong << 8) & 0x000000FF00000000) |
		((hostlonglong >> 8) & 0x00000000FF000000) |
		((hostlonglong >> 24) & 0x0000000000FF0000) |
		((hostlonglong >> 40) & 0x000000000000FF00) |
		(hostlonglong >> 56)
}
