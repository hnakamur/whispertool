package whispertool

import "testing"

func TestHtonl(t *testing.T) {
	got := htonl(0x01020304)
	want := uint32(0x04030201)
	if isBigEndian {
		want = got
	}
	if got != want {
		t.Errorf("htonl unmatch, got=0x%08x, want=0x%08x", got, want)
	}
}

func TestNtohl(t *testing.T) {
	got := ntohl(0x01020304)
	want := uint32(0x04030201)
	if isBigEndian {
		want = got
	}
	if got != want {
		t.Errorf("htonl unmatch, got=0x%08x, want=0x%08x", got, want)
	}
}

func TestHtonll(t *testing.T) {
	got := htonll(0x0102030405060708)
	want := uint64(0x0807060504030201)
	if isBigEndian {
		want = got
	}
	if got != want {
		t.Errorf("htonl unmatch, got=0x%016x, want=0x%016x", got, want)
	}
}

func TestNtohll(t *testing.T) {
	got := ntohll(0x0102030405060708)
	want := uint64(0x0807060504030201)
	if isBigEndian {
		want = got
	}
	if got != want {
		t.Errorf("htonl unmatch, got=0x%016x, want=0x%016x", got, want)
	}
}
