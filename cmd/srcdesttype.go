//go:generate enumer -type SrcDestType -transform=snake

package cmd

type SrcDestType int

const (
	Source SrcDestType = iota + 1
	Destination
)
