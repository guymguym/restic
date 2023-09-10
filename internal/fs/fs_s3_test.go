package fs

import (
	"fmt"
	"testing"
)

func TestFS3(t *testing.T) {
	fs3, err := NewFS3()
	if err != nil {
		t.Fatal(err)
	}
	info, err := fs3.Stat("logs")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(info)
}
