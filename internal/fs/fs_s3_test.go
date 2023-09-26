package fs

import (
	"fmt"
	"testing"
)

func TestFS3(t *testing.T) {
	fs, err := NewS3Filesystem(nil, "")
	if err != nil {
		t.Fatal(err)
	}

	info, err := fs.Stat("test")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(info)
}
