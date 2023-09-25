package fs

import (
	"fmt"
	"net/url"
	"testing"
)

func TestFS3(t *testing.T) {
	u, err := url.Parse("s3/http://profile@endpoint")
	if err != nil {
		t.Fatal(err)
	}

	fs3, err := NewFS3(u)
	if err != nil {
		t.Fatal(err)
	}

	info, err := fs3.Stat("logs")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(info)
}
