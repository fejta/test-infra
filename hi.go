package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type visitor struct {
	origin string
	dest   string
}

func (v visitor) visit(path string, info os.FileInfo, verr error) error {
	r, err := filepath.Rel(v.origin, path)
	if err != nil {
		return fmt.Errorf("%q is not relative to %q: %v", path, v.origin, err)
	}
	if r == ".." || strings.HasPrefix(r, "../") {
		return fmt.Errorf("%q is not a child of %q", path, v.origin)
	}
	if strings.HasSuffix(path, "/testdata") || strings.Contains(path, "/testdata/") {
		log.Printf("Skipping %s...", path)
		return nil
	}
	d := filepath.Join(v.dest, r)
	if info.IsDir() {
		log.Printf("mkdir -p %q", d)
		if err = os.MkdirAll(d, info.Mode()); err != nil {
			return fmt.Errorf("failed to create %q: %v", d, err)
		}
		return nil
	}
	if !strings.HasSuffix(path, ".go") {
		return nil
	}
	if err = os.Link(path, d); err == nil {
		log.Printf("ln %q %q", path, d)
		return nil
	}
	log.Printf("ln -s %q %q", path, d)
	return os.Symlink(path, d)
}

func main() {
	v := visitor{
		origin: os.Args[1],
		dest:   os.Args[2],
	}
	log.Println(filepath.Walk(v.origin, v.visit))
}
