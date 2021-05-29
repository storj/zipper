package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/jtolio/zipper"
	"storj.io/uplink"
)

var (
	flagAccess = flag.String("access", "", "")
	flagBucket = flag.String("bucket", "", "")
	flagKey    = flag.String("key", "", "")
	flagFiles  = flag.Int("files", 1, "")
)

func main() {
	flag.Parse()
	err := Main(context.Background())
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
}

func Main(ctx context.Context) error {
	ag, err := uplink.ParseAccess(*flagAccess)
	if err != nil {
		return err
	}
	log.Println("opening project")
	proj, err := uplink.OpenProject(ctx, ag)
	if err != nil {
		return err
	}
	defer proj.Close()

	switch flag.Arg(0) {
	case "create":
		return Create(ctx, proj)
	case "open":
		return Open(ctx, proj)
	default:
		return fmt.Errorf("expected create or open command")
	}
}

func Create(ctx context.Context, proj *uplink.Project) error {
	log.Println("creating pack")
	p, err := zipper.CreatePack(ctx, proj, *flagBucket, *flagKey, nil)
	if err != nil {
		return err
	}
	defer p.Abort()

	log.Println("adding files")
	for i := 0; i < *flagFiles; i++ {
		w, err := p.Add(ctx, fmt.Sprintf("hello-%d.txt", i), nil)
		if err != nil {
			return err
		}
		_, err = w.Write([]byte("Hello, world!"))
		if err != nil {
			return err
		}
	}

	log.Println("committing")
	return p.Commit(ctx)
}

func Open(ctx context.Context, proj *uplink.Project) error {
	log.Println("opening pack")
	p, err := zipper.OpenPack(ctx, proj, *flagBucket, *flagKey)
	if err != nil {
		return err
	}
	log.Println("listing")
	for _, fname := range p.List() {
		rc, err := p.Open(ctx, fname)
		if err != nil {
			return err
		}
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			return err
		}
		_, err = fmt.Printf("%q\n%q\n", fname, string(data))
		if err != nil {
			return err
		}
	}
	return nil
}
