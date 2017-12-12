package main

import (
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/dgraph-io/badger"
    "github.com/dgraph-io/badger/options"
    "github.com/dgraph-io/badger/y"
    "github.com/pkg/profile"
)

const mil float64 = 1000000

var (
    numKeys   = flag.Float64("keys_mil", 10.0, "How many million keys to write.")
    valueSize = flag.Int("valsz", 128, "Value size in bytes.")
    dir       = flag.String("dir", "", "Base dir for writes.")
    mode      = flag.String("profile.mode", "", "enable profiling mode, one of [cpu, mem, mutex, block]")
)

var bdb *badger.DB

func humanize(n int64) string {
    if n >= 1000000 {
        return fmt.Sprintf("%6.2fM", float64(n)/1000000.0)
    }
    if n >= 1000 {
        return fmt.Sprintf("%6.2fK", float64(n)/1000.0)
    }
    return fmt.Sprintf("%5.2f", float64(n))
}

func main() {
    flag.Parse()
    switch *mode {
    case "cpu":
        defer profile.Start(profile.CPUProfile).Stop()
    case "mem":
        defer profile.Start(profile.MemProfile).Stop()
    case "mutex":
        defer profile.Start(profile.MutexProfile).Stop()
    case "block":
        defer profile.Start(profile.BlockProfile).Stop()
    default:
        // do nothing
    }

    nw := *numKeys * mil
    fmt.Printf("TOTAL KEYS TO WRITE: %s\n", humanize(int64(nw)))
    opt := badger.DefaultOptions
    opt.TableLoadingMode = options.MemoryMap
    opt.Dir = *dir + "/badger"
    opt.ValueDir = opt.Dir
    opt.SyncWrites = true

    var err error

    fmt.Println("Init Badger")
    y.Check(os.RemoveAll(*dir + "/badger"))
    os.MkdirAll(*dir+"/badger", 0777)
    bdb, err = badger.Open(opt)
    if err != nil {
        log.Fatalf("while opening badger: %v", err)
    }

    // Add your benchmarking code here
    bdb.LoadFromVLogToSnapshot("vlogs", "snap1")

    fmt.Println("closing badger")
    bdb.Close()
}
