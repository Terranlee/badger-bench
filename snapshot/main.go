package main

import (
    "flag"
    "fmt"
    "log"
    "math/rand"
    "os"
    "sync"


    "github.com/dgraph-io/badger"
    "github.com/dgraph-io/badger/options"
    "github.com/dgraph-io/badger/y"
)

const mil float64 = 1000000

var (
    which     = flag.String("kv", "badger", "Which KV store to use. Options: badger, rocksdb, lmdb, bolt")
    numKeys   = flag.Float64("keys_mil", 10.0, "How many million keys to write.")
    valueSize = flag.Int("valsz", 128, "Value size in bytes.")
    dir       = flag.String("dir", "", "Base dir for writes.")
    mode      = flag.String("profile.mode", "", "enable profiling mode, one of [cpu, mem, mutex, block]")
)

type entry struct {
    Key   []byte
    Value []byte
    Meta  byte
}

func fillEntry(e *entry) {
    k := rand.Int() % int(*numKeys*mil)
    key := fmt.Sprintf("vsz=%05d-k=%010d", *valueSize, k) // 22 bytes.
    if cap(e.Key) < len(key) {
        e.Key = make([]byte, 2*len(key))
    }
    e.Key = e.Key[:len(key)]
    copy(e.Key, key)

    //rand.Read(e.Value)

    for i, _ := range e.Value {
        e.Value[i] = byte(97 + i % 26)
    }

    e.Meta = 0
}

var bdb *badger.DB

func writeBatch(entries []*entry) int {
    for _, e := range entries {
        fillEntry(e)
    }

    txn := bdb.NewTransaction(true)
    
    for _, e := range entries {
        y.Check(txn.Set(e.Key, e.Value))
    }
    y.Check(txn.Commit(nil))

    return len(entries)
}

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

    N := 12
    var wg sync.WaitGroup
    for i := 0; i < N; i++ {
        wg.Add(1)
        go func(proc int) {
            entries := make([]*entry, 1000)
            for i := 0; i < len(entries); i++ {
                e := new(entry)
                e.Key = make([]byte, 22)
                e.Value = make([]byte, *valueSize)
                entries[i] = e
            }

            var written float64
            for written < nw/float64(N) {
                wrote := float64(writeBatch(entries))
                written += wrote
            }
            wg.Done()
        }(i)
    }
    wg.Wait()

    // Create snapshot, change value size
    bdb.Snapshot("snap1")
    *valueSize = 100

    for i := 0; i < N; i++ {
        wg.Add(1)
        go func(proc int) {
            entries := make([]*entry, 1000)
            for i := 0; i < len(entries); i++ {
                e := new(entry)
                e.Key = make([]byte, 22)
                e.Value = make([]byte, *valueSize)
                entries[i] = e
            }

            var written float64
            for written < nw/float64(N) {
                wrote := float64(writeBatch(entries))
                written += wrote
            }
            wg.Done()
        }(i)
    }
    wg.Wait()

    bdb.Snapshot("snap2")
    *valueSize = 150

    for i := 0; i < N; i++ {
        wg.Add(1)
        go func(proc int) {
            entries := make([]*entry, 1000)
            for i := 0; i < len(entries); i++ {
                e := new(entry)
                e.Key = make([]byte, 22)
                e.Value = make([]byte, *valueSize)
                entries[i] = e
            }

            var written float64
            for written < nw/float64(N) {
                wrote := float64(writeBatch(entries))
                written += wrote
            }
            wg.Done()
        }(i)
    }
    wg.Wait()

    fmt.Println("closing badger")
    bdb.Close()
}
