// Read a zip file and report some stats on whatever .dbf is contained within it, as per a Census shapefile bundle for FACES or EDGES etc.

package main

import (
	"archive/zip"
	"log"
	"os"
	"strings"

	dbf "github.com/brianolson/go-dbf"
)

func getField(d *dbf.Dbf, name string) *dbf.DbfField {
	for i, df := range d.Fields {
		if name == df.Name {
			return &d.Fields[i]
		}
	}
	return nil
}

func getBestField(d *dbf.Dbf, names []string) *dbf.DbfField {
	for _, name := range names {
		out := getField(d, name)
		if out != nil {
			return out
		}
	}
	return nil
}

func main() {
	for _, fname := range os.Args[1:] {
		zf, err := zip.OpenReader(fname)
		if err != nil {
			log.Print(fname, ": ", err)
			os.Exit(1)
			return
		}

		for _, zff := range zf.File {
			if strings.HasSuffix(zff.Name, ".dbf") {
				log.Print(fname, " ", zff.Name)
				ior, err := zff.Open()
				if err != nil {
					log.Print(fname, " ", zff.Name, ": ", err)
					os.Exit(1)
					return
				}
				d, err := dbf.NewDbf(ior)
				if err != nil {
					log.Print(fname, " ", zff.Name, ": ", err)
					os.Exit(1)
					return
				}
				state := getBestField(d, []string{"STATEFP10", "STATEFP00"})
				county := getBestField(d, []string{"COUNTYFP10", "COUNTYFP00"})
				tract := getBestField(d, []string{"TRACTCE10", "TRACTCE00"})
				block := getBestField(d, []string{"BLOCKCE10", "BLOCKCE00"})
				if state == nil || county == nil || tract == nil || block == nil {
					log.Print("missing a field. fields...")
					for _, df := range d.Fields {
						log.Print(df.GoString())
					}
					continue
				}
				okcount := 0
				shortcount := 0
				for {
					err = d.Next()
					if err != nil {
						break
					}
					ubid := state.StringValue() + county.StringValue() + tract.StringValue() + block.StringValue()
					//log.Print(ubid)
					if len(ubid) == 15 {
						okcount++
					} else {
						shortcount++
					}
				}
				log.Print("good ubid count=", okcount, " short=", shortcount, " num records=", d.NumRecords)
				//state := getBestField(d, []string{"",""})
			}
		}
	}
}
