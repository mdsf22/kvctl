package main

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/magiconair/properties"
	"github.com/mdsf22/kvctl/db"
	_ "github.com/mdsf22/kvctl/db/leveldb"
	"github.com/spf13/cobra"
	// _ "github.com/mdsf22/kvctl/db/rocksdb"
	_ "github.com/mdsf22/kvctl/db/tikv"
)

var (
	//threads is the number of goroutine
	threads int
	//vallen is the length of value
	vallen int
	//keypre is the prefix of the key
	keypre string
	//0 repeat data, 1 random data
	valtype int
	//dbname is name of db
	dbname string
	//initdata
	initdata int
	//propertyvalues is a property value with name=value
	propertyvalues []string
	propertyFiles  []string
)

var (
	cliName        = "kvctl"
	cliDescription = "A simple command line tool for kvdb."
	rootCmd        = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"kvctl"},
	}
	globalProps *properties.Properties
	globalDB    db.KVDB
)

func getCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "get value of key from db",
		Run:   getCommandFunc,
	}
	return cmd
}

func getCommandFunc(cmd *cobra.Command, args []string) {
	initialGlobal()
	key := args[0]
	val, err := globalDB.Get([]byte(key))
	if err != nil {
		fmt.Printf("get %s failed\n", key)
	}
	fmt.Println("val:", string(val))
}

func putCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put <key> <val>",
		Short: "put data to db",
		Run:   putCommandFunc,
	}
	return cmd
}

func putCommandFunc(cmd *cobra.Command, args []string) {
	initialGlobal()
	key := args[0]
	val := args[1]
	err := globalDB.Put([]byte(key), []byte(val))
	if err != nil {
		fmt.Printf("put %s failed\n", key)
	}
}

func delCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "del <key>",
		Short: "del data of key",
		Run:   delCommandFunc,
	}
	return cmd
}

func delCommandFunc(cmd *cobra.Command, args []string) {
	initialGlobal()
	key := args[0]
	err := globalDB.Del([]byte(key))
	if err != nil {
		fmt.Printf("del %s failed\n", key)
	}
}

func getNCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "getn <num>",
		Short: "get num of data from db, default key is start with test_(test_0, test_1 ...)",
		Run:   getNCommandFunc,
	}
	return cmd
}

func getNCommandFunc(cmd *cobra.Command, args []string) {
	initialGlobal()
	num, err := strconv.Atoi(args[0])
	if err != nil {
		num = 10
	}
	globalDB.GetN(num)
}

func putNCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "putn <num>",
		Short: "put data to db, default key is start with test_(test_0, test_1 ...)",
		Run:   putNCommandFunc,
	}
	return cmd
}

func putNCommandFunc(cmd *cobra.Command, args []string) {
	initialGlobal()
	num, err := strconv.Atoi(args[0])
	if err != nil {
		num = 10
	}
	globalDB.PutN(num)
}

func testCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "test <total> <num>",
		Short: "1: init total(MB) data; 2: insert num(MB) data; " +
			"3: query data in step2; 4: query data not in story; 5: update data in step2; 6: delete data in step2",
		Run: testCommandFunc,
	}
	return cmd
}

func testCommandFunc(cmd *cobra.Command, args []string) {
	initialGlobal()
	total, err := strconv.Atoi(args[0])
	if err != nil {
		total = 60
	}
	num, err := strconv.Atoi(args[1])
	if err != nil {
		num = 60
	}
	globalDB.Test(total, num)
}

func initialGlobal() (err error) {
	globalProps = properties.NewProperties()

	if len(propertyFiles) > 0 {
		globalProps = properties.MustLoadFiles(propertyFiles, properties.UTF8, false)
	}

	for _, prop := range propertyvalues {
		seps := strings.SplitN(prop, "=", 2)
		globalProps.Set(seps[0], seps[1])
	}
	globalProps.Set("threads", strconv.Itoa(threads))
	globalProps.Set("keypre", keypre)
	globalProps.Set("vallen", strconv.Itoa(vallen))
	globalProps.Set("initdata", strconv.Itoa(initdata))
	globalProps.Set("valtype", strconv.Itoa(valtype))
	dbCreator := db.GetDBCreator(dbname)
	if dbCreator == nil {
		err = errors.New("GetDBCreator failed")
		return
	}
	if globalDB = dbCreator.Create(globalProps); globalDB != nil {
		err = errors.New("Create db failed")
		return
	}
	return
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rootCmd.AddCommand(getCommand())
	rootCmd.AddCommand(getNCommand())
	rootCmd.AddCommand(putCommand())
	rootCmd.AddCommand(putNCommand())
	rootCmd.AddCommand(delCommand())
	rootCmd.AddCommand(testCommand())

	rootCmd.PersistentFlags().IntVar(&threads, "threads", 10, "thread num")
	rootCmd.PersistentFlags().IntVar(&vallen, "valLen", 1024, "length of value")
	rootCmd.PersistentFlags().StringVar(&keypre, "keyPre", "test", "start char of key")
	rootCmd.PersistentFlags().StringVar(&dbname, "dbname", "tikv", "name of db")
	rootCmd.PersistentFlags().IntVar(&valtype, "valType", 0, "0: repeat data, 1: random data")
	rootCmd.PersistentFlags().IntVar(&initdata, "initData", 0, "0: do not init data, 1: init data")
	rootCmd.PersistentFlags().StringArrayVarP(&propertyvalues, "prop", "p", nil, "specify a property value with name=value")
	rootCmd.PersistentFlags().StringArrayVarP(&propertyFiles, "property_file", "c", nil, "spefify a property file")
	rootCmd.Execute()
}
