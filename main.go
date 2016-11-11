package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/willf/bloom"
	"gopkg.in/redis.v5"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

var (
	redis_server   *string
	redis_password *string
	redis_key      *string

	index_file        *string
	idlen             *uint
	import_file       *string
	redis_client      *redis.Client
	bfilter           *bloom.BloomFilter
	bfilter_header    *index_file_header
	is_index_not_sync bool
)

const (
	default_id_len  = 20
	interval        = 1 //redis监测间隔
	watermark_low   = 100
	generate_number = 10
	generate_step   = 3
)

func main() {
	flag.Parse()
	command := flag.Arg(0)
	parse_arg(command)

	switch command {
	case "start":
		log.Printf("redis=%s, idlen=%d, key=\"%s\"\n", *redis_server, *idlen, *redis_key)
		do_start_server()
	case "import":
		do_import()
	case "top":
		do_top()
	case "clear-redis":
		do_clear_redis()
	case "has":
		do_has()
	default:
		topic := flag.Arg(1)
		if topic != "" {
			fmt.Fprintf(os.Stderr, "%s %s <options>%s\noptions:\n", os.Args[0], topic, ommand_arg_line_info(topic))
			parse_arg(topic)
			flag.PrintDefaults()
		} else {
			fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
			fmt.Fprintln(os.Stderr, "Commands:")
			fmt.Fprintln(os.Stderr, "   start       - start service")
			fmt.Fprintln(os.Stderr, "   import      - import id list to bloomfilter from a file")
			fmt.Fprintln(os.Stderr, "   top         - get top 10 id in redis")
			fmt.Fprintln(os.Stderr, "   clear-redis - truncate id list in redis")
			fmt.Fprintln(os.Stderr, "   has         - test id in bloomfilter")
			fmt.Fprintln(os.Stderr, "\nMore: guid help <command>")
		}
		if command != "help" {
			os.Exit(1)
		}
	}
}

func ommand_arg_line_info(command string) string {
	switch command {
	case "has":
		return " <test>"
	}
	return ""
}

// functions....

func redis_conn() {
	redis_client = redis.NewClient(&redis.Options{
		Addr:     *redis_server,
		Password: *redis_password,
		DB:       0,
	})
}

func watchloop() {
	for {
		llen := redis_client.LLen(*redis_key)
		if llen.Err() == nil {
			if llen.Val() < watermark_low {
				log.Printf("count(\"%s\")=%d < %d, generate %d ids\n", *redis_key, llen.Val(), watermark_low, generate_number)
				generate_id_list(*redis_key, *idlen, generate_number)
			} else {
				if is_index_not_sync {
					write_index_file()
				}
				time.Sleep(time.Second * interval)
			}
		} else {
			log.Println("redis-error:", llen.Err())
			time.Sleep(time.Second * interval)
		}
	}
}

func write_index_file() {
	log.Println("writing " + *index_file + "...")
	fd, err := os.OpenFile(*index_file+".tmp", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println("write error", *index_file+".tmp", err)
	}
	defer fd.Close()
	fd.Truncate(0)

	buf, _ := json.Marshal(bfilter_header)
	fd.Write(buf)

	buf, err = bfilter.GobEncode()
	if err != nil {
		log.Println("encoding error", err)
	}

	fd.Seek(256, 0)
	n, err := fd.Write(buf)

	if err == nil {
		fd.Sync()
		defer func() {
			os.Remove(*index_file)
			os.Rename(*index_file+".tmp", *index_file)
			is_index_not_sync = false
			log.Println("writing done", n)
		}()
	} else {
		log.Println("write error", *index_file, err)
	}
}

type index_file_header struct {
	FilterN uint
	FilterK uint
}

func load_filter() (err error) {
	fd, err := os.OpenFile(*index_file, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Println(*index_file, err)
		return err
	}

	defer fd.Close()

	buf := make([]byte, 256)
	n, err := fd.Read(buf)

	if err == nil && n == 256 {
		buf := buf[0:bytes.IndexByte(buf, '\x00')]
		bfilter_header = &index_file_header{}
		err = json.Unmarshal(buf, bfilter_header)
	}

	if err != nil {
		log.Println("header decode error", err)
		if idlen == nil || *idlen < 3 {
			log.Fatal("error : idlen must more than 3 ")
		}

		max_items := 20 * math.Min(math.Pow(36, float64(*idlen)), 100000000) //100000000
		bfilter_header = &index_file_header{
			FilterN: uint(max_items),
			FilterK: uint(10),
		}
	}

	log.Printf("bfilter, n=%d, k=%d\n", bfilter_header.FilterN, bfilter_header.FilterK)
	bfilter = bloom.New(bfilter_header.FilterN, bfilter_header.FilterK)

	if err == nil {
		buf, err = ioutil.ReadAll(fd)
		err = bfilter.GobDecode(buf)
		if err != nil {
			log.Println("bfilter decode error", err)
		}
	} else {
		log.Println("loading error", err)
		log.Printf("creating bloomfilter index file - %s\n", *index_file)
	}

	return err
}

func generate_id_list(key string, idlen uint, number int) (err error) {

	var (
		cnt  = 0
		step = generate_step
	)

	for cnt < number {
		if step > (number - cnt) {
			step = number - cnt
		}

		ids := make([]interface{}, 0)
		i := 0
		for i < step {
			new_id := generate_id(int(idlen))
			if is_in_bloomfilter(new_id) == false {
				ids = append(ids, new_id)
				bfilter.AddString(new_id)
				is_index_not_sync = true
				i++
			}
		}

		redis_client.RPush(key, ids...)

		cnt += step
	}

	return nil
}

func is_in_bloomfilter(test string) bool {
	return bfilter.TestString(test)
}

func parse_arg(command string) {
	switch command {
	case "import", "has":
		index_file = flag.String("index", "guid.idx", "bloomfilter index file")
	case "top", "clear-redis", "start":
		redis_server = flag.String("redis", "127.0.0.1:6379", "redis server address")
		redis_password = flag.String("password", "", "redis password")
		redis_key = flag.String("key", "guid-"+strconv.Itoa(default_id_len), "redis id-key")
	}

	if command == "start" {
		idlen = flag.Uint("idlen", default_id_len, "id length.")
		index_file = flag.String("index", "guid.idx", "bloomfilter index file")
	} else if command == "import" {
		import_file = flag.String("file", "", "file to import")
	}
}

// command....

func do_start_server() {
	load_filter()
	redis_conn()

	_, err := redis_client.Ping().Result()
	if err == nil {
		log.Printf("redis connected, starting watchloop for \"%s\"\n", *redis_key)
		watchloop()
	} else {
		log.Fatal("redis", err)
	}
}

func do_top() {
	redis_conn()
	rst, err := redis_client.LRange(*redis_key, 0, 10).Result()
	if err == nil {
		for _, id := range rst {
			fmt.Println(id)
		}
	} else {
		fmt.Println(err)
		os.Exit(1)
	}
}

func do_import() {
	load_filter()
}

func do_clear_redis() {
	redis_conn()
	err := redis_client.LTrim(*redis_key, 0, 0).Err()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func do_has() {
	load_filter()
	word := flag.Arg(1)
	if is_in_bloomfilter(word) {
		fmt.Printf("%s is exists\n", word)
	} else {
		fmt.Printf("%s not found\n", word)
	}
}
