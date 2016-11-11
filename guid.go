package main

import (
	"crypto/md5"
	"encoding/base32"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"
)

var guidre *regexp.Regexp
var global_step uint64

func init() {
	guidre, _ = regexp.Compile("[^a-z0-9A-Z]")
	global_step = 0
}

func generate_id(l int) string {
	h := md5.New()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fmt.Fprintf(h, "%d.%d.%f", global_step, r.Int63(), r.Float64())
	global_step += 1
	str := base32.StdEncoding.EncodeToString(h.Sum(nil))
	str = guidre.ReplaceAllString(str, "")

	n := len(str)
	if n < l {
		str = str + generate_id(l-n)
	} else {
		str = str[0:l]
	}
	return strings.ToLower(str)
}
