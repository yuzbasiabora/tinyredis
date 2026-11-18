// Tinyredis: a Redis-compatible in-memory database server in ONE Go file (standard library only).
//
//	go run main.go                       # listen on :6380
//	go run main.go -addr :6379 -aof data.aof
//	go run main.go -selftest             # run the built-in tests
//
// Speaks the Redis wire protocol (RESP), so redis-cli and client libraries work.
// Supports strings, lists, hashes, sets, key expiry, pipelining and append-only-file persistence.
// MIT License, Copyright (c) 2026.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------- data model

type value struct {
	kind byte // 's' string, 'l' list, 'h' hash, 'e' set
	s    string
	l    []string
	h    map[string]string
	e    map[string]struct{}
	exp  int64 // unix milliseconds, 0 = never
}

type connState struct{ proto int } // 2 = RESP2 (default), 3 = RESP3 after HELLO 3

type Store struct {
	mu     sync.Mutex
	d      map[string]*value
	aof    *os.File
	custom [][]string // commands to log instead of the original (absolute expiry times)
	cs     *connState // protocol state of the connection currently executing
}

func NewStore() *Store { return &Store{d: map[string]*value{}} }

func nowMs() int64 { return time.Now().UnixMilli() }

// get returns the live value for key, deleting it first if it has expired.
func (s *Store) get(k string) *value {
	v := s.d[k]
	if v != nil && v.exp != 0 && v.exp <= nowMs() {
		delete(s.d, k)
		return nil
	}
	return v
}

// sweep removes a sample of expired keys; run regularly in the background.
func (s *Store) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k := range s.d {
		s.get(k)
		if n++; n >= 500 {
			break
		}
	}
}

// ---------------------------------------------------------------- RESP encoding

const (
	rOK   = "+OK\r\n"
	rNull = "$-1\r\n"
)

func rErr(m string) string  { return "-" + m + "\r\n" }
func rInt(n int64) string   { return ":" + strconv.FormatInt(n, 10) + "\r\n" }
func rBulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
func rArr(items []string) string { // items are already-encoded replies
	return "*" + strconv.Itoa(len(items)) + "\r\n" + strings.Join(items, "")
}
func rBulks(xs []string) string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = rBulk(x)
	}
	return rArr(out)
}

const (
	errWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errInt       = "ERR value is not an integer or out of range"
	errSyntax    = "ERR syntax error"
)

func errArgs(cmd string) string {
	return rErr("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
}

// readCommand parses one command: a RESP array of bulk strings, or an inline line such as "PING".
func readCommand(r *bufio.Reader) ([]string, error) {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		if line[0] != '*' {
			return strings.Fields(line), nil
		}
		n, err := strconv.Atoi(line[1:])
		if err != nil || n < 0 || n > 1<<20 {
			return nil, fmt.Errorf("bad array length")
		}
		args := make([]string, 0, n)
		for i := 0; i < n; i++ {
			h, err := r.ReadString('\n')
			if err != nil {
				return nil, err
			}
			h = strings.TrimRight(h, "\r\n")
			if len(h) < 2 || h[0] != '$' {
				return nil, fmt.Errorf("expected bulk string")
			}
			size, err := strconv.Atoi(h[1:])
			if err != nil || size < 0 || size > 512<<20 {
				return nil, fmt.Errorf("bad bulk length")
			}
			buf := make([]byte, size+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			args = append(args, string(buf[:size]))
		}
		return args, nil
	}
}

// ---------------------------------------------------------------- glob matching (KEYS)

func glob(p, s string) bool {
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 0 && p[0] == '*' {
				p = p[1:]
			}
			if p == "" {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if glob(p, s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if s == "" {
				return false
			}
		default:
			if s == "" || s[0] != p[0] {
				return false
			}
		}
		p, s = p[1:], s[1:]
	}
	return s == ""
}

// ---------------------------------------------------------------- commands

var writeCmds = map[string]bool{"SET": true, "DEL": true, "INCR": true, "DECR": true, "INCRBY": true, "DECRBY": true, "APPEND": true, "MSET": true,
	"EXPIRE": true, "PEXPIREAT": true, "PERSIST": true, "FLUSHALL": true, "LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true,
	"HSET": true, "HDEL": true, "SADD": true, "SREM": true}

// Exec runs one command and returns the encoded reply. replay=true means "loading the AOF": nothing is logged.
func (s *Store) Exec(args []string, replay bool, cs *connState) string {
	if len(args) == 0 {
		return rErr("ERR empty command")
	}
	cmd := strings.ToUpper(args[0])
	s.mu.Lock()
	defer s.mu.Unlock()
	s.custom = nil
	if cs == nil {
		cs = &connState{proto: 2}
	}
	s.cs = cs
	reply := s.run(cmd, args)
	if s.aof != nil && !replay && writeCmds[cmd] && reply[0] != '-' {
		entries := s.custom
		if entries == nil {
			entries = [][]string{args}
		}
		for _, e := range entries {
			s.aof.WriteString(rBulks(e))
		}
	}
	return reply
}

// nul is the null reply: "$-1" in RESP2, "_" in RESP3.
func (s *Store) nul() string {
	if s.cs.proto == 3 {
		return "_\r\n"
	}
	return rNull
}

// agg encodes a collection of bulk strings: an array, or in RESP3 a map ('%') or set ('~').
func (s *Store) agg(kind byte, items []string) string {
	if s.cs.proto != 3 || kind == '*' {
		return rBulks(items)
	}
	count := len(items)
	if kind == '%' {
		count /= 2
	}
	return string(kind) + strconv.Itoa(count) + "\r\n" + strings.Join(bulkEach(items), "")
}

func bulkEach(xs []string) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = rBulk(x)
	}
	return out
}

func (s *Store) typed(k string, kind byte) (*value, string) {
	v := s.get(k)
	if v != nil && v.kind != kind {
		return nil, rErr(errWrongType)
	}
	return v, ""
}

func (s *Store) setExpiry(k string, at int64) {
	s.d[k].exp = at
	s.custom = [][]string{{"PEXPIREAT", k, strconv.FormatInt(at, 10)}}
}

func (s *Store) run(cmd string, a []string) string {
	n := len(a)
	need := func(min int) bool { return n >= min }
	switch cmd {
	case "PING":
		if n > 1 {
			return rBulk(a[1])
		}
		return "+PONG\r\n"
	case "ECHO":
		if n != 2 {
			return errArgs(cmd)
		}
		return rBulk(a[1])
	case "QUIT", "SELECT", "CLIENT":
		return rOK
	case "HELLO": // connection handshake used by modern clients: switches between RESP2 and RESP3
		if n >= 2 {
			if a[1] != "2" && a[1] != "3" {
				return "-NOPROTO unsupported protocol version\r\n"
			}
			s.cs.proto = int(a[1][0] - '0')
		}
		pairs := []string{rBulk("server"), rBulk("tinyredis"), rBulk("version"), rBulk("1.0.0"), rBulk("proto"), rInt(int64(s.cs.proto)), rBulk("id"), rInt(1),
			rBulk("mode"), rBulk("standalone"), rBulk("role"), rBulk("master"), rBulk("modules"), "*0\r\n"}
		if s.cs.proto == 3 {
			return "%7\r\n" + strings.Join(pairs, "")
		}
		return rArr(pairs)
	case "COMMAND", "CONFIG":
		return "*0\r\n"
	case "INFO":
		return rBulk(fmt.Sprintf("# Server\r\ntinyredis_version:1.0\r\n# Keyspace\r\nkeys:%d\r\n", len(s.d)))
	case "DBSIZE":
		for k := range s.d {
			s.get(k)
		}
		return rInt(int64(len(s.d)))
	case "FLUSHALL", "FLUSHDB":
		s.d = map[string]*value{}
		return rOK

	// ---- strings
	case "SET":
		if !need(3) {
			return errArgs(cmd)
		}
		var exp int64
		nx, xx := false, false
		for i := 3; i < n; i++ {
			switch strings.ToUpper(a[i]) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			case "EX", "PX":
				if i+1 >= n {
					return rErr(errSyntax)
				}
				t, err := strconv.ParseInt(a[i+1], 10, 64)
				if err != nil || t <= 0 {
					return rErr("ERR invalid expire time in 'set' command")
				}
				if strings.ToUpper(a[i]) == "EX" {
					t *= 1000
				}
				exp = nowMs() + t
				i++
			default:
				return rErr(errSyntax)
			}
		}
		old := s.get(a[1])
		if (nx && old != nil) || (xx && old == nil) {
			return s.nul()
		}
		s.d[a[1]] = &value{kind: 's', s: a[2]}
		if exp != 0 {
			s.d[a[1]].exp = exp
			s.custom = [][]string{{"SET", a[1], a[2]}, {"PEXPIREAT", a[1], strconv.FormatInt(exp, 10)}}
		}
		return rOK
	case "GET":
		if n != 2 {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 's')
		if e != "" {
			return e
		}
		if v == nil {
			return s.nul()
		}
		return rBulk(v.s)
	case "MSET":
		if n < 3 || n%2 == 0 {
			return errArgs(cmd)
		}
		for i := 1; i < n; i += 2 {
			s.d[a[i]] = &value{kind: 's', s: a[i+1]}
		}
		return rOK
	case "MGET":
		out := []string{}
		for _, k := range a[1:] {
			if v := s.get(k); v != nil && v.kind == 's' {
				out = append(out, rBulk(v.s))
			} else {
				out = append(out, s.nul())
			}
		}
		return rArr(out)
	case "APPEND", "STRLEN":
		if n < 2 {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 's')
		if e != "" {
			return e
		}
		if cmd == "STRLEN" {
			if v == nil {
				return rInt(0)
			}
			return rInt(int64(len(v.s)))
		}
		if n != 3 {
			return errArgs(cmd)
		}
		if v == nil {
			v = &value{kind: 's'}
			s.d[a[1]] = v
		}
		v.s += a[2]
		return rInt(int64(len(v.s)))
	case "INCR", "DECR", "INCRBY", "DECRBY":
		by := int64(1)
		if cmd == "INCRBY" || cmd == "DECRBY" {
			if n != 3 {
				return errArgs(cmd)
			}
			var err error
			if by, err = strconv.ParseInt(a[2], 10, 64); err != nil {
				return rErr(errInt)
			}
		} else if n != 2 {
			return errArgs(cmd)
		}
		if cmd == "DECR" || cmd == "DECRBY" {
			by = -by
		}
		v, e := s.typed(a[1], 's')
		if e != "" {
			return e
		}
		cur := int64(0)
		if v != nil {
			var err error
			if cur, err = strconv.ParseInt(v.s, 10, 64); err != nil {
				return rErr(errInt)
			}
		}
		if (by > 0 && cur > 1<<63-1-by) || (by < 0 && cur < -1<<63-by) {
			return rErr("ERR increment or decrement would overflow")
		}
		cur += by
		if v == nil {
			s.d[a[1]] = &value{kind: 's', s: strconv.FormatInt(cur, 10)}
		} else {
			v.s = strconv.FormatInt(cur, 10)
		}
		return rInt(cur)

	// ---- keys and expiry
	case "DEL", "EXISTS":
		c := int64(0)
		for _, k := range a[1:] {
			if s.get(k) != nil {
				c++
				if cmd == "DEL" {
					delete(s.d, k)
				}
			}
		}
		return rInt(c)
	case "TYPE":
		if n != 2 {
			return errArgs(cmd)
		}
		v := s.get(a[1])
		if v == nil {
			return "+none\r\n"
		}
		return "+" + map[byte]string{'s': "string", 'l': "list", 'h': "hash", 'e': "set"}[v.kind] + "\r\n"
	case "KEYS":
		if n != 2 {
			return errArgs(cmd)
		}
		out := []string{}
		for k := range s.d {
			if s.get(k) != nil && glob(a[1], k) {
				out = append(out, k)
			}
		}
		sort.Strings(out)
		return rBulks(out)
	case "EXPIRE", "PEXPIREAT":
		if n != 3 {
			return errArgs(cmd)
		}
		t, err := strconv.ParseInt(a[2], 10, 64)
		if err != nil {
			return rErr(errInt)
		}
		if s.get(a[1]) == nil {
			return rInt(0)
		}
		if cmd == "EXPIRE" {
			t = nowMs() + t*1000
		}
		s.setExpiry(a[1], t)
		return rInt(1)
	case "PERSIST":
		if n != 2 {
			return errArgs(cmd)
		}
		if v := s.get(a[1]); v != nil && v.exp != 0 {
			v.exp = 0
			return rInt(1)
		}
		return rInt(0)
	case "TTL", "PTTL":
		if n != 2 {
			return errArgs(cmd)
		}
		v := s.get(a[1])
		if v == nil {
			return rInt(-2)
		}
		if v.exp == 0 {
			return rInt(-1)
		}
		if cmd == "PTTL" {
			return rInt(v.exp - nowMs())
		}
		return rInt((v.exp - nowMs() + 500) / 1000)

	// ---- lists
	case "LPUSH", "RPUSH":
		if !need(3) {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 'l')
		if e != "" {
			return e
		}
		if v == nil {
			v = &value{kind: 'l'}
			s.d[a[1]] = v
		}
		for _, x := range a[2:] {
			if cmd == "LPUSH" {
				v.l = append([]string{x}, v.l...)
			} else {
				v.l = append(v.l, x)
			}
		}
		return rInt(int64(len(v.l)))
	case "LPOP", "RPOP":
		if n != 2 {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 'l')
		if e != "" {
			return e
		}
		if v == nil || len(v.l) == 0 {
			return s.nul()
		}
		var x string
		if cmd == "LPOP" {
			x, v.l = v.l[0], v.l[1:]
		} else {
			x, v.l = v.l[len(v.l)-1], v.l[:len(v.l)-1]
		}
		if len(v.l) == 0 {
			delete(s.d, a[1])
		}
		return rBulk(x)
	case "LLEN":
		if n != 2 {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 'l')
		if e != "" {
			return e
		}
		if v == nil {
			return rInt(0)
		}
		return rInt(int64(len(v.l)))
	case "LRANGE":
		if n != 4 {
			return errArgs(cmd)
		}
		lo, err1 := strconv.Atoi(a[2])
		hi, err2 := strconv.Atoi(a[3])
		if err1 != nil || err2 != nil {
			return rErr(errInt)
		}
		v, e := s.typed(a[1], 'l')
		if e != "" {
			return e
		}
		if v == nil {
			return "*0\r\n"
		}
		if lo < 0 {
			lo += len(v.l)
		}
		if hi < 0 {
			hi += len(v.l)
		}
		lo, hi = max(lo, 0), min(hi, len(v.l)-1)
		if lo > hi {
			return "*0\r\n"
		}
		return rBulks(v.l[lo : hi+1])

	// ---- hashes
	case "HSET":
		if n < 4 || n%2 != 0 {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 'h')
		if e != "" {
			return e
		}
		if v == nil {
			v = &value{kind: 'h', h: map[string]string{}}
			s.d[a[1]] = v
		}
		added := int64(0)
		for i := 2; i < n; i += 2 {
			if _, ok := v.h[a[i]]; !ok {
				added++
			}
			v.h[a[i]] = a[i+1]
		}
		return rInt(added)
	case "HGET", "HDEL", "HLEN", "HGETALL":
		v, e := s.typed(a[1], 'h')
		if e != "" {
			return e
		}
		switch cmd {
		case "HGET":
			if n != 3 {
				return errArgs(cmd)
			}
			if v != nil {
				if x, ok := v.h[a[2]]; ok {
					return rBulk(x)
				}
			}
			return s.nul()
		case "HDEL":
			c := int64(0)
			for _, f := range a[2:] {
				if v != nil {
					if _, ok := v.h[f]; ok {
						delete(v.h, f)
						c++
					}
				}
			}
			if v != nil && len(v.h) == 0 {
				delete(s.d, a[1])
			}
			return rInt(c)
		case "HLEN":
			if v == nil {
				return rInt(0)
			}
			return rInt(int64(len(v.h)))
		default:
			if v == nil {
				return "*0\r\n"
			}
			keys := make([]string, 0, len(v.h))
			for f := range v.h {
				keys = append(keys, f)
			}
			sort.Strings(keys)
			out := []string{}
			for _, f := range keys {
				out = append(out, f, v.h[f])
			}
			return s.agg('%', out)
		}

	// ---- sets
	case "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD":
		if n < 2 {
			return errArgs(cmd)
		}
		v, e := s.typed(a[1], 'e')
		if e != "" {
			return e
		}
		switch cmd {
		case "SADD":
			if n < 3 {
				return errArgs(cmd)
			}
			if v == nil {
				v = &value{kind: 'e', e: map[string]struct{}{}}
				s.d[a[1]] = v
			}
			c := int64(0)
			for _, m := range a[2:] {
				if _, ok := v.e[m]; !ok {
					v.e[m] = struct{}{}
					c++
				}
			}
			return rInt(c)
		case "SREM":
			c := int64(0)
			for _, m := range a[2:] {
				if v != nil {
					if _, ok := v.e[m]; ok {
						delete(v.e, m)
						c++
					}
				}
			}
			if v != nil && len(v.e) == 0 {
				delete(s.d, a[1])
			}
			return rInt(c)
		case "SISMEMBER":
			if n != 3 {
				return errArgs(cmd)
			}
			if v != nil {
				if _, ok := v.e[a[2]]; ok {
					return rInt(1)
				}
			}
			return rInt(0)
		case "SCARD":
			if v == nil {
				return rInt(0)
			}
			return rInt(int64(len(v.e)))
		default:
			out := []string{}
			if v != nil {
				for m := range v.e {
					out = append(out, m)
				}
				sort.Strings(out)
			}
			return s.agg('~', out)
		}
	}
	return rErr("ERR unknown command '" + a[0] + "'")
}

// ---------------------------------------------------------------- persistence (append-only file)

// OpenAOF replays the log into the store, then keeps appending new write commands to it.
func (s *Store) OpenAOF(path string) (int, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	r := bufio.NewReader(f)
	count := 0
	for {
		args, err := readCommand(r)
		if err != nil {
			break // io.EOF, or a torn final record after a crash: ignore the tail
		}
		s.Exec(args, true, nil)
		count++
	}
	s.aof = f
	return count, nil
}

// ---------------------------------------------------------------- server

type Server struct {
	st   *Store
	ln   net.Listener
	wg   sync.WaitGroup
	conn sync.Map
}

func Listen(addr string, st *Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{st: st, ln: ln}, nil
}

func (sv *Server) Serve() {
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			sv.st.sweep()
		}
	}()
	for {
		c, err := sv.ln.Accept()
		if err != nil {
			return
		}
		sv.conn.Store(c, true)
		sv.wg.Add(1)
		go sv.handle(c)
	}
}

func (sv *Server) Close() {
	sv.ln.Close()
	sv.conn.Range(func(c, _ any) bool { c.(net.Conn).Close(); return true })
	sv.wg.Wait()
	sv.st.mu.Lock()
	if sv.st.aof != nil {
		sv.st.aof.Sync()
		sv.st.aof.Close()
		sv.st.aof = nil
	}
	sv.st.mu.Unlock()
}

func (sv *Server) handle(c net.Conn) {
	defer sv.wg.Done()
	defer c.Close()
	defer sv.conn.Delete(c)
	r, w := bufio.NewReader(c), bufio.NewWriter(c)
	cs := &connState{proto: 2}
	for {
		args, err := readCommand(r)
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "closed") {
				w.WriteString(rErr("ERR protocol error: " + err.Error()))
				w.Flush()
			}
			return
		}
		w.WriteString(sv.st.Exec(args, false, cs))
		if strings.EqualFold(args[0], "QUIT") {
			w.Flush()
			return
		}
		if r.Buffered() == 0 { // flush once per pipelined batch
			if w.Flush() != nil {
				return
			}
		}
	}
}

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	aof := flag.String("aof", "", "append-only file for persistence (empty = memory only)")
	self := flag.Bool("selftest", false, "run the built-in tests and exit")
	flag.Parse()
	if *self {
		os.Exit(selfTest())
	}
	st := NewStore()
	if *aof != "" {
		n, err := st.OpenAOF(*aof)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cannot open AOF:", err)
			os.Exit(1)
		}
		fmt.Printf("loaded %d commands from %s\n", n, *aof)
	}
	sv, err := Listen(*addr, st)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Tinyredis listening on %s\n", sv.ln.Addr())
	sv.Serve()
}

// ---------------------------------------------------------------- self-test (go run main.go -selftest)

type client struct {
	c net.Conn
	r *bufio.Reader
}

func dial(addr string) *client {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	return &client{c, bufio.NewReader(c)}
}

// reply reads one complete RESP reply and returns it as raw text.
func (c *client) reply() string {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "<" + err.Error() + ">"
	}
	switch line[0] {
	case '$':
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		if n < 0 {
			return line
		}
		buf := make([]byte, n+2)
		io.ReadFull(c.r, buf)
		return line + string(buf)
	case '*', '~', '%':
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		if line[0] == '%' {
			n *= 2
		}
		out := line
		for i := 0; i < n; i++ {
			out += c.reply()
		}
		return out
	}
	return line
}

func (c *client) do(args ...string) string {
	c.c.Write([]byte(rBulks(args)))
	return c.reply()
}

func selfTest() int {
	passed, failed := 0, 0
	check := func(name string, ok bool, detail ...any) {
		if ok {
			passed++
			return
		}
		failed++
		fmt.Println("  FAIL:", name, fmt.Sprint(detail...))
	}
	st := NewStore()
	sv, _ := Listen("127.0.0.1:0", st)
	go sv.Serve()
	addr := sv.ln.Addr().String()
	c := dial(addr)

	type tc struct {
		args []string
		want string
	}
	sections := []struct {
		name  string
		cases []tc
	}{
		{"basics", []tc{
			{[]string{"PING"}, "+PONG\r\n"}, {[]string{"ECHO", "hi"}, rBulk("hi")}, {[]string{"NOPE"}, rErr("ERR unknown command 'NOPE'")},
			{[]string{"GET"}, errArgs("get")}, {[]string{"get", "missing"}, rNull}}},
		{"strings", []tc{
			{[]string{"SET", "k", "v"}, rOK}, {[]string{"GET", "k"}, rBulk("v")}, {[]string{"SET", "k", "v2", "NX"}, rNull}, {[]string{"SET", "new", "1", "XX"}, rNull},
			{[]string{"SET", "k", "v3", "XX"}, rOK}, {[]string{"APPEND", "k", "!!"}, rInt(4)}, {[]string{"STRLEN", "k"}, rInt(4)}, {[]string{"GET", "k"}, rBulk("v3!!")},
			{[]string{"INCR", "n"}, rInt(1)}, {[]string{"INCRBY", "n", "41"}, rInt(42)}, {[]string{"DECR", "n"}, rInt(41)}, {[]string{"INCR", "k"}, rErr(errInt)},
			{[]string{"SET", "big", "9223372036854775807"}, rOK}, {[]string{"INCR", "big"}, rErr("ERR increment or decrement would overflow")},
			{[]string{"MSET", "a", "1", "b", "2"}, rOK}, {[]string{"MGET", "a", "zz", "b"}, rArr([]string{rBulk("1"), rNull, rBulk("2")})},
			{[]string{"EXISTS", "a", "b", "zz"}, rInt(2)}, {[]string{"DEL", "a", "b", "zz"}, rInt(2)}, {[]string{"TYPE", "k"}, "+string\r\n"}, {[]string{"TYPE", "zz"}, "+none\r\n"}}},
		{"lists", []tc{
			{[]string{"RPUSH", "L", "1", "2", "3"}, rInt(3)}, {[]string{"LPUSH", "L", "0"}, rInt(4)}, {[]string{"LRANGE", "L", "0", "-1"}, rBulks([]string{"0", "1", "2", "3"})},
			{[]string{"LRANGE", "L", "-2", "-1"}, rBulks([]string{"2", "3"})}, {[]string{"LRANGE", "L", "5", "9"}, "*0\r\n"}, {[]string{"LPOP", "L"}, rBulk("0")}, {[]string{"RPOP", "L"}, rBulk("3")},
			{[]string{"LLEN", "L"}, rInt(2)}, {[]string{"GET", "L"}, rErr(errWrongType)}, {[]string{"LPOP", "L"}, rBulk("1")}, {[]string{"LPOP", "L"}, rBulk("2")},
			{[]string{"LPOP", "L"}, rNull}, {[]string{"EXISTS", "L"}, rInt(0)}, {[]string{"LPUSH", "L2", "a", "b", "c"}, rInt(3)}, {[]string{"LRANGE", "L2", "0", "-1"}, rBulks([]string{"c", "b", "a"})}}},
		{"hashes", []tc{
			{[]string{"HSET", "h", "f1", "v1", "f2", "v2"}, rInt(2)}, {[]string{"HSET", "h", "f1"}, errArgs("hset")}, {[]string{"HSET", "h", "f1", "x"}, rInt(0)}, {[]string{"HSET", "h", "f1", "new", "f3", "v3"}, rInt(1)},
			{[]string{"HGET", "h", "f1"}, rBulk("new")}, {[]string{"HGET", "h", "zz"}, rNull}, {[]string{"HLEN", "h"}, rInt(3)},
			{[]string{"HGETALL", "h"}, rBulks([]string{"f1", "new", "f2", "v2", "f3", "v3"})}, {[]string{"HDEL", "h", "f1", "f2", "nope"}, rInt(2)}, {[]string{"HDEL", "h", "f3"}, rInt(1)}, {[]string{"EXISTS", "h"}, rInt(0)}}},
		{"sets", []tc{
			{[]string{"SADD", "S", "a", "b", "a"}, rInt(2)}, {[]string{"SISMEMBER", "S", "a"}, rInt(1)}, {[]string{"SISMEMBER", "S", "z"}, rInt(0)},
			{[]string{"SMEMBERS", "S"}, rBulks([]string{"a", "b"})}, {[]string{"SCARD", "S"}, rInt(2)}, {[]string{"SREM", "S", "a", "b"}, rInt(2)}, {[]string{"EXISTS", "S"}, rInt(0)}}},
		{"keys", []tc{
			{[]string{"FLUSHALL"}, rOK}, {[]string{"MSET", "user:1", "x", "user:2", "x", "order:1", "x"}, rOK},
			{[]string{"KEYS", "user:*"}, rBulks([]string{"user:1", "user:2"})}, {[]string{"KEYS", "*:1"}, rBulks([]string{"order:1", "user:1"})},
			{[]string{"KEYS", "?ser:?"}, rBulks([]string{"user:1", "user:2"})}, {[]string{"KEYS", "*"}, rBulks([]string{"order:1", "user:1", "user:2"})}, {[]string{"DBSIZE"}, rInt(3)}}},
	}
	for _, sec := range sections {
		bad := 0
		for _, t := range sec.cases {
			if got := c.do(t.args...); got != t.want {
				bad++
				failed++
				fmt.Printf("  FAIL: %v\n    want %q\n    got  %q\n", t.args, t.want, got)
			} else {
				passed++
			}
		}
		fmt.Printf("  %-8s %2d commands %s\n", sec.name, len(sec.cases), map[bool]string{true: "ok", false: "FAILED"}[bad == 0])
	}

	fmt.Println("  expiry")
	c.do("SET", "e", "v", "PX", "60")
	p := c.do("PTTL", "e")
	check("PTTL counts down", p[0] == ':' && p != ":-1\r\n" && p != ":-2\r\n", p)
	time.Sleep(110 * time.Millisecond)
	check("key vanishes after PX", c.do("GET", "e") == rNull)
	c.do("SET", "t", "v", "EX", "10")
	check("TTL after SET EX", c.do("TTL", "t") == rInt(10))
	check("PERSIST removes expiry", c.do("PERSIST", "t") == rInt(1) && c.do("TTL", "t") == rInt(-1))
	check("EXPIRE sets expiry", c.do("EXPIRE", "t", "100") == rInt(1) && c.do("TTL", "t") == rInt(100))
	check("TTL of missing key", c.do("TTL", "ghost") == rInt(-2))
	c.do("SET", "s", "v", "PX", "30")
	time.Sleep(250 * time.Millisecond)
	st.mu.Lock()
	_, still := st.d["s"]
	st.mu.Unlock()
	check("background sweeper evicts untouched expired keys", !still)

	fmt.Println("  protocol")
	h2 := c.do("HELLO", "2")
	check("HELLO 2 handshake", strings.HasPrefix(h2, "*14\r\n") && strings.Contains(h2, "tinyredis"))
	check("unknown protocol version is refused", strings.HasPrefix(c.do("HELLO", "9"), "-NOPROTO"))
	c3 := dial(addr)
	c3.do("HSET", "r3h", "a", "1")
	c3.do("SADD", "r3s", "m")
	c3.do("MSET", "r3k", "v")
	check("HELLO 3 switches to RESP3", strings.HasPrefix(c3.do("HELLO", "3"), "%7\r\n"))
	check("RESP3: null is '_'", c3.do("GET", "nothing") == "_\r\n")
	check("RESP3: HGETALL is a map", c3.do("HGETALL", "r3h") == "%1\r\n"+rBulk("a")+rBulk("1"))
	check("RESP3: SMEMBERS is a set", c3.do("SMEMBERS", "r3s") == "~1\r\n"+rBulk("m"))
	check("RESP3: other replies unchanged, nulls inside arrays too", c3.do("MGET", "r3k", "zz") == "*2\r\n"+rBulk("v")+"_\r\n")
	check("RESP2 connections are unaffected", c.do("GET", "nothing") == rNull && strings.HasPrefix(c.do("HGETALL", "r3h"), "*2\r\n"))
	check("CLIENT SETINFO accepted", c.do("CLIENT", "SETINFO", "LIB-NAME", "x") == rOK)
	c.c.Write([]byte("PING\r\n"))
	check("inline commands", c.reply() == "+PONG\r\n")
	c.c.Write([]byte(rBulks([]string{"SET", "p", "1"}) + rBulks([]string{"INCR", "p"}) + rBulks([]string{"GET", "p"})))
	check("pipelining (3 commands, one write)", c.reply() == rOK && c.reply() == rInt(2) && c.reply() == rBulk("2"))
	big := strings.Repeat("x", 1<<20)
	c.do("SET", "big1m", big)
	check("1 MB value round-trips", c.do("GET", "big1m") == rBulk(big))
	check("value containing CRLF and binary bytes", c.do("SET", "bin", "a\r\n\x00b") == rOK && c.do("GET", "bin") == rBulk("a\r\n\x00b"))
	garbage := dial(addr)
	garbage.c.Write([]byte("*abc\r\n"))
	check("protocol error is reported, server survives", strings.HasPrefix(garbage.reply(), "-ERR protocol error") && c.do("PING") == "+PONG\r\n")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := dial(addr)
			defer cl.c.Close()
			for j := 0; j < 100; j++ {
				cl.do("INCR", "counter")
			}
		}()
	}
	wg.Wait()
	check("20 clients x 100 INCR = 2000 (no lost updates)", c.do("GET", "counter") == rBulk("2000"))
	q := dial(addr)
	check("QUIT closes the connection", q.do("QUIT") == rOK && strings.HasPrefix(q.reply(), "<"))
	sv.Close()

	fmt.Println("  persistence (append-only file)")
	f, _ := os.CreateTemp("", "tinyredis-*.aof")
	f.Close()
	defer os.Remove(f.Name())
	st1 := NewStore()
	st1.OpenAOF(f.Name())
	s1, _ := Listen("127.0.0.1:0", st1)
	go s1.Serve()
	c1 := dial(s1.ln.Addr().String())
	c1.do("SET", "name", "ada")
	c1.do("INCRBY", "visits", "7")
	c1.do("RPUSH", "queue", "a", "b", "c")
	c1.do("LPOP", "queue")
	c1.do("HSET", "user", "id", "1", "role", "admin")
	c1.do("SADD", "tags", "x", "y")
	c1.do("SET", "long", "alive", "EX", "3600")
	c1.do("SET", "short", "gone", "PX", "40")
	c1.do("SET", "deleted", "x")
	c1.do("DEL", "deleted")
	time.Sleep(80 * time.Millisecond)
	s1.Close()
	fh, _ := os.OpenFile(f.Name(), os.O_APPEND|os.O_WRONLY, 0o644)
	fh.WriteString("*3\r\n$3\r\nSET\r\n$4\r\ntorn\r\n$10\r\nabc") // a half-written record, as after a crash
	fh.Close()
	st2 := NewStore()
	n, err := st2.OpenAOF(f.Name())
	check("log replays without error", err == nil && n > 0, err)
	s2, _ := Listen("127.0.0.1:0", st2)
	go s2.Serve()
	c2 := dial(s2.ln.Addr().String())
	check("strings survive a restart", c2.do("GET", "name") == rBulk("ada") && c2.do("GET", "visits") == rBulk("7"))
	check("lists, hashes and sets survive", c2.do("LRANGE", "queue", "0", "-1") == rBulks([]string{"b", "c"}) && c2.do("HGET", "user", "role") == rBulk("admin") && c2.do("SCARD", "tags") == rInt(2))
	check("long TTL survives, with the right deadline", func() bool { t := c2.do("TTL", "long"); return t != rInt(-1) && t != rInt(-2) }())
	check("expired key stays expired", c2.do("GET", "short") == rNull)
	check("deleted key stays deleted", c2.do("EXISTS", "deleted") == rInt(0))
	check("torn final record is ignored", c2.do("EXISTS", "torn") == rInt(0))
	s2.Close()

	fmt.Printf("%d/%d checks passed\n", passed, passed+failed)
	if failed > 0 {
		return 1
	}
	return 0
}
