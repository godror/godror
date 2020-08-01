// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package connstr

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-logfmt/logfmt"
	errors "golang.org/x/xerrors"
)

const (
	// DefaultPoolMinSessions specifies the default value for minSessions for pool creation.
	DefaultPoolMinSessions = 1
	// DefaultPoolMaxSessions specifies the default value for maxSessions for pool creation.
	DefaultPoolMaxSessions = 1000
	// DefaultSessionIncrement specifies the default value for increment for pool creation.
	DefaultSessionIncrement = 1
	// DefaultPoolIncrement is a deprecated name for DefaultSessionIncrement.
	DefaultPoolIncrement = DefaultSessionIncrement
	// DefaultConnectionClass is empty, which allows to use the poolMinSessions created as part of session pool creation for non-DRCP. For DRCP, connectionClass needs to be explicitly mentioned.
	DefaultConnectionClass = ""
	// NoConnectionPoolingConnectionClass is a special connection class name to indicate no connection pooling.
	// It is the same as setting standaloneConnection=1
	NoConnectionPoolingConnectionClass = "NO-CONNECTION-POOLING"
	// DefaultSessionTimeout is the seconds before idle pool sessions get evicted
	DefaultSessionTimeout = 5 * time.Minute
	// DefaultWaitTimeout is the milliseconds to wait for a session to become available
	DefaultWaitTimeout = 30 * time.Second
	// DefaultMaxLifeTime is the maximum time in seconds till a pooled session may exist
	DefaultMaxLifeTime = 1 * time.Hour
	//DefaultStandaloneConnection holds the default for standaloneConnection.
	DefaultStandaloneConnection = false
)

type CommonParams struct {
	Username, ConnectString string
	Password                Password
	ConfigDir, LibDir       string
	OnInit                  func(driver.Conn) error
	Timezone                *time.Location
	EnableEvents            bool
}

func (P CommonParams) String() string {
	q := NewParamsArray(8)
	q.Add("user", P.Username)
	q.Add("password", P.Password.String())
	q.Add("connectString", P.ConnectString)
	if P.ConfigDir != "" {
		q.Add("configDir", P.ConfigDir)
	}
	if P.LibDir != "" {
		q.Add("libDir", P.LibDir)
	}
	s := "local"
	tz := P.Timezone
	if tz != nil && tz != time.Local {
		s = tz.String()
	}
	q.Add("timezone", s)
	if P.EnableEvents {
		q.Add("enableEvents", "1")
	}

	return q.String()
}

type ConnParams struct {
	NewPassword                             Password
	ConnClass                               string
	IsSysDBA, IsSysOper, IsSysASM, IsPrelim bool
	ShardingKey, SuperShardingKey           []interface{}
}

func (P ConnParams) String() string {
	q := NewParamsArray(8)
	if P.ConnClass != "" {
		q.Add("connectionClass", P.ConnClass)
	}
	if !P.NewPassword.IsZero() {
		q.Add("newPassword", P.NewPassword.String())
	}
	if P.IsSysDBA {
		q.Add("sysdba", "1")
	}
	if P.IsSysOper {
		q.Add("sysoper", "1")
	}
	if P.IsSysASM {
		q.Add("sysasm", "1")
	}
	for _, v := range P.ShardingKey {
		q.Add("shardingKey", fmt.Sprintf("%v", v))
	}
	for _, v := range P.SuperShardingKey {
		q.Add("superShardingKey", fmt.Sprintf("%v", v))
	}
	return q.String()
}

type PoolParams struct {
	MinSessions, MaxSessions, SessionIncrement int
	WaitTimeout, MaxLifeTime, SessionTimeout   time.Duration
	Heterogeneous, ExternalAuth                bool
}

func (P PoolParams) String() string {
	q := NewParamsArray(8)
	q.Add("poolMinSessions", strconv.Itoa(P.MinSessions))
	q.Add("poolMaxSessions", strconv.Itoa(P.MaxSessions))
	q.Add("poolIncrement", strconv.Itoa(P.SessionIncrement))
	if P.Heterogeneous {
		q.Add("heterogeneousPool", "1")
	}
	q.Add("poolWaitTimeout", P.WaitTimeout.String())
	q.Add("poolSessionMaxLifetime", P.MaxLifeTime.String())
	q.Add("poolSessionTimeout", P.SessionTimeout.String())
	if P.ExternalAuth {
		q.Add("externalAuth", "1")
	}
	return q.String()
}

// ConnectionParams holds the params for a connection (pool).
// You can use ConnectionParams{...}.StringWithPassword()
// as a connection string in sql.Open.
type ConnectionParams struct {
	CommonParams
	ConnParams
	PoolParams
	// NewPassword is used iff StandaloneConnection is true!
	NewPassword          Password
	onInitStmts          []string
	StandaloneConnection bool
}

func (P ConnectionParams) IsStandalone() bool {
	return P.StandaloneConnection || P.IsSysDBA || P.IsSysOper || P.IsSysASM || P.IsPrelim
}

func (P *ConnectionParams) comb() {
	P.StandaloneConnection = P.StandaloneConnection || P.ConnClass == NoConnectionPoolingConnectionClass
	if P.IsPrelim || P.StandaloneConnection {
		// Prelim: the shared memory may not exist when Oracle is shut down.
		P.ConnClass = ""
		P.Heterogeneous = false
	}
	if !P.IsStandalone() {
		P.NewPassword.Reset()
		// only enable external authentication if we are dealing with a
		// homogeneous pool and no user name/password has been specified
		if P.Username == "" && P.Password.IsZero() && !P.Heterogeneous {
			P.ExternalAuth = true
		}
	}
}

// SetSessionParamOnInit adds an "ALTER SESSION k=v" to the OnInit task list.
func (P *ConnectionParams) SetSessionParamOnInit(k, v string) {
	s := fmt.Sprintf("ALTER SESSION SET %s = q'(%s)'", k, strings.Replace(v, "'", "''", -1))
	P.onInitStmts = append(P.onInitStmts, s)
	if old := P.OnInit; old == nil {
		P.OnInit = MkExecMany(P.onInitStmts)
	} else {
		n := MkExecMany([]string{s})
		P.OnInit = func(conn driver.Conn) error {
			if err := old(conn); err != nil {
				return err
			}
			return n(conn)
		}
	}
}

// String returns the string representation of ConnectionParams.
// The password is replaced with a "SECRET" string!
func (P ConnectionParams) String() string {
	return P.string(true, false)
}

// StringNoClass returns the string representation of ConnectionParams, without class info.
// The password is replaced with a "SECRET" string!
func (P ConnectionParams) StringNoClass() string {
	return P.string(false, false)
}

// StringWithPassword returns the string representation of ConnectionParams (as String() does),
// but does NOT obfuscate the password, just prints it as is.
func (P ConnectionParams) StringWithPassword() string {
	return P.string(true, true)
}

func (P ConnectionParams) string(class, withPassword bool) string {
	q := NewParamsArray(32)
	q.Add("connectString", P.ConnectString)
	s := P.ConnClass
	if !class {
		s = ""
	}
	q.Add("connectionClass", s)

	q.Add("user", P.Username)
	if withPassword {
		q.Add("password", P.Password.Secret())
		q.Add("newPassword", P.NewPassword.Secret())
	} else {
		q.Add("password", P.Password.String())
		if !P.NewPassword.IsZero() {
			q.Add("newPassword", P.NewPassword.String())
		}
	}
	s = "local"
	tz := P.Timezone
	if tz != nil && tz != time.Local {
		s = tz.String()
	}
	q.Add("timezone", s)
	B := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	q.Add("poolMinSessions", strconv.Itoa(P.MinSessions))
	q.Add("poolMaxSessions", strconv.Itoa(P.MaxSessions))
	q.Add("poolIncrement", strconv.Itoa(P.SessionIncrement))
	q.Add("sysdba", B(P.IsSysDBA))
	q.Add("sysoper", B(P.IsSysOper))
	q.Add("sysasm", B(P.IsSysASM))
	q.Add("standaloneConnection", B(P.StandaloneConnection))
	q.Add("enableEvents", B(P.EnableEvents))
	q.Add("heterogeneousPool", B(P.Heterogeneous))
	q.Add("prelim", B(P.IsPrelim))
	q.Add("poolWaitTimeout", P.WaitTimeout.String())
	q.Add("poolSessionMaxLifetime", P.MaxLifeTime.String())
	q.Add("poolSessionTimeout", P.SessionTimeout.String())
	q.Values["onInit"] = P.onInitStmts
	q.Add("configDir", P.ConfigDir)
	q.Add("libDir", P.LibDir)
	//return quoteRunes(P.Username, "/@") + "/" + quoteRunes(password, "@") + "@" + P.CommonParams.ConnectString + "\n" + q.String()

	return q.String()
}

// Parse parses the given connection string into a struct.
func Parse(dataSourceName string) (ConnectionParams, error) {
	P := ConnectionParams{
		StandaloneConnection: DefaultStandaloneConnection,
		CommonParams: CommonParams{
			Timezone: time.Local,
		},
		ConnParams: ConnParams{
			ConnClass: DefaultConnectionClass,
		},
		PoolParams: PoolParams{
			MinSessions:      DefaultPoolMinSessions,
			MaxSessions:      DefaultPoolMaxSessions,
			SessionIncrement: DefaultPoolIncrement,
			MaxLifeTime:      DefaultMaxLifeTime,
			WaitTimeout:      DefaultWaitTimeout,
			SessionTimeout:   DefaultSessionTimeout,
		},
	}

	var paramsString string
	dataSourceName = strings.TrimSpace(dataSourceName)
	var q url.Values

	if strings.HasPrefix(dataSourceName, "oracle://") {
		// URL
		u, err := url.Parse(dataSourceName)
		if err != nil {
			return P, errors.Errorf("%s: %w", dataSourceName, err)
		}
		if usr := u.User; usr != nil {
			P.Username = usr.Username()
			P.Password.secret, _ = usr.Password()
		}
		P.ConnectString = u.Hostname()
		// IPv6 literal address brackets are removed by u.Hostname,
		// so we have to put them back
		if strings.HasPrefix(u.Host, "[") && (len(P.ConnectString) <= 1 || !strings.Contains(P.ConnectString[1:], "]")) {
			P.ConnectString = "[" + P.ConnectString + "]"
		}
		if u.Port() != "" {
			P.ConnectString += ":" + u.Port()
		}
		if u.Path != "" && u.Path != "/" {
			P.ConnectString += u.Path
		}
		q = u.Query()
	} else if isLogfmt(dataSourceName) {
		//fmt.Printf("logfmt=%q\n", dataSourceName)
		paramsString, dataSourceName = dataSourceName, ""
	} else {
		// Not URL, not logfmt-ed - either starts with some old DSN or plain wrong
		if i := strings.IndexByte(dataSourceName, '\n'); i >= 0 {
			// Multiline, start with DSN, then the logfmt-ed parameters
			dataSourceName, paramsString = strings.TrimSpace(dataSourceName[:i]), strings.TrimSpace(dataSourceName[i+1:])
		}
		// Old, or Easy Connect, or anything
		P.Username, P.Password.secret, dataSourceName = parseUserPassw(dataSourceName)
		//fmt.Printf("dsn=%q\n", dataSourceName)
		uSid := strings.ToUpper(dataSourceName)
		//fmt.Printf("dataSourceName=%q SID=%q\n", dataSourceName, uSid)
		if strings.Contains(uSid, " AS ") {
			if P.IsSysDBA = strings.HasSuffix(uSid, " AS SYSDBA"); P.IsSysDBA {
				dataSourceName = dataSourceName[:len(dataSourceName)-10]
			} else if P.IsSysOper = strings.HasSuffix(uSid, " AS SYSOPER"); P.IsSysOper {
				dataSourceName = dataSourceName[:len(dataSourceName)-11]
			} else if P.IsSysASM = strings.HasSuffix(uSid, " AS SYSASM"); P.IsSysASM {
				dataSourceName = dataSourceName[:len(dataSourceName)-10]
			}
		}
		P.ConnectString = dataSourceName
	}

	if paramsString != "" {
		if q == nil {
			q = make(url.Values, 32)
		}
		// Parse the logfmt-formatted parameters string
		d := logfmt.NewDecoder(strings.NewReader(paramsString))
		for d.ScanRecord() {
			for d.ScanKeyval() {
				switch key, value := string(d.Key()), string(d.Value()); key {
				case "connectString":
					//fmt.Printf("connectString=%q\n", value)
					var user, passw string
					if user, passw, P.ConnectString = parseUserPassw(value); P.Username == "" && P.Password.IsZero() {
						P.Username, P.Password.secret = user, passw
					}
				case "user":
					P.Username = value
				case "password":
					P.Password.secret = value
				case "onInit", "shardingKey", "superShardingKey":
					q.Add(key, value)
				default:
					q.Set(key, value)
				}
			}
		}
		if err := d.Err(); err != nil {
			return P, errors.Errorf("parsing parameters %q: %w", paramsString, err)
		}
	}

	// Override everything from the parameters,
	// which can come from the URL values or the logfmt-formatted parameters string.
	if vv, ok := q["connectionClass"]; ok {
		P.ConnClass = vv[0]
	}
	for _, task := range []struct {
		Dest *bool
		Key  string
	}{
		{&P.IsSysDBA, "sysdba"},
		{&P.IsSysOper, "sysoper"},
		{&P.IsSysASM, "sysasm"},
		{&P.IsPrelim, "prelim"},

		{&P.EnableEvents, "enableEvents"},
		{&P.Heterogeneous, "heterogeneousPool"},
		{&P.StandaloneConnection, "standaloneConnection"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		if *task.Dest, err = strconv.ParseBool(s); err != nil {
			return P, errors.Errorf("%s=%q: %w", task.Key, s, err)
		}
		if task.Key == "heterogeneousPool" {
			P.StandaloneConnection = !P.Heterogeneous
		}
	}

	if tz := q.Get("timezone"); tz != "" {
		var err error
		if strings.EqualFold(tz, "local") {
			// P.Timezone = time.Local // already set
		} else if strings.Contains(tz, "/") {
			if P.Timezone, err = time.LoadLocation(tz); err != nil {
				return P, errors.Errorf("%s: %w", tz, err)
			}
		} else if off, err := ParseTZ(tz); err == nil {
			P.Timezone = time.FixedZone(tz, off)
		} else {
			return P, errors.Errorf("%s: %w", tz, err)
		}
	}

	for _, task := range []struct {
		Dest *int
		Key  string
	}{
		{&P.MinSessions, "poolMinSessions"},
		{&P.MaxSessions, "poolMaxSessions"},
		{&P.SessionIncrement, "poolIncrement"},
		{&P.SessionIncrement, "sessionIncrement"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = strconv.Atoi(s)
		if err != nil {
			return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
		}
	}
	for _, task := range []struct {
		Dest *time.Duration
		Key  string
	}{
		{&P.SessionTimeout, "poolSessionTimeout"},
		{&P.WaitTimeout, "poolWaitTimeout"},
		{&P.MaxLifeTime, "poolSessionMaxLifetime"},
	} {
		s := q.Get(task.Key)
		if s == "" {
			continue
		}
		var err error
		*task.Dest, err = time.ParseDuration(s)
		if err != nil {
			if !strings.Contains(err.Error(), "time: missing unit in duration") {
				return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
			}
			i, err := strconv.Atoi(s)
			if err != nil {
				return P, errors.Errorf("%s: %w", task.Key+"="+s, err)
			}
			base := time.Second
			if task.Key == "poolWaitTimeout" {
				base = time.Millisecond
			}
			*task.Dest = time.Duration(i) * base
		}
	}
	if P.MinSessions > P.MaxSessions {
		P.MinSessions = P.MaxSessions
	}
	if P.MinSessions == P.MaxSessions {
		P.SessionIncrement = 0
	} else if P.SessionIncrement < 1 {
		P.SessionIncrement = 1
	}
	if P.onInitStmts = q["onInit"]; len(P.onInitStmts) != 0 {
		P.OnInit = MkExecMany(P.onInitStmts)
	}
	P.ShardingKey = strToIntf(q["shardingKey"])
	P.SuperShardingKey = strToIntf(q["superShardingKey"])

	P.NewPassword.secret = q.Get("newPassword")
	P.ConfigDir = q.Get("configDir")
	P.LibDir = q.Get("libDir")

	P.comb()

	return P, nil
}

// Password is printed obfuscated with String, use Secret to reveal the secret.
type Password struct {
	secret string
}

// NewPassword creates a new Password, containing the given secret.
func NewPassword(secret string) Password { return Password{secret: secret} }

func (P Password) String() string {
	if P.secret == "" {
		return ""
	}
	hsh := fnv.New64()
	io.WriteString(hsh, P.secret)
	return "SECRET-" + base64.URLEncoding.EncodeToString(hsh.Sum(nil))
}
func (P Password) Secret() string { return P.secret }
func (P Password) IsZero() bool   { return P.secret == "" }
func (P Password) Len() int       { return len(P.secret) }
func (P *Password) Reset()        { P.secret = "" }

type ParamsArray struct {
	url.Values
}

func NewParamsArray(cap int) ParamsArray { return ParamsArray{Values: make(url.Values, cap)} }

func (p ParamsArray) WriteTo(w io.Writer) (int64, error) {
	firstKeys := make([]string, 0, len(p.Values))
	keys := make([]string, 0, len(p.Values))
	for k := range p.Values {
		if k == "password" || k == "user" || k == "connectString" {
			firstKeys = append(firstKeys, k)
		} else {
			keys = append(keys, k)
		}
	}
	sort.Strings(firstKeys)
	// reverse
	for i, j := 0, len(firstKeys)-1; i < j; i, j = i+1, j-1 {
		firstKeys[i], firstKeys[j] = firstKeys[j], firstKeys[i]
	}
	sort.Strings(keys)

	cw := &countingWriter{W: w}
	enc := logfmt.NewEncoder(cw)
	var firstErr error
	for _, k := range append(firstKeys, keys...) {
		for _, v := range p.Values[k] {
			if err := enc.EncodeKeyval(k, v); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := enc.EndRecord(); err != nil && firstErr == nil {
		firstErr = err
	}
	return cw.N, firstErr
}

// String returns the values in the params array, logfmt-formatted,
// starting with username and password, then the rest sorted alphabetically.
func (p ParamsArray) String() string {
	var buf strings.Builder
	var n int
	for k, vv := range p.Values {
		for _, v := range vv {
			n += len(k) + 1 + len(v) + 1
		}
	}
	buf.Grow(n)
	if _, err := p.WriteTo(&buf); err != nil {
		fmt.Fprintf(&buf, "\tERROR: %+v", err)
	}
	return buf.String()
}

/*
func quoteRunes(s, runes string) string {
	if !strings.ContainsAny(s, runes) {
		return s
	}
	var buf strings.Builder
	buf.Grow(2 * len(s))
	for _, r := range s {
		if strings.ContainsRune(runes, r) {
			buf.WriteByte('\\')
		}
		buf.WriteRune(r)
	}
	return buf.String()
}
*/
func unquote(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var buf strings.Builder
	buf.Grow(len(s))
	var quoted bool
	for _, r := range s {
		if r == '\\' {
			if !quoted {
				quoted = true
			}
			continue
		}
		buf.WriteRune(r)
		quoted = false
	}
	return buf.String()
}
func splitQuoted(s string, sep rune) (string, string) {
	var off int
	sepLen := len(string([]rune{sep}))
	for {
		i := strings.IndexRune(s[off:], sep)
		if i < 0 {
			return s, ""
		}
		off += i
		if off == 0 || s[off-1] != '\\' {
			return s[:off], s[off+sepLen:]
		}
		off += sepLen
	}
	return s, ""
}

func parseUserPassw(dataSourceName string) (user, passw, connectString string) {
	var up string
	if up, connectString = splitQuoted(dataSourceName, '@'); connectString == "" {
		return "", "", dataSourceName
	}
	user, passw = splitQuoted(up, '/')
	return unquote(user), unquote(passw), connectString
}

type countingWriter struct {
	W io.Writer
	N int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.W.Write(p)
	cw.N += int64(n)
	return n, err
}

func MkExecMany(qrys []string) func(driver.Conn) error {
	return func(conn driver.Conn) error {
		for _, qry := range qrys {
			st, err := conn.Prepare(qry)
			if err != nil {
				return errors.Errorf("%s: %w", qry, err)
			}
			_, err = st.Exec(nil) //lint:ignore SA1019 it's hard to use ExecContext here
			st.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}
}
func ParseTZ(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, io.EOF
	}
	if s == "Z" || s == "UTC" {
		return 0, nil
	}
	var tz int
	var ok bool
	if i := strings.IndexByte(s, ':'); i >= 0 {
		i64, err := strconv.ParseInt(s[i+1:], 10, 6)
		if err != nil {
			return tz, errors.Errorf("%s: %w", s, err)
		}
		tz = int(i64 * 60)
		s = s[:i]
		ok = true
	}
	if !ok {
		if i := strings.IndexByte(s, '/'); i >= 0 {
			targetLoc, err := time.LoadLocation(s)
			if err != nil {
				return tz, errors.Errorf("%s: %w", s, err)
			}

			_, localOffset := time.Now().In(targetLoc).Zone()

			tz = localOffset
			return tz, nil
		}
	}
	i64, err := strconv.ParseInt(s, 10, 5)
	if err != nil {
		return tz, errors.Errorf("%s: %w", s, err)
	}
	if i64 < 0 {
		tz = -tz
	}
	tz += int(i64 * 3600)
	return tz, nil
}

func strToIntf(ss []string) []interface{} {
	n := len(ss)
	if n == 0 {
		return nil
	}
	intf := make([]interface{}, n)
	for i, s := range ss {
		intf[i] = s
	}
	return intf
}
func isLogfmt(s string) bool {
	if s == "" {
		return false
	}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if !strings.Contains(line, "=") {
			return false
		}
	}
	var buf strings.Builder
	e := logfmt.NewEncoder(&buf)
	d := logfmt.NewDecoder(strings.NewReader(s))
	for d.ScanRecord() {
		for d.ScanKeyval() {
			e.EncodeKeyval(d.Key(), d.Value())
		}
	}
	if d.Err() != nil {
		return false
	}
	noSpcQ := func(s string) string {
		return strings.Map(func(r rune) rune {
			if r == '"' || unicode.IsSpace(r) {
				return -1
			}
			return r
		}, s)
	}
	return noSpcQ(s) == noSpcQ(buf.String())
}
