// Copyright 2019, 2020 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

import (
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logfmt/logfmt"
	errors "golang.org/x/xerrors"
)

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
	s := P.ConnClass
	if !class {
		s = ""
	}
	q.Add("connectionClass", s)

	q.Add("username", P.Username)
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
	//return quoteRunes(P.Username, "/@") + "/" + quoteRunes(password, "@") + "@" + P.CommonParams.DSN + "\n" + q.String()
	return P.CommonParams.DSN + "\n" + q.String()
}

// ParseConnString parses the given connection string into a struct.
func ParseConnString(connString string) (ConnectionParams, error) {
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
	connString = strings.TrimSpace(connString)
	if i := strings.IndexByte(connString, '\n'); i >= 0 {
		connString, paramsString = strings.TrimSpace(connString[:i]), strings.TrimSpace(connString[i+1:])
	}
	origConnString := connString
	var q url.Values

	if !strings.HasPrefix(connString, "oracle://") {
		up, connString := splitQuoted(connString, '@')
		if connString == "" {
			connString = up
		} else {
			P.Username, P.Password.secret = splitQuoted(up, '/')
			P.Username, P.Password.secret = unquote(P.Username), unquote(P.Password.secret)
		}

		uSid := strings.ToUpper(connString)
		//fmt.Printf("connString=%q SID=%q\n", connString, uSid)
		if strings.Contains(uSid, " AS ") {
			if P.IsSysDBA = strings.HasSuffix(uSid, " AS SYSDBA"); P.IsSysDBA {
				connString = connString[:len(connString)-10]
			} else if P.IsSysOper = strings.HasSuffix(uSid, " AS SYSOPER"); P.IsSysOper {
				connString = connString[:len(connString)-11]
			} else if P.IsSysASM = strings.HasSuffix(uSid, " AS SYSASM"); P.IsSysASM {
				connString = connString[:len(connString)-10]
			}
		}
		P.DSN = connString
		q = make(url.Values, 32)
	} else {
		u, err := url.Parse(connString)
		if err != nil {
			return P, errors.Errorf("%s: %w", connString, err)
		}
		if usr := u.User; usr != nil {
			P.Username = usr.Username()
			P.Password.secret, _ = usr.Password()
		}
		P.DSN = u.Hostname()
		// IPv6 literal address brackets are removed by u.Hostname,
		// so we have to put them back
		if strings.HasPrefix(u.Host, "[") && !strings.Contains(P.DSN[1:], "]") {
			P.DSN = "[" + P.DSN + "]"
		}
		if u.Port() != "" {
			P.DSN += ":" + u.Port()
		}
		if u.Path != "" && u.Path != "/" {
			P.DSN += u.Path
		}
		q = u.Query()
	}
	if paramsString != "" {
		d := logfmt.NewDecoder(strings.NewReader(paramsString))
		for d.ScanRecord() {
			for d.ScanKeyval() {
				if key, value := string(d.Key()), string(d.Value()); key == "onInit" {
					q.Add(key, value)
				} else {
					q.Set(key, value)
				}
			}
		}
		if err := d.Err(); err != nil {
			return P, errors.Errorf("parsing parameters %q: %w", paramsString, err)
		}
		if vv, ok := q["username"]; ok {
			P.Username = vv[0]
		}
		if vv, ok := q["password"]; ok {
			P.Password.secret = vv[0]
		}
	}

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
		} else if off, err := parseTZ(tz); err == nil {
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
		P.OnInit = mkExecMany(P.onInitStmts)
	}
	P.NewPassword.secret = q.Get("newPassword")
	P.ConfigDir = q.Get("configDir")
	P.LibDir = q.Get("libDir")

	P.comb()

	if Log != nil {
		Log("msg", "ParseConnString", "connString", origConnString, "common", P.CommonParams, "connParams", P.ConnParams, "poolParams", P.PoolParams)
	}
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
func (p ParamsArray) String() string {
	var buf strings.Builder
	var n int
	keys := make([]string, 0, len(p.Values))
	for k, vv := range p.Values {
		for _, v := range vv {
			n += len(k) + 1 + len(v) + 1
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	buf.Grow(n)

	enc := logfmt.NewEncoder(&buf)
	var firstErr error
	for _, k := range keys {
		for _, v := range p.Values[k] {
			if err := enc.EncodeKeyval(k, v); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if err := enc.EndRecord(); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr != nil {
		fmt.Fprintf(&buf, "\tERROR: %+v", firstErr)
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
