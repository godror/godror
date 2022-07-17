//go:build ignore
// +build ignore

package main

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

func main() {
	if err := Main(); err != nil {
		log.Fatalf("%+v", err)
	}
}

func Main() error {
	flagForce := flag.Bool("force", false, "force fresh download")
	flag.Parse()

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	var zig string
	if !*flagForce {
		var err error
		if zig, err = findZig("zig"); err != nil {
			log.Println(err)
		} else if err = checkZig(zig); err == nil {
			fmt.Println(filepath.Join(cwd, zig))
			return nil
		}
	}

	configs, err := getDownloadConfigs()
	if err != nil {
		return err
	}
	var max string
	for k := range configs {
		if k != "master" && k != "" && '0' <= k[0] && k[0] <= '9' {
			if max == "" || semver.Compare("v"+k, "v"+max) > 0 {
				max = k
			}
		}
	}
	master := configs[max]
	fn, err := master.Archives[zigKey(runtime.GOOS, runtime.GOARCH)].Download()
	if err != nil {
		return err
	}
	defer os.RemoveAll(filepath.Dir(fn))

	//log.Println("fn:", fn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	err = exec.CommandContext(ctx, "tar", "xaf", fn, "--one-top-level=zig").Run()
	cancel()
	if err != nil {
		return err
	}
	bn := strings.TrimSuffix(filepath.Base(fn), ".tar")
	if i := strings.Index(bn, ".tar."); i >= 0 {
		bn = bn[:i]
	}
	zig = filepath.Join(cwd, "zig", bn, "zig")
	fmt.Println(zig)

	return nil
}
func zigKey(goos, goarch string) string {
	arch := goarch
	switch arch {
	case "amd64":
		arch = "x86_64"
	case "386":
		arch = "i386"
	}
	return arch + "-" + goos
}

func getDownloadConfigs() (map[string]dlConfig, error) {
	resp, err := http.Get("https://ziglang.org/download/index.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	downloads := make(map[string]json.RawMessage, 32)
	if err = json.NewDecoder(resp.Body).Decode(&downloads); err != nil {
		return nil, err
	}
	configs := make(map[string]dlConfig, len(downloads))
	m := make(map[string]json.RawMessage, 16)
	for k, b := range downloads {
		cfg := dlConfig{Archives: make(map[string]Archive)}
		if err = json.Unmarshal(b, &cfg); err != nil {
			return configs, fmt.Errorf("%s: %w", string(b), err)
		}
		if err = json.Unmarshal(b, &m); err != nil {
			return configs, err
		}
		for k, b := range m {
			if k == "src" || strings.IndexByte(k, '-') >= 0 {
				var a Archive
				if err = json.Unmarshal(b, &a); err != nil {
					return configs, err
				}
				cfg.Archives[k] = a
			}
			delete(m, k)
		}
		configs[k] = cfg
	}
	return configs, nil
}

type dlConfig struct {
	Version  string `json:"version"`
	Date     string `json:"date"`
	Docs     string `json:"docs"`
	StdDocs  string `json:"stdDocs"`
	Notes    string `json:"notes"`
	Archives map[string]Archive
}

type Archive struct {
	URL    string `json:"tarball"`
	ShaSum string `json:"shasum"`
	Size   string `json:"size"`
}

func (a Archive) Download() (string, error) {
	var hsh hash.Hash
	switch x := len(a.ShaSum) / 2 * 8; x {
	case 160:
		hsh = sha1.New()
	case 256:
		hsh = sha256.New()
	case 512:
		hsh = sha512.New()
	default:
		return "", fmt.Errorf("unknown hash size %d", x)
	}
	length, err := strconv.ParseInt(a.Size, 10, 64)
	if err != nil {
		return "", err
	}
	resp, err := http.Get(a.URL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	dn, err := os.MkdirTemp("", a.ShaSum)
	if err != nil {
		return "", err
	}
	var success bool
	defer func() {
		if !success {
			os.RemoveAll(dn)
		}
	}()
	fh, err := os.Create(filepath.Join(dn, path.Base(a.URL)))
	if err != nil {
		return "", err
	}
	defer fh.Close()
	log.Printf("Downloading %q to %q.", a.URL, fh.Name())
	if n, err := io.Copy(fh, io.TeeReader(resp.Body, hsh)); err != nil {
		return "", err
	} else if n != length {
		return "", fmt.Errorf("size mismatch: got %d, wanted %d", n, length)
	} else if got := hex.EncodeToString(hsh.Sum(nil)); got != a.ShaSum {
		return "", fmt.Errorf("hash mismatch: got %s, wanted %s", got, a.ShaSum)
	}

	err = fh.Close()
	success = err == nil
	return fh.Name(), err
}

func checkZig(zig string) error {
	if zig == "" {
		return io.EOF
	}
	cmd := exec.Command(zig, "version")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	return cmd.Run()
}

func findZig(root string) (string, error) {
	var zig string
	err := filepath.Walk(
		root,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if zig != "" {
				return filepath.SkipDir
			}
			if info.Name() == "zig" && info.Mode().IsRegular() && info.Mode().Perm()&0111 != 0 {
				zig = path
				return filepath.SkipDir
			}
			return nil
		},
	)
	return zig, err
}
