// Copyright 2021, 2022 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
	//"github.com/kortschak/utter"
)

func main() {
	if err := Main(); err != nil {
		log.SetOutput(os.Stderr)
		log.Fatalf("%+v", err)
	}
}

type goList struct {
	CgoFiles []string
}

func Main() error {
	flagVerbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()
	if !*flagVerbose {
		log.SetOutput(io.Discard)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	b, err := exec.CommandContext(ctx, "go", "list", "-json").Output()
	if err != nil {
		return err
	}
	var lst goList
	if err = json.Unmarshal(b, &lst); err != nil {
		return err
	}

	fset := token.NewFileSet()
	for _, fn := range lst.CgoFiles {
		fi, err := os.Stat(fn)
		if err != nil {
			return err
		}
		fset.AddFile(fn, -1, int(fi.Size()))
		f, err := parser.ParseFile(fset, fn, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %q: %w", fn, err)
		}
		if err = checkFile(fset, f); err != nil {
			return err
		}
	}

	return nil
}

func checkFile(fset *token.FileSet, f *ast.File) error {
	var errs []error
	for _, fun := range funcs(f.Decls) {
		//utter.Dump(fun.Body.List)
		fpos := fset.Position(fun.Pos())
		prefix := fmt.Sprintf("%s[%s line %d]: ", fun.Name, fpos.Filename, fpos.Line)
		ce := calls(fun.Body.List)
		cgoc := cgoCalls(ce)
		if len(cgoc) == 0 {
			continue
		}
		getErrors := filterCalls(ce, func(se *ast.SelectorExpr) bool { return se.Sel.Name == "getError" })
		if len(getErrors) == 0 {
			log.Println(prefix, "no getError call")
			continue
		}
		flot := firstLockOSThread(ce)
		if flot >= 0 && flot < cgoc[0].Pos() {
			log.Printf(prefix+" the first runtime.LockOSThread() call is on line %d, before the first cgo call (line %d)", fset.Position(flot).Line, fset.Position(cgoc[0].Pos()).Line)
			continue
		}
		errs = append(errs, fmt.Errorf(
			prefix+" has cgo calls (line %d), which is not protected with runtime.LockOSThread",
			fset.Position(cgoc[0].Pos()).Line,
		))
		log.Println(errs[len(errs)-1])
	}
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

var _ = (ast.Visitor)((*funcDeclVisitor)(nil))

type funcDeclVisitor struct {
	FuncDecls []*ast.FuncDecl
}

func (v *funcDeclVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}
	if fd, ok := node.(*ast.FuncDecl); ok && fd.Body != nil && len(fd.Body.List) != 0 {
		v.FuncDecls = append(v.FuncDecls, fd)
	}
	return v
}

func funcs(decls []ast.Decl) []*ast.FuncDecl {
	var v funcDeclVisitor
	for _, d := range decls {
		ast.Walk(&v, d)
	}
	return v.FuncDecls
}

type callExprVisitor struct {
	CallExprs []*ast.CallExpr
}

var _ = (ast.Visitor)((*callExprVisitor)(nil))

func (v *callExprVisitor) Visit(node ast.Node) ast.Visitor {
	if node == nil {
		return nil
	}
	switch x := node.(type) {
	case *ast.ExprStmt:
		if c, ok := x.X.(*ast.CallExpr); ok {
			v.CallExprs = append(v.CallExprs, c)
		}
	case *ast.CallExpr:
		v.CallExprs = append(v.CallExprs, x)
	}
	return v
}

func calls(stmts []ast.Stmt) []*ast.CallExpr {
	var v callExprVisitor
	for _, s := range stmts {
		ast.Walk(&v, s)
	}
	return v.CallExprs
}
func filterCalls(ce []*ast.CallExpr, filter func(se *ast.SelectorExpr) bool) []*ast.CallExpr {
	var cs []*ast.CallExpr
	for _, c := range ce {
		se, ok := c.Fun.(*ast.SelectorExpr)
		if ok && filter(se) {
			cs = append(cs, c)
		}
	}
	return cs
}
func firstLockOSThread(ce []*ast.CallExpr) token.Pos {
	for _, c := range filterCalls(ce, func(se *ast.SelectorExpr) bool {
		if se.Sel.Name == "LockOSThread" {
			if id, ok := se.X.(*ast.Ident); ok && id.Name == "runtime" {
				return true
			}
		} else if se.Sel.Name == "checkExec" {
			return true
		}
		return false
	}) {
		return c.Fun.(*ast.SelectorExpr).Sel.NamePos
	}
	return -1
}

func cgoCalls(ce []*ast.CallExpr) []*ast.CallExpr {
	return filterCalls(ce, func(se *ast.SelectorExpr) bool {
		if len(se.Sel.Name) < 3 {
			return false
		}
		switch strings.ToLower(se.Sel.Name[:3]) {
		case "oci", "dpi":
			if id, ok := se.X.(*ast.Ident); ok && id.Name == "C" {
				return true
			}
		}
		return false
	})
}
