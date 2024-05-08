// Copyright 2024 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror

/*
#include <stdlib.h>
#include <stdio.h>
#include "dpiImpl.h"

int TokenCallbackHandlerDebug(void* context, dpiAccessToken *token);

*/
import "C"

import (
	"context"
	"github.com/godror/godror/dsn"
	"log"
	"sync"
	"unsafe"
)

// AccessToken Callback information.
type accessTokenCB struct {
	ctx         context.Context
	callback    func(context.Context, *dsn.AccessToken) error
	id          uint64
	ctoken      *C.char
	cprivateKey *C.char
}

// Cannot pass *accessTokenCB to C, so pass an uint64 that points to
// this map entry
var (
	accessTokenCBsMu sync.Mutex
	accessTokenCBs   = make(map[uint64]*accessTokenCB)
	accessTokenCBsID uint64
)

// tokenCallbackHandler is the callback for C code on token expiry.
//
//export TokenCallbackHandler
func TokenCallbackHandler(ctx unsafe.Pointer, accessTokenC *C.dpiAccessToken) {
	log.Printf("CB %p %+v", ctx, accessTokenC)
	accessTokenCBsMu.Lock()
	acessTokenCB := accessTokenCBs[*((*uint64)(ctx))]
	accessTokenCBsMu.Unlock()

	// Call user function which provides the new token and privateKey.
	var refreshAccessToken dsn.AccessToken
	if err := acessTokenCB.callback(acessTokenCB.ctx, &refreshAccessToken); err != nil {
		log.Printf("Token Generation Failed CB %p %+v", ctx, acessTokenCB)
		return
	}
	if acessTokenCB.ctoken != nil {
		C.free(unsafe.Pointer(acessTokenCB.ctoken))
	}
	if acessTokenCB.cprivateKey != nil {
		C.free(unsafe.Pointer(acessTokenCB.cprivateKey))
	}
	token := refreshAccessToken.Token
	privateKey := refreshAccessToken.PrivateKey
	if token != "" {
		accessTokenC.token = C.CString(token)
		acessTokenCB.ctoken = accessTokenC.token
		accessTokenC.tokenLength = C.uint32_t(len(token))
		if privateKey != "" {
			accessTokenC.privateKey = C.CString(privateKey)
			acessTokenCB.cprivateKey = accessTokenC.privateKey
			accessTokenC.privateKeyLength = C.uint32_t(len(privateKey))
		}
	}
}

// RegisterTokenCallback will add an entry of callback data in a map.
func RegisterTokenCallback(poolCreateParams *C.dpiPoolCreateParams,
	tokenGenFn func(context.Context, *dsn.AccessToken) error,
	tokenCtx context.Context, id *uint64) {
	// typedef int (*dpiAccessTokenCallback)(void* context, dpiAccessToken *accessToken);
	poolCreateParams.accessTokenCallback = C.dpiAccessTokenCallback(C.TokenCallbackHandlerDebug)

	// cannot pass &accessTokenCB to C, so pass indirectly
	tokenCB := accessTokenCB{callback: tokenGenFn, ctx: tokenCtx}
	accessTokenCBsMu.Lock()
	accessTokenCBsID++
	tokenCB.id = accessTokenCBsID
	*id = accessTokenCBsID
	accessTokenCBs[tokenCB.id] = &tokenCB
	accessTokenCBsMu.Unlock()
	tokenID := (*C.uint64_t)(C.malloc(8))
	*tokenID = C.uint64_t(accessTokenCBsID)
	poolCreateParams.accessTokenCallbackContext = unsafe.Pointer(tokenID)
}

// UnRegisterTokenCallback will remove the token callback data registered
// during pool creation.
func UnRegisterTokenCallback(key uint64) {
	accessTokenCBsMu.Lock()
	value, ok := accessTokenCBs[key]
	accessTokenCBsMu.Unlock()
	if ok {
		if value.ctoken != nil {
			C.free(unsafe.Pointer(value.ctoken))
		}
		if value.cprivateKey != nil {
			C.free(unsafe.Pointer(value.cprivateKey))
		}
		delete(accessTokenCBs, key)
	}
}
