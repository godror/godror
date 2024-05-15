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
	"fmt"
	"sync"
	"unsafe"

	"github.com/godror/godror/dsn"
	"github.com/godror/godror/slog"
)

// AccessToken Callback information.
type accessTokenCB struct {
	ctx         context.Context
	callback    func(context.Context, *dsn.AccessToken) error
	id          *C.uint64_t
	ctoken      *C.char
	cprivateKey *C.char
}

// Cannot pass *accessTokenCB to C, so pass an uint64 that points to
// this map entry
var (
	accessTokenCBsMu sync.RWMutex
	accessTokenCBs   = make(map[uint64]*accessTokenCB)
	accessTokenCBsID uint64
)

// tokenCallbackHandler is the callback for C code on token expiry.
//
//export TokenCallbackHandler
func TokenCallbackHandler(ctx unsafe.Pointer, accessToken *C.dpiAccessToken) {
	logger := getLogger(context.TODO())
	if logger != nil && logger.Enabled(context.TODO(), slog.LevelDebug) {
		logger.Debug("TokenCallbackHandler", "ctx", fmt.Sprintf("%p", ctx), "accessToken", accessToken)
	}
	accessTokenCBsMu.RLock()
	tokenCB := accessTokenCBs[*((*uint64)(ctx))]
	accessTokenCBsMu.RUnlock()

	// Call user function which provides the new token and privateKey.
	var refreshAccessToken dsn.AccessToken
	if err := tokenCB.callback(tokenCB.ctx, &refreshAccessToken); err != nil {
		if logger != nil && logger.Enabled(context.TODO(), slog.LevelDebug) {
			logger.Debug("tokenCB.callback", "ctx", fmt.Sprintf("%p", ctx),
				"tokenCB", tokenCB)
		}
		return
	}
	if tokenCB.ctoken != nil {
		C.free(unsafe.Pointer(tokenCB.ctoken))
	}
	if tokenCB.cprivateKey != nil {
		C.free(unsafe.Pointer(tokenCB.cprivateKey))
	}
	token := refreshAccessToken.Token
	privateKey := refreshAccessToken.PrivateKey
	if token != "" {
		accessToken.token = C.CString(token)
		tokenCB.ctoken = accessToken.token
		accessToken.tokenLength = C.uint32_t(len(token))
		if privateKey != "" {
			accessToken.privateKey = C.CString(privateKey)
			tokenCB.cprivateKey = accessToken.privateKey
			accessToken.privateKeyLength = C.uint32_t(len(privateKey))
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
	*id = accessTokenCBsID
	accessTokenCBs[accessTokenCBsID] = &tokenCB
	accessTokenCBsMu.Unlock()
	tokenID := (*C.uint64_t)(C.malloc(8))
	tokenCB.id = tokenID
	*tokenID = C.uint64_t(accessTokenCBsID)
	poolCreateParams.accessTokenCallbackContext = unsafe.Pointer(tokenID)
}

// UnRegisterTokenCallback will remove the token callback data registered
// during pool creation.
func UnRegisterTokenCallback(key uint64) {
	accessTokenCBsMu.Lock()
	value, ok := accessTokenCBs[key]
	defer accessTokenCBsMu.Unlock()
	if ok {
		if value.id != nil {
			C.free(unsafe.Pointer(value.id))
		}
		if value.ctoken != nil {
			C.free(unsafe.Pointer(value.ctoken))
		}
		if value.cprivateKey != nil {
			C.free(unsafe.Pointer(value.cprivateKey))
		}
		delete(accessTokenCBs, key)
	}
}
