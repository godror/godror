#include <stdint.h>
#include "dpiImpl.h"
#include "token.h"

void *godrorWrapHandle(uintptr_t handle) {
  godrorHwrap *a1 = calloc(1, sizeof(godrorHwrap));
  a1->handle = handle;
  dpiMutex__initialize(a1->mutex);
  return (void *)a1;
}

// godrorUnwrapHandle frees what godrorWrapHandle and the callbacks allocated.
// It must only be called when no callback can run anymore.
void godrorUnwrapHandle(void *ptr) {
  godrorHwrap *a1 = (godrorHwrap*)ptr;
  if (a1->token) {
    free((void *)a1->token);
  }
  if (a1->privateKey) {
    free((void *)a1->privateKey);
  }
  dpiMutex__destroy(a1->mutex);
  free(a1);
}

void TokenCallbackHandler(uintptr_t handle, dpiAccessToken *access_token);

int godrorTokenCallbackHandlerDebug(void *context, dpiAccessToken *acToken) {
  godrorHwrap *a1 = (godrorHwrap*)context;
  // Without the lock, two concurrent callbacks for the same pool would both
  // free the previous token (double free).
  dpiMutex__acquire(a1->mutex);
  if (a1->token) {
    free((void *)a1->token);
  }
  if (a1->privateKey) {
    free((void *)a1->privateKey);
  }
  TokenCallbackHandler(a1->handle, acToken);
  a1->token = acToken->token;
  a1->privateKey = acToken->privateKey;
  dpiMutex__release(a1->mutex);
  return 0;
}
