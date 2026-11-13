#ifndef GODROR_TOKEN
#define GODROR_TOKEN

// token.h must be included after dpiImpl.h (for dpiMutexType).

struct godrorHwrap {
  uintptr_t handle;
  const char *token;
  const char *privateKey;
  // serializes callbacks that replace token/privateKey
  dpiMutexType mutex;
};
typedef struct godrorHwrap godrorHwrap;

void *godrorWrapHandle(uintptr_t handle);
void godrorUnwrapHandle(void *ptr);
int godrorTokenCallbackHandlerDebug(void* context, dpiAccessToken *token);

#endif
