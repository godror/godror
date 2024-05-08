#include <stdio.h>
#include "dpiImpl.h"

void TokenCallbackHandler(void *context, dpiAccessToken *access_token);

void TokenCallbackHandlerDebug(void *context, dpiAccessToken *acToken) {
	TokenCallbackHandler(context, acToken);
}
