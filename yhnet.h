#ifndef __YH_YHNET_H__
#define __YH_YHNET_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C"
{
#endif

int get_interface_ip_v4(char *interface_name, char * ipbuf, int len);

#ifdef __cplusplus
}
#endif
#endif /* __YH_YHNET_H__ */
