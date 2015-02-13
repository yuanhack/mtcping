#include "yhnet.h"

//ifconfig eth0 | grep 'inet addr'| awk '{print $2}'| awk -F: '{print $2}'
int get_interface_ip_v4(char *interface_name, char * ipbuf, int len)
{
	char rbuf[1024] = {0};
	char cmdbuf[256];
	FILE* fp;
    memset(ipbuf, 0, len);
    snprintf(cmdbuf, sizeof(cmdbuf), "/sbin/ifconfig %s | "
            "grep 'inet addr'| awk '{print $2}'| awk -F: '{printf $2}'"
            , interface_name);
	if ((fp = popen(cmdbuf, "r")) == 0) {
		ipbuf[0] = 0;
        return -1;
	}
	fgets(rbuf, sizeof(rbuf), fp);
	fclose(fp);
    if (strlen(rbuf) > 0)
        snprintf(ipbuf, len, "%s", rbuf);
    else
        return -1;
	return 0;
}

