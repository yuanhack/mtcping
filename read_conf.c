#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <fcntl.h>

#include "yhlog.h"
#include "read_conf.h"

// C99

#define DB_IP                     "DB_IP="
#define DB_PORT                   "DB_PORT="
#define DB_USER                   "DB_USER="
#define DB_PASS                   "DB_PASSWD="
#define DB_NAME                   "DB_NAME="
#define DB_TAB                    "DB_TAB="

#define FIELD_NAME_id             "field_id="
#define FIELD_NAME_dest           "field_dest="
#define FIELD_NAME_port           "field_port="
#define FIELD_NAME_tcping_delay   "field_tcping_delay="
#define FIELD_NAME_ping_delay     "field_ping_delay="  // datlastup
#define FIELD_NAME_ping_loss      "field_ping_loss="   // numlostrate
#define FIELD_NAME_tcping_time    "field_tcping_time="
#define FIELD_NAME_ping_time      "field_ping_time="
#define FIELD_NAME_gamename       "field_gamename="
#define FIELD_NAME_numtype        "field_numtype="
#define FIELD_NAME_addr           "field_addr="
#define FIELD_NAME_numbatch       "field_numbatch="


extern char db_ip[256];
extern int  db_port;
extern char db_user[64];
extern char db_pass[64];
extern char db_name[64];
extern char db_tab[64];

extern char	 field_id[64];
extern char	 field_dest[];
extern char	 field_port[64];
extern char	 field_tcping_delay[64];
extern char	 field_ping_delay[64];
extern char	 field_ping_loss[64];
extern char  field_tcping_time[64];
extern char  field_ping_time[64];

extern char  field_gamename[128];
extern char  field_numtype[64];
extern char  field_addr[256];
extern char  field_numbatch[64];


char _local_comm_sym[32] = {0};

char *next_line(const char *p)
{
    const char *pline;
    for (pline = p; *pline != '\n' && *pline != 0; pline++) ;
    if (*pline == '\n')
        return (char*)(pline + 1);
    else 
        return 0;
}

// Must be finished '\0'
char * fine_first_line_usable(const char *block
        , const char *item, const char *sym)
{
    const char *phead = block;
    while (*phead != 0) {
        // Line search with item and comment symbol
        if (stritem_usable(phead, item, sym) == 0) {
            //printf("line enable\n");
            break;
        } else {
            //printf("line disable\n");
        }
        phead = next_line(phead);
    }
    return (char *)phead;
}

char *jump_space (char *p)
{
	for ( ; *p == ' ' || *p == '\t'; p++ )	;
	return p;
}
char *strstr_after(const char *haystack, const char *needle)
{
	char *p = strstr((char*)haystack, needle);
	if ( p )
		p += strlen(needle);
	return p;
}

void set_item_error(const char *s)
{
	tlog_out("error config item: %s", s);
	exit(1);
}

void set_line_comment_symbol(const char *symbol)
{
    if (symbol != 0 && symbol[0])
        snprintf(_local_comm_sym, sizeof(_local_comm_sym), "%s", symbol);
}

void set_item_i(const char *buff, const char *item, int *pin)
{
	char *p = 0;
    char *head = fine_first_line_usable(buff, item, _local_comm_sym);
    if (head == 0)
		set_item_error(item);
	p = strstr_after(head, item);
	if(p == 0)
		set_item_error(item);
	p = jump_space(p);
	*pin = atoi(p);
}
void set_item_d(const char *buff, const char *item, double *pdo)
{
	char *p = 0;
    char *head = fine_first_line_usable(buff, item, _local_comm_sym);
    if (head == 0)
		set_item_error(item);
	p = strstr_after(head, item);
	if(p == 0)
		set_item_error(item);
	p = jump_space(p);
	*pdo = atof(p);
}
void set_item_l(const char *buff, const char *item, long *plo)
{
	char *p = 0;
    char *head = fine_first_line_usable(buff, item, _local_comm_sym);
    if (head == 0)
		set_item_error(item);
	p = strstr_after(head, item);
	if(p == 0)
		set_item_error(item);
	p = jump_space(p);
	*plo = atol(p);
}
// ps: Be careful to overflo
void set_item_s(const char *buff, const char *item, char *ps)
{
	char *p = 0;
    char *head = fine_first_line_usable(buff, item, _local_comm_sym);
    if (head == 0)
		set_item_error(item);

	p = strstr_after(head, item);
	if(p == 0)
		set_item_error(item);

    // example item=   abc 
    // original p ---> "   abc"
    // p = jump_space(p);  
    // after  p ---> "aa"
	p = jump_space(p);

    if (_local_comm_sym[0]) {
        // find line '\n' symbol
        const char *pn;
        for (pn = p; *pn != '\n' && *pn != '\0'; ++pn) ;
        int len = pn-p;
        char line[len + 1];
        memset(line, 0, len + 1);
        memcpy(line, p, len);

        char *psym = strstr(line, _local_comm_sym);
        if (psym)
            strncpy(ps, p, abs(psym - line));
        else
            strncpy(ps, line, len);
            //sscanf(p, "%[^\n]", ps); // here
    } else {
        sscanf(p, "%[^\n]", ps); // here
    }
}

// 测试一行配置项是否有效
// 如果行没有找到item, 整行都是无效的
// 注释标记到该行末尾是注释内容, 不被算作有效item
int stritem_usable(const char *linehead, const char *item, const char *sym)
{
      const char *p, *pend;

      // find line '\n' symbol
      for (p = linehead; *p != '\n' && *p != '\0'; ++p) ;
      pend = p;

      int len = pend - linehead;
      if (len <= 0) 
          return -3;  // No content
      
      char buff[len+1];
      memset(buff, 0, len+1);
      memcpy(buff, linehead, len);
      
      if ((p = strstr(buff, item)) == 0) 
          return -1; // no item

      const char *psym;

//#define _YH_SHOW_LINE 

      if (sym && sym[0]) {
          if ((psym = strstr(buff, sym))) {
              if (p <= psym) {
#ifdef _YH_SHOW_LINE
                    printf("line usable  |%s\n", buff);
#endif
                  return 0;
              } else {
#ifdef _YH_SHOW_LINE
                    printf("line comment |%s\n", buff);
#endif
                  return -1;
              }
          } else {
#ifdef _YH_SHOW_LINE
                    printf("line usable  |%s\n", buff);
#endif
              return 0;
          }
      } else {
          return 0;
      }
}


void read_conf(const char *conf, const char *comm_sym)
{
	int len;
	int fd;
	char _buff[1024*6] = {0};
    
	if ( (fd = open(conf, O_RDONLY)) < 0 ) 
		tlog_err_out_exit(1, "打开文件失败: %s", conf);
	len = read(fd, _buff, sizeof(_buff)-1);
	if ( len == sizeof(_buff) ) 
		tlog_out_exit(1, "配置文件内容异常\n");
	close(fd);

    set_line_comment_symbol(comm_sym);

	set_item_s(_buff, DB_IP,    db_ip);
	set_item_i(_buff, DB_PORT, &db_port);
	set_item_s(_buff, DB_USER,  db_user);
	set_item_s(_buff, DB_PASS,  db_pass);
	set_item_s(_buff, DB_NAME,  db_name);
	set_item_s(_buff, DB_TAB,   db_tab);

	set_item_s(_buff, FIELD_NAME_id,            field_id);
	set_item_s(_buff, FIELD_NAME_dest,          field_dest);
	set_item_s(_buff, FIELD_NAME_port,          field_port);

	set_item_s(_buff, FIELD_NAME_tcping_delay,  field_tcping_delay);
	set_item_s(_buff, FIELD_NAME_tcping_time,   field_tcping_time);

	set_item_s(_buff, FIELD_NAME_ping_delay,    field_ping_delay);
	set_item_s(_buff, FIELD_NAME_ping_loss,     field_ping_loss);
	set_item_s(_buff, FIELD_NAME_ping_time,     field_ping_time);

	set_item_s(_buff, FIELD_NAME_gamename,      field_gamename);
	set_item_s(_buff, FIELD_NAME_numtype,       field_numtype);
	set_item_s(_buff, FIELD_NAME_addr,          field_addr);
	set_item_s(_buff, FIELD_NAME_numbatch,      field_numbatch);
}

