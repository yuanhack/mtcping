#include <ctype.h>
#include <getopt.h>
#include <mysql/mysql.h>

#include "yhlog.h"
#include "read_conf.h"
#include "yhnet.h"


#define tr_path "/usr/sbin/traceroute"


char db_ip[256];
int  db_port;
char db_user[64];
char db_pass[64];
char db_name[64];
char db_tab[64];
char  field_id[64];
char  field_dest[64];
char  field_port[64];
char  field_tcping_delay[64];
char  field_tcping_time[64];
char  field_ping_delay[64];
char  field_ping_loss[64];
char  field_ping_time[64];
char  field_gamename[64];
char  field_numtype[64];
char  field_addr[64];
char  field_numbatch[64];

void traceroute(MYSQL *m, char *s1, char *s2, char *s3);
void process()
{
    MYSQL * mysql = 0;
    MYSQL_RES *mysql_res = 0;
    MYSQL_ROW mysql_row;

    int num_row;
    int line;
    char sql[1024*4];

    if ( !(mysql = mysql_init((MYSQL*)0)) ) {
        tlog_out_exit(1, "mysql init error");
    }
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8");
    if (!(mysql_real_connect(mysql, db_ip, db_user, db_pass, db_name, db_port, NULL, CLIENT_LOCAL_FILES)))
        tlog_out_exit(1, "fetch src, mysql connect error !!!");

    snprintf(sql, sizeof(sql), "select DISTINCT t.vc2gamename, t.vc2ip, numbatch from game_monitip t ");
    //tlog_out(sql);
    if ( mysql_real_query(mysql, sql, strlen(sql)) ) {
        tlog_out("mysql_real_query() error");
        return;
    }
    if ( 0 == (mysql_res = mysql_store_result(mysql)) ) {
        tlog_out("mysql_store_result() error");
        goto err_out;
    }
    num_row = mysql_num_rows(mysql_res);
    if ( num_row <= 0) 
        goto err_out;
    for(line = 0; line < num_row; line++) {
        if((mysql_row = mysql_fetch_row(mysql_res)) == 0) break;
        if (!mysql_row[0] || !mysql_row[1] || !mysql_row[2]) {
            tlog_out("跳过存在空数据字段的测试源: %s | %s | %s", mysql_row[0], mysql_row[1], mysql_row[2]);
            continue;
        }
        //                游戏名        ip            numbatch
        traceroute(mysql, mysql_row[0], mysql_row[1], mysql_row[2]);
    }
    mysql_free_result(mysql_res);
    mysql_close(mysql);
    return;

err_out:
    tlog_out("mysql error %d:%s", mysql_errno(mysql), mysql_error(mysql));
    mysql_close(mysql);
    return;
}

static struct option opt[] = {
        {"log", 1, 0, 'l'},
        {0,0,0,0}
};

int conf_flag;
int log_flag ;
int intf_flag;
int daemon_flag;
int nodns_flag;

int max_hops_flag;
int max_hops = 30;

int wait_time_flag;
int wait_time = 5;

char conf_path[256];
char log_path[256];
char intf_name[56];
char intf_ip[256];

void usage(int argc, char ** argv, int exno)
{
    printf("用法: %s [参数列表]...\n", argv[0]);
    printf("    选项说明\n");
    printf("    -d                  后台运行\n");
    printf("    -n                  不解析DNS域名\n");
    printf("    -m  max-hops        设置最大跳数\n");
    printf("    -w  seconds         设置探测包的等待应答超时时间\n");
    printf("    -c  file-path       指定Mysql连接属性配置文件file-path\n");
    printf("    -I  interface       绑定名称为interface的网卡\n");
    printf("    --log=file-path     指定日志文件输出到file-path\n");
    exit(exno);
}

int
main(int argc, char **argv)
{

    char *cptr;
    if (argc < 2) usage(argc, argv, 1);

    char c;
    opterr = 1;
    while ((c = getopt_long(argc, argv, "dc:l:I:nm:w:", opt, 0)) != -1) {
        switch (c) {
            case 'd':
                daemon_flag = 1;
                break;
            case 'c':
                conf_flag = 1; conf_path[0] = 0;
                snprintf(conf_path, sizeof(conf_path), "%s", optarg);
                break;
            case 'I':
                intf_flag = 1; intf_name[0] = 0;
                snprintf(intf_name, sizeof(intf_name), "%s", optarg);
                break;
            case 'l':
                log_flag = 1; log_path[0] = 0;
                snprintf(log_path, sizeof(log_path), "%s", optarg);
                break;
            case 'n':
                nodns_flag = 1;
                break;
            case 'm':
                cptr = NULL;
                max_hops_flag = 1;
                max_hops = strtol(optarg, &cptr, 10);
                if (cptr == optarg) 
                { printf("-m max-hops 选项错误\n");exit(1); }
                if (max_hops <= 0) 
                { printf("最大跳数选项设置错误 -m max-hops，合法值 max-hops >= 0\n"); exit(1); }
                max_hops_flag = 1;
                break;
            case 'w':
                cptr = NULL;
                wait_time_flag = 1;
                wait_time = strtol(optarg, &cptr, 10);
                if (cptr == optarg) 
                { printf("-w seconds 选项错误\n");exit(1); }
                if (wait_time < 1) 
                { printf("设置探测包的等待应答超时时间错误，合法值 seconds > 0\n"); exit(1); }
                max_hops_flag = 1;
                break;
            default:
                usage(argc, argv, 1);
                break;
        }
    }
    if (intf_flag == 0) {
        printf("未绑定网卡，参考选项 -I interface\n"); 
        exit(1);
    }
    if (log_flag) {
        if (open_log(log_path, -1) < 0) {
            printf("日志文件打开失败: %s: %s\n", log_path, strerror(errno));
            exit(1);
        }
    }
    if (conf_flag == 0) {
        printf("未指定数据库配置文件, 参考选项 -c file\n");
        exit(1);
    }
    if (get_interface_ip_v4(intf_name, intf_ip, sizeof(intf_ip)) < 0) {
        log_out("获取网卡 %s IP地址失败，", intf_name);
        log_out(" 1  请使用系统命令 ifconfig 检查网络配置.");
        log_out(" 2  请检查 -I interface 选项对应网卡名称是否正确.");
        exit(1);
    }
    read_conf(conf_path, "#");
    if (daemon_flag)
        daemon(0, 0);
    process();
    return 0;
}

char * fmt_time(char *p, size_t len)
{
    time_t timee;
    struct tm tmm;

    timee = time(0);
    localtime_r(&timee, &tmm);
    strftime(p, len, "%Y-%m-%d %H:%M:%S", &tmm);
    return p;
}
void traceroute(MYSQL *m, char *vc2gamenm, char *vc2ip, char *numbatch)
{
    char cmd[1024] = {0};
    char buf[1024*4] = {0};
    char sql[1024*5] = {0};
    char line[512];
    char date[56];
    FILE * fp;
    int n = 0;

    n = snprintf(cmd, sizeof(cmd), "%s -S %s", tr_path, intf_ip); 
    if (nodns_flag)
        n += snprintf(cmd+n, sizeof(cmd)-n, " -n"); 
    if (max_hops_flag)
        n += snprintf(cmd+n, sizeof(cmd)-n, " -m %d", max_hops); 
    if (wait_time_flag)
        n += snprintf(cmd+n, sizeof(cmd)-n, " -w %d", wait_time); 
    n += snprintf(cmd+n, sizeof(cmd)-n, " %s", vc2ip); 

    tlog_out("%s", cmd);

    if ((fp = popen(cmd, "r")) == 0) { tlog_out("failed: %s", cmd); return; }

    n = 0;
    tlog_out("---------- %s %s %s ----------", vc2gamenm, vc2ip, numbatch);
    while (1) {
        if (fgets(line, sizeof(line), fp) == 0)
        { if (ferror(fp)) tlog_out("fgets error: %s", cmd); break; }

        tlog_outn("%s", line);
        n += snprintf(buf+n, sizeof(buf)-n, "%s", line);
    }
    fclose(fp);

    if (strlen(buf) <= 0) return;

    fmt_time(date, sizeof(date));
    snprintf(sql, sizeof(sql), "insert into his_ip_trace set vc2state='1',"
            "vc2gamenm='%s', vc2ip='%s', numbatch='%s',dattime='%s', vc2trace='%s'"
            , vc2gamenm, vc2ip, numbatch, date, buf);
    if (mysql_real_query(m, sql, strlen(sql))) 
        tlog_out("----- insert error: %s", mysql_error(m));
    else 
        tlog_out("----- insert success %s traceroute.", vc2ip);
}
