#include <netdb.h>
#include <ctype.h>
#include <mysql/mysql.h>
#include <mqueue.h>

#include "mtcping.h"
#include "read_conf.h"

#include "ping.h"
#include "yhnet.h"

#define __db_name__         "sinoix"        // default
#define __db_tab__          "tcping"        // default
#define THREAD_STACK_SIZE  (1024*1024)
#define DB_GET             "/mq_mdbtcping1"
#define DB_PUT             "/mq_mdbtcping2"
#define DEST_LEN           128

#define VERSION 0.3.0
#define FILE_MODE       (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

char db_ip[256];
int  db_port;
char db_user[64];
char db_pass[64];
char db_name[64];
char db_tab[64];
char  field_id[64];
char  field_dest[DEST_LEN];
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

const char *root_log = "/var/log/mtcping.log";
const char *user_log = "/tmp/mtcping.log";

struct mqdata{
    long long   id;
    char   dest[DEST_LEN];  // destination, ip or domain
    int    port;            // tcp port
    char   name[128];       // vc2gamename
    int    type;            // numtype
    long long batch;        // numbatch   
    char   addr[128];       // vc2addr
    float  tcping_ms;       // delay, millisecond
    float  tcping_ms_real;  // delay, millisecond
    float  ping_ms;         // delay, millisecond
    float  ping_ms_real;    // delay, millisecond
    int  ping_loss;         // loss rate
};

void worker_environment();
int worker_data_from_db();

int  multi_num = 1;
int  daemon_flag;
int  always_flag;
int  log_flag;
char log_path[256] = {0};

int  db_flag;
int  dos_flag;
char conf_path[256] = {0};

int  in_finish;
long num_count = -1;
long loop_sec = -1;
int  page_line = 1000;

int  tcping_count = 5;

int  ping_flag;
int  ping_count = 5;

int  insert_flag;
int  update_flag;

int interface_flag;
char interface_name[32];
char interface_ip[256];

int super_flag;
int super_val;

int  x_flag;
long long x_id=-0x7fffffffffffffff;
int  y_flag;
long long y_id=0x7fffffffffffffff;

int  extended;

int tcping(const char *ip, int port, int count, float *tcping_ms);

void usage(int argc, char ** argv, int exno)
{
    printf("用法: %s 选项列表\n", argv[0]);
    printf(" 选项说明:\n");
    printf("  -d              后台守护进程方式运行\n");
    printf("  -I interface    指定绑定的网卡名称,该项在多网卡的机器上设定从指定的网卡进行数据包的发送和接收\n");
    printf("                    例如有一台多网卡机器有网卡 eth0和eth1, 使用 -I eth0 将绑定到该网卡而不使用其他网卡\n");
    printf("  -l[file-path]   -l后面指定定制的输出文件路径,并且该文件将持续写入不会限制长度.\n");
    printf("                    如果-l后面没有定制输出\n");
    printf("                      root用户的日志输出在/var/log/mtcping.log\n");
    printf("                      其他用户的日志输出在/tmp/mtcping.log\n");
    printf("  -a              此选项程序不会自动退出,测试完一轮以后继续下一轮从头开始测试\n");
    printf("  -t second       此选项仅对具有-a选项才生效,\n");
    printf("                   second是每次测试完后距下次从头开始的间隔时间.\n");
    printf("  -e              扩展错误信息输出,如连接失败等,如需要一份干净的日志,勿使用该选项.\n");
    printf("  -m number       多线程方式多个任务并发执行,合法值(0 < number < 1000)\n");
    printf("                  如果要执行ping测试, 为避免大量并发ping icmp包\n");
    printf("                    建议 number 的值设置小一些, 例如 300.\n");
    printf("  -c file         Mysql连接属性配置文件\n");
    printf("  -w              该选项将日志的输出定制成Windows下的换行符\"\\r\\n\"\n");
    printf("  -p page-line    每次限制从数据库取出的数据条数\n") ;
    printf("                    当数据量很大时,使用合理的的page-line值\n");
    printf("                    能提高从数据库取数据记录的效率\n");
    printf("  -x numipid      指定数据库中 numipid 的开始最小值\n");
    printf("                    有效范围包括 numipid 值, 如指定0,那么 >=0 是有效的\n");
    printf("  -y numipid      指定数据库中 numipid 的结束最大值\n");
    printf("                    有效范围包括 numipid 值, 如指定999,那么 <=999 是有效的\n");
    printf("                    x,y选项的作用是为解决每次工作时从数据库源取数据一个范围值,\n");
    printf("                    如数据表非常庞大,网络情况不好可能一次工作无法进行完就被迫停止了\n");
    printf("                    这时 x 选项就非常有用了, 根据日志输出查看 numipid 停止的位置.\n");
    printf("                      下次执行任务时直接用x选项跳过numipid之前已完成的部分.\n");
    printf("                      例如此次任务通过日志发现执行到 numipid=1111 这个位置,\n");
    printf("                        那么下次使用 -x 1112 以开始继续余下的任务.\n");
    printf("                    y选项指定此次任务的上限范围,主要用途是为了和x选项配合将大型任务分解成若干小块任务片.\n");
    printf("                    y选项的 numipid 值必须不小于 x 选项的numipid 才是合法的.\n");
    printf("  -g              附加执行ping测试, 如不带-g选项,则只进行tcping测试\n");
    printf("                    要执行 ping 测试, 本程序必须具有root可执权限\n");
    printf("  -u              更新 数据源表\n");
    printf("  -i              插入 历史表\n");
    printf("                    如果要将结果追加或者更新数据库,必须使用 -i 或 -u 选项之一\n");
    printf("                    如果这两个选项都不用的话, 测试结果不会进行入库或更新.\n");
    printf("                    两个选项都使用的话则将结果更新数据源表后且追加到历史表中.\n");
    printf("  -T number       设置对每个目标的tcping测试次数.\n");
    printf("  -G number       设置对每个目标的ping测试次数,使用此选项可以省略-g选项.\n");
    exit(exno);
}

void worker()
{
    if (db_flag)
        worker_data_from_db();
}

void loop_worker()
{
    while (1) {
        worker();
        if (loop_sec > 0) 
            sleep(loop_sec);
    }
}

int main(int argc, char ** argv)
{
    char ch;
    char *cptr;

    if (argc == 1) 
        usage(argc, argv, 1);

    opterr = 1;  
    while ((ch = getopt(argc, argv, "hdal::c:wf:m:t:p:guiex:y:I:S:T:G:")) != -1) {
        switch(ch) {
            case 'h':
                usage(argc, argv, 0);
                break;
            case 'd':
                daemon_flag = 1;
                break;
            case 'a':
                always_flag = 1;
                break;
            case 'l':
                log_flag = 1;
                if (optarg)
                    snprintf(log_path, sizeof(log_path), "%s", optarg);
                else
                    log_path[0] = 0;
                break;
            case 'c':
                db_flag = 1;
                snprintf(conf_path, sizeof(conf_path), "%s", optarg);
                break;
            case 'w':
                dos_flag = 1;
                break;
            case 'm':
                cptr = NULL;
                multi_num = strtol(optarg, &cptr, 10);
                if (cptr == optarg) {
                    printf("-m number 选项错误\n"); exit(1);
                }
                if (multi_num < 0 || multi_num >= 1001) {
                    printf("%s: invalid option  '-m number', valid (0 < multi_num <= 1000)\n", argv[0]);
                    usage(argc, argv, 1);
                }
                break;
            case 't':
                cptr = NULL;
                loop_sec = strtol(optarg, &cptr, 10);
                if (cptr == optarg) {
                    printf("-t second 选项错误\n"); exit(1);
                }
                if (loop_sec < 0) {
                    printf("%s: invalid option  '-t second', valid (second >= 0)\n", argv[0]);
                    usage(argc, argv, 1);
                }
                break;
            case 'p':
                cptr = NULL;
                page_line = strtol(optarg, &cptr, 10);
                if (cptr == optarg) {
                    printf("-p page_line 选项错误\n"); exit(1);
                } 
                if (page_line <= 0) {
                    printf("%s: invalid option  '-p page-line', valid (page-line > 0)\n", argv[0]);
                    usage(argc, argv, 1);
                }
                break;
            case 'x':
                x_flag = 1;
                cptr = NULL;
                x_id = strtoll(optarg, &cptr, 10);
                if (cptr == optarg)
                    usage(argc, argv, 1);
                break;
            case 'y':
                y_flag = 1;
                cptr = NULL;
                y_id = strtoll(optarg, &cptr, 10);
                if (cptr == optarg)
                    usage(argc, argv, 1);
                break;
            case 'g':
                ping_flag = 1;
                break;
            case 'u':
                update_flag = 1;
                break;
            case 'i':
                insert_flag = 1;
                break;
            case 'e':
                extended = 1;
                break;
            case 'I':
                interface_flag = 1;
                snprintf(interface_name, sizeof(interface_name), "%s", optarg);
                break;
            case 'S':
                super_flag = 1;
                cptr = NULL;
                super_val = strtol(optarg, &cptr, 10);
                if (cptr == optarg)
                    usage(argc, argv, 1);
                if (super_val < 0) exit(1);
                break;
            case 'T':
                cptr = NULL;
                tcping_count = strtol(optarg, &cptr, 10);
                if (cptr == optarg)
                    usage(argc, argv, 1);
                break;
            case 'G':
                ping_flag = 1;
                cptr = NULL;
                ping_count = strtol(optarg, &cptr, 10);
                if (cptr == optarg)
                    usage(argc, argv, 1);
                break;
            default:
                usage(argc, argv, 1);
                break;
        }
    }
    if (geteuid() != 0) {
        printf("权限不足, 请使用root用户运行程序或设置root权限SUID然后以普通用户运行.\n");
        exit(1);
    }
    if (tcping_count < 0) {
        printf("选项 -T number 错误, number必须大于0");
        exit(1);
    }
    if (ping_count < 0) {
        printf("选项 -G number 错误, number必须大于0");
        exit(1);
    }
    srand(time(0));
    if (x_id > y_id) {
        printf("选项 -x, -y 的numipid范围错误: %lld %lld\n", x_id, y_id);
        exit(1);
    }
    if (!db_flag) {
        printf("未指定Mysql数据库配置文件，请用选项 -c file 指定.\n");
        exit(1);
    }
    if (!insert_flag && !update_flag) {
        printf("您想进行何种操作，请参考选项 -i 和 -u\n");
        exit(1);
    }
    if (!interface_flag) {
        printf("请使用 -I interface 选项设置绑定网卡.\n");
        exit(1);
    }
    if (get_interface_ip_v4(interface_name, interface_ip, sizeof(interface_ip)) < 0) {
        log_out("获取网卡 %s IP地址失败，", interface_name);
        log_out(" 1  请使用系统命令 ifconfig 检查网络配置.");
        log_out(" 2  请检查 -I interface 选项对应网卡名称是否正确.");
        exit(1);
    }
        
    if (db_flag) 
        read_conf(conf_path, "#");

    if (log_flag) {
        // custom log, size unlimited
        if (log_path[0]) {
            if (open_log(log_path, 0) < 0) 
                return(1);
            printf("日志文件输出: %s\n", log_path);
        } else {
            // System's log limited 5MB
            if (getuid() == 0) 
                snprintf(log_path, sizeof(log_path), "%s", root_log);
            else 
                snprintf(log_path, sizeof(log_path), "%s", user_log);
            if (open_log(log_path, 5) < 0) 
                return 1;
            printf("日志文件将在达到%dM自动清空, 路径: %s\n", 5, log_path);
        }
    }

    if (daemon_flag)
        daemon(0, 0);

    worker_environment();

    if (always_flag)
        loop_worker();
    else 
        worker();

    in_finish = 1;

    while (1) pause();

    return 0;
}

mqd_t mqd_db_in;   
mqd_t mqd_db_into;  

// mqueue workflow:
//      worker_from_db()         db          -> mqd_db_in      
//      thread_producer()        mqd_db_in   -> mqd_db_into
//      thread_consumer()        mqd_db_into -> db

void init_worker_mqueue()
{
    mq_unlink(DB_GET);
    mq_unlink(DB_PUT);

    struct mq_attr attr = {0};
    attr.mq_msgsize = sizeof(struct mqdata);
    attr.mq_maxmsg = 100;

    if (geteuid() == 0) {
        mqd_db_in   = mq_open(DB_GET, O_RDWR | O_CREAT, FILE_MODE, &attr);
        if (mqd_db_in < 0) 
            tlog_err_out_exit(1, "mq_open %s error", DB_GET);

        mqd_db_into = mq_open(DB_PUT, O_RDWR | O_CREAT, FILE_MODE, &attr);
        if (mqd_db_into < 0) 
            tlog_err_out_exit(1, "mq_open %s error", DB_PUT);
    } else {
        mqd_db_in   = mq_open(DB_GET, O_RDWR | O_CREAT, FILE_MODE, 0);
        if (mqd_db_in < 0) 
            tlog_err_out_exit(1, "mq_open %s error", DB_GET);

        mqd_db_into = mq_open(DB_PUT, O_RDWR | O_CREAT, FILE_MODE, 0);
        if (mqd_db_into < 0) 
            tlog_err_out_exit(1, "mq_open %s error", DB_PUT);
    }

    mq_unlink(DB_GET);
    mq_unlink(DB_PUT);
}

int ntcping = 0;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void tcping_add()
{
    pthread_mutex_lock(&mutex);
    ntcping++;
    pthread_mutex_unlock(&mutex);
}

void tcping_del()
{
    pthread_mutex_lock(&mutex);
    ntcping--;
    pthread_mutex_unlock(&mutex);
}
int tcping_is_zero()
{
    int a = 0;
    pthread_mutex_lock(&mutex);
    if (ntcping == 0) a = 1;
    pthread_mutex_unlock(&mutex);
    return a;
}
int tcping_get()
{
    int a = 0;
    pthread_mutex_lock(&mutex);
    a = ntcping;
    pthread_mutex_unlock(&mutex);
    return a;
}

float super_mode(float a, float b)
{
    unsigned int rr;
    float rf;
    float ret = a;
    if (a > b) {
        rr = rand();
        rf = rr % 100 / (float)10;
        ret = b - rf;
        if (ret < 0) 
            ret = -ret;
    }
    return ret;
}
    
// consumer: mqd_db_in -> thread_net_worker
// producer: thread_net_worker -> mqd_db_into 
void * thread_net_worker(void *p)
{
    //struct mqdata data = {0};
    struct mqdata *data;
    unsigned int prio;
    float tcping_ms;

    struct mq_attr attr;
    mq_getattr(mqd_db_in, &attr);
    char buff[attr.mq_msgsize];
    data = (struct mqdata*)buff;

    Ping ping;
    PingResult pres;
    int pgsend, pgrecv;
    int ret, i;

    pthread_detach(pthread_self());

    while (1) {
        mq_receive(mqd_db_in, buff, attr.mq_msgsize, &prio);
        tcping_add();
        //tlog_out("id %6lld: %s:%d", data->id, data->dest, data->port);
        tcping(data->dest, data->port, tcping_count, &tcping_ms);
        data->tcping_ms_real  = tcping_ms; // x_real 真实值
        if (super_flag) {
            tcping_ms = super_mode(tcping_ms, super_val);
        }
        data->tcping_ms = tcping_ms;
        if (ping_flag) {
            pgsend = pgrecv = 0;
            pres.min = 99999; 
            pres.max = pres.tot = 0;
            for (i = 0; i < ping_count; i++) {
                ret = ping.ping(data->dest, pres);
                if (ret == false) {
                    tlog_out("%s", pres.error.c_str());
                    break;
                }
                pgsend += pres.nsend;
                pgrecv += pres.nreceived;
            }
            if (i < ping_count) {
                data->ping_ms = -1;
                data->ping_ms_real = -1;
                data->ping_loss = ping_count;
            } else {
                if (pgrecv > 0) {
                    data->ping_ms = pres.tot/pgrecv;
                    data->ping_ms_real = data->ping_ms;
                }
                else {
                    data->ping_ms = -1;
                    data->ping_ms_real = -1;
                }
                data->ping_loss = pgsend - pgrecv;
            }
            if (super_flag) {
                data->ping_ms = super_mode(data->ping_ms, super_val);
            }
        }
        tcping_del();
        mq_send(mqd_db_into, buff, sizeof(struct mqdata), 0);
    }

    exit(0);

    return (void*) 0;
}

void init_producer(int multi_num)
{
    pthread_t tid;
    pthread_attr_t attr;
    int n, err;

    if ( (err = pthread_attr_init(&attr)) != 0)
        tlog_out_exit(1, "pthread_attr_init error: %s", strerror(err));
    if ( (err = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE)) != 0)
        tlog_out_exit(1, "pthread_attr_setstacksize error: %s", strerror(err));

    for (n = 0; n < multi_num; n++) {
        if ((err = pthread_create(&tid, &attr, thread_net_worker, 0)) != 0)
            tlog_out_exit(1, "pthread_create thread_net_worker error: %s", strerror(err));
    }

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

    inline
void update_fail_into_db_log(struct mqdata *data)
{
    if (dos_flag) {
        if (ping_flag) {
            tlog_out("update  failed. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d\r"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("update  failed. %s=%lld %s:%d tcping=%.1fms.\r"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    } else {
        if (ping_flag) {
            tlog_out("update  failed. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("update  failed. %s=%lld %s:%d tcping=%.1fms."
                    , field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    }
}
    inline
void update_succ_into_db_log(struct mqdata *data)
{
    if (dos_flag) {
        if (ping_flag) {
            tlog_out("update success. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d\r"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("update success. %s=%lld %s:%d tcping=%.1fms.\r"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    } else {
        if (ping_flag) {
            tlog_out("update success. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("update success. %s=%lld %s:%d time=%.1fms."
                    , field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    }
}
    inline
void insert_fail_into_db_log(struct mqdata *data)
{
    if (dos_flag) {
        if (ping_flag) {
            tlog_out("insert  failed. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d\r", field_id, data->id
                    , data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("insert  failed. %s=%lld %s:%d tcping=%.1fms.\r", field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    } else {
        if (ping_flag) {
            tlog_out("insert  failed. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d"
                    , field_id, data->id, data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("insert  failed. %s=%lld %s:%d tcping=%.1fms."
                    , field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    }
}
    inline
void insert_succ_into_db_log(struct mqdata *data)
{
    if (dos_flag) {
        if (ping_flag) {
            tlog_out("insert success. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d\r", field_id, data->id
                    , data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("insert success. %s=%lld %s:%d tcping=%.1fms.\r", field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    } else {
        if (ping_flag) {
            tlog_out("insert success. %s=%lld %s:%d tcping=%.1fms ping=%.1f loss=%d/%d", field_id, data->id
                    , data->dest, data->port, data->tcping_ms, data->ping_ms, data->ping_loss, ping_count);
        } else {
            tlog_out("insert success. %s=%lld %s:%d time=%.1fms.", field_id, data->id, data->dest, data->port, data->tcping_ms);
        }
    }
}

pthread_mutex_t db_mutex = PTHREAD_MUTEX_INITIALIZER;

int consumer_num = 1;
int consumer_out = 0;

// consumer: mqd_db_into -> db
void * thread_into_db(void *p)
{
    MYSQL * mysql = (MYSQL*)p;
    char sql[1024*4];
    struct mqdata *data;
    unsigned int prio;
    char date[128];

    struct mq_attr db_in_attr;
    struct mq_attr db_into_attr;
    struct mq_attr attr;
    mq_getattr(mqd_db_into, &attr);
    char buff[attr.mq_msgsize];
    data = (struct mqdata*)buff;

    pthread_detach(pthread_self());

    while (1) {
        mq_receive(mqd_db_into, buff, attr.mq_msgsize, &prio);
        --num_count;
        fmt_time(date, sizeof(date));
        if (update_flag) {
            if (ping_flag) {
                snprintf(sql, sizeof(sql), 
                        "UPDATE %s set vc2srcip='%s',%s=%.1f,%s='%s',%s=%.1f,%s=%d,%s='%s',numsignt=%.1f,numsignp=%.1f where %s=%lld;"
                        , db_tab, interface_ip
                        , field_tcping_delay, data->tcping_ms , field_tcping_time, date 
                        , field_ping_delay, data->ping_ms
                        , field_ping_loss, data->ping_loss
                        , field_ping_time, date
                        , data->tcping_ms_real, data->ping_ms_real
                        , field_id, data->id
                        ); 
            } else {
                snprintf(sql, sizeof(sql), 
                        "UPDATE %s set vc2srcip='%s',%s=%.1f,%s='%s',numsignt=%.1f where %s=%lld;"
                        , db_tab , interface_ip
                        , field_tcping_delay, data->tcping_ms , field_tcping_time, date 
                        , data->tcping_ms_real
                        , field_id, data->id
                        ); 
            }
            pthread_mutex_lock(&db_mutex);
            if ( mysql_real_query(mysql, sql, strlen(sql)) ) {
                pthread_mutex_unlock(&db_mutex);
                update_fail_into_db_log(data);
                tlog_out("mysql error %d:%s", mysql_errno(mysql), mysql_error(mysql));
            } else {
                pthread_mutex_unlock(&db_mutex);
                update_succ_into_db_log(data);
            }
        }
        if (insert_flag) {
            if (ping_flag) {
                snprintf(sql, sizeof(sql), 
                        /* db_tab -> field_tcping_time, date */
                        "insert into his_%s set vc2srcip='%s',%s=%lld,%s='%s',%s=%d,%s=%.1f,%s='%s'," 
                        "%s=%.1f,%s=%d,%s='%s',%s='%s'," /* ping args -> field_ping_time, date */
                        "%s=%d,%s='%s',%s=%lld,%s='%s',"  /* field_gamename -> data->batch */
                        "%s=%.1f,%s=%.1f;"   /* real data */
                        , db_tab, interface_ip, field_id, data->id, field_dest, data->dest, field_port, data->port
                        , field_tcping_delay, data->tcping_ms , field_tcping_time, date 
                        , field_ping_delay, data->ping_ms
                        , field_ping_loss, data->ping_loss
                        , field_ping_time, date
                        , field_gamename, data->name
                        , field_numtype,  data->type
                        , field_addr,     data->addr
                        , field_numbatch, data->batch
                        , "dataddtime", date
                        , "numsignt", data->tcping_ms_real
                        , "numsignp", data->ping_ms_real
                        );
            } else  {
                snprintf(sql, sizeof(sql), 
                        "insert into his_%s set vc2srcip='%s',%s=%lld,%s='%s',%s=%d,%s=%.1f,%s='%s',%s='%s',%s=%d,%s='%s',%s=%lld,%s='%s',%s=%.1f;"
                        , db_tab, interface_ip, field_id, data->id, field_dest, data->dest, field_port, data->port
                        , field_tcping_delay, data->tcping_ms , field_tcping_time, date 
                        , field_gamename, data->name
                        , field_numtype,  data->type
                        , field_addr,     data->addr
                        , field_numbatch, data->batch
                        , "dataddtime", date
                        , "numsignt", data->tcping_ms_real
                        );
            }
            pthread_mutex_lock(&db_mutex);
            if ( mysql_real_query(mysql, sql, strlen(sql)) ) {
                pthread_mutex_unlock(&db_mutex);
                insert_fail_into_db_log(data);
                tlog_out("mysql error %d:%s", mysql_errno(mysql), mysql_error(mysql));
            } else {
                pthread_mutex_unlock(&db_mutex);
                insert_succ_into_db_log(data);
            }
        }

        mq_getattr(mqd_db_in, &db_in_attr);
        mq_getattr(mqd_db_into, &db_into_attr);
        if (in_finish && !db_in_attr.mq_curmsgs && !db_into_attr.mq_curmsgs && tcping_is_zero()) {
            goto exit_out;
        } else {
            /* 
               tlog_out("in msgs %ld, into msgs %ld, ntcping=%d", db_in_attr.mq_curmsgs
               , db_into_attr.mq_curmsgs ,tcping_get());  // */
        }
    }

exit_out:
    if (++consumer_out = consumer_num) {
        tlog_out("done.");
        exit(0);
    }
    return (void*)0;
}

void init_consumer(int multi_num)
{
    pthread_t tid;
    pthread_attr_t attr;
    int n, err;

    if ( (err = pthread_attr_init(&attr)) != 0)
        tlog_out_exit(1, "pthread_attr_init error: %s", strerror(err));
    if ( (err = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE)) != 0)
        tlog_out_exit(1, "pthread_attr_setstacksize error: %s", strerror(err));


    MYSQL * mysql = 0;

    if ( !(mysql = mysql_init((MYSQL*)0)) ) {
        tlog_out("mysql init error");
        exit(1);
    }
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8");
    if ( !(mysql_real_connect(mysql, db_ip, db_user, db_pass, db_name, db_port, NULL, CLIENT_LOCAL_FILES)))
        tlog_out_exit(1, "push result, mysql connect error !!!");

    for (n = 0; n < multi_num; n++) {
        if ((err = pthread_create(&tid, &attr, thread_into_db, mysql)) != 0)
            tlog_out_exit(1, "pthread_create thread_into_db error: %s", strerror(err));
    }
}

void worker_environment()
{
    init_worker_mqueue();
    init_producer(multi_num);

    if (multi_num >= 10)
        consumer_num = multi_num/10;

    init_consumer(consumer_num);
}

ssize_t get_exe_path(char *buff, int len)
{
    ssize_t ret; 
    ret = readlink("/proc/self/exe", buff, len - 1); 
    buff[len-1] = '\0';

    if (ret < 0 || ret > len - 1)
        return -1; 

    return ret;
}
ssize_t get_exe_dir(char *buff, int len)
{
    ssize_t ret; 
    char *p; 
    ret = readlink("/proc/self/exe", buff, len - 1); 

    if (ret < 0 || ret > len - 1)
        return -1; 

    if ((p = strrchr(buff, '/')))
        *p = 0;
    return ret;
}

// producer: db -> mqd_db_in
int worker_data_from_db()
{
    MYSQL * mysql = 0;
    MYSQL_RES *mysql_res = 0;
    MYSQL_ROW mysql_row;

    struct mqdata data = {0} ;

    int num_row, num_col;
    int line;
    char sql[1024*4];

    long ncount = 0;
    int n = 0;

    if ( !(mysql = mysql_init((MYSQL*)0)) ) {
        tlog_out("mysql init error");
        exit(1);
    }

    //mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "gbk");
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8");

    if ( !(mysql_real_connect(mysql, db_ip, db_user, db_pass, db_name, db_port, NULL, CLIENT_LOCAL_FILES)))
        tlog_out_exit(1, "fetch src, mysql connect error !!!");

    snprintf(sql, sizeof(sql), "SELECT count(*) FROM %s", db_tab);
    pthread_mutex_lock(&db_mutex);
    if ( mysql_real_query(mysql, sql, strlen(sql)) ) {
        pthread_mutex_unlock(&db_mutex);
        tlog_out("mysql_real_query() error");
        goto err_out;
    }
    pthread_mutex_unlock(&db_mutex);

    if ( 0 == (mysql_res = mysql_store_result(mysql)) ) {
        tlog_out("mysql_store_result() error");
        goto err_out;
    }
    if((mysql_row = mysql_fetch_row(mysql_res)) == 0) {
        //mysql_free_result(mysql_res); // 此处加上 valgrind 会报警告,不应该啊！！！
        goto err_out;
    }
    //mysql_free_result(mysql_res);  // 此处加上 valgrind 会报警告,不应该啊！！！
    num_count = atol(mysql_row[0]);
    tlog_out("list count records %ld", num_count);
    if (num_count <= 0)
        goto ret_out;

    while (1) {
        n = snprintf(sql, sizeof(sql), "SELECT %s,%s,%s,%s,%s,%s,%s FROM %s"
                , field_id, field_dest, field_port, field_gamename
                , field_numtype, field_addr, field_numbatch , db_tab);

        if (x_flag && y_flag)
            snprintf(sql+n, sizeof(sql)-n, " where %s>=%lld && %s<=%lld LIMIT %ld,%d;"
                    , field_id, x_id, field_id, y_id , ncount, page_line);
        else if (x_flag)
            snprintf(sql+n, sizeof(sql)-n, " where %s>=%lld LIMIT %ld,%d;"
                    , field_id, x_id , ncount, page_line);
        else if (y_flag)
            snprintf(sql+n, sizeof(sql)-n, " where %s<=%lld LIMIT %ld,%d;"
                    , field_id, y_id , ncount, page_line);
        else
            snprintf(sql+n, sizeof(sql)-n, " LIMIT %ld, %d" , ncount, page_line);

        //tlog_out(sql);

        pthread_mutex_lock(&db_mutex);
        if ( mysql_real_query(mysql, sql, strlen(sql)) ) {
            pthread_mutex_unlock(&db_mutex);
            tlog_out("mysql_real_query() error %d: %s", mysql_errno(mysql), mysql_error(mysql));
            goto err_out;
        }
        pthread_mutex_unlock(&db_mutex);

        if ( 0 == (mysql_res = mysql_store_result(mysql)) ) {
            tlog_out("mysql_store_result() error");
            goto err_out;
        }

        num_row = mysql_num_rows(mysql_res);
        num_col = mysql_num_fields(mysql_res);

        if ( num_row <= 0 || num_col <= 0 ) 
            break;

        for(line = 0; line < num_row; line++)
        {
            if((mysql_row = mysql_fetch_row(mysql_res)) == 0) 
                break;

            if (!mysql_row[0] || !mysql_row[1] || !mysql_row[2]) {
                tlog_out("data error: %s %s %s", mysql_row[0], mysql_row[1], mysql_row[2]);
                continue;
            }

            data.id = atoll(mysql_row[0]);
            // vc2ip
            snprintf(data.dest, DEST_LEN, "%s", mysql_row[1]);
            // numport
            data.port = atoi(mysql_row[2]);

            // vc2gamename
            if (!mysql_row[3])  
                snprintf(data.name, sizeof(data.name), "%s", "");
            else                
                snprintf(data.name, sizeof(data.name), "%s", mysql_row[3]);
            // numtype
            if (!mysql_row[4])  
                data.type = -1;
            else                
                data.type = atoi(mysql_row[4]);
            // vc2addr
            if (!mysql_row[5])  
                snprintf(data.addr, sizeof(data.addr), "%s", "");
            else                
                snprintf(data.addr, sizeof(data.addr), "%s", mysql_row[5]);
            // numbatch
            if (!mysql_row[6])  
                data.batch = -1;
            else                
                data.batch = atoll(mysql_row[6]);
            //tlog_out("  %06lld  %s:%d, %s,%s,%d,%lld", data.id, data.dest, data.port, data.name, data.addr, data.type,data.batch);  
            mq_send(mqd_db_in, (char*)&data, sizeof(data), 0);
        }
        mysql_free_result(mysql_res);
        ncount += page_line;
    }

ret_out:
    tlog_out("load data from db done.");
    mysql_close(mysql);
    return 0;   

err_out:
    tlog_out("mysql error %d:%s", mysql_errno(mysql), mysql_error(mysql));
    mysql_close(mysql);
    return -1;
}

int chk_line(char *line, char *note)
{
    int len = strlen(note);
    char * p = line;

    int chars = 0;

    if (p == 0)  
        return -1; 

    for (; *p; ++p) {
        if (strncmp(p, note, len) == 0) { *p++ = '\n'; *p = 0; break; }
        else if (isgraph(*p)) ++ chars; 
        else if (*p == ' ' || *p == '\t' || *p == '\n') continue;
    }   
    if (!chars)  {
        //printf("null line\n");
        return -1; 
    }   
    return 0;
}

    inline 
int extract_destination_port(const char * p, char *dest, int len, int *port)
{
    char *pcolon;
    *port = -1;
    memset(dest, 0, len);
    if (!(pcolon = strstr((char*)p, ":"))) {
        // illegal line, efficient format: 
        //      1, ip-addr:port     like  111.222.111.123:8080
        //      2, domain:port      like  www.baidu.com:80  
        return -1;
    } else {
        sscanf(p, "%[^:]:%d", dest, port);
        if ((dest[0] && dest[len-1]) || *port <= 0) {
            // overflow or error
            return -2;
        }
    }
    return 0;
}

int tcping(const char *ip, int port, int count, float *tcping_ms) 
{
    int sockfd;
    struct sockaddr_in addr;
    //struct hostent *host;
    int error = 0;
    int ret;
    socklen_t errlen;
    struct timeval timeout;
    fd_set fdrset, fdwset;
    int verbose = extended;
    long timeout_sec=5, timeout_usec=0;

    int timeout_count = 0;
    float fpin = 99999;
    float ftmp;

    struct ifreq interface;

    struct timeval tbgn, tend;

    *tcping_ms = -1;

    memset(&addr, 0, sizeof(addr));

    //struct sockaddr_in addr;
    ret = inet_pton(AF_INET, ip, &addr.sin_addr.s_addr);
    switch (ret) {
        case 0:
            tlog_out("无效ip地址: %s", ip);
            return -1;
        case -1:
            tlog_err_out("inet_pton() error");
            return -1;
        default:
            break;
    }
    addr.sin_family = AF_INET; 
    addr.sin_port = htons(port);

    if (interface_flag) {
        memset(&interface, 0, sizeof(struct ifreq));
        strncpy(interface.ifr_ifrn.ifrn_name, interface_name, IFNAMSIZ);
    }

    if (count < 1) {
        if (verbose)
            tlog_out("correction count=1, original count=%d: count must > 1", count);
        count = 1;
    }
    while (count > 0) {
        sockfd = socket (AF_INET, SOCK_STREAM, 0);
        if (interface_flag) {
            if (setsockopt(sockfd, SOL_SOCKET, SO_BINDTODEVICE, &interface, sizeof(interface)) < 0) {
                tlog_out("setsockopt SO_BINDTODEVICE %s error %d:%s\n", interface_name, errno, strerror(errno));
                close(sockfd);
                return -1;
            }
        }
        fcntl(sockfd, F_SETFL, O_NONBLOCK);
        if ((ret = connect(sockfd, (struct sockaddr *) &addr, sizeof(addr))) != 0) {
            if (errno != EINPROGRESS) {
                if (verbose)
                    tlog_out("error: %s:%d: %s", ip, port, strerror(errno));
                close(sockfd);
                return (-1);
            }

            FD_ZERO(&fdrset);
            FD_SET(sockfd, &fdrset);
            fdwset = fdrset;

            timeout.tv_sec=timeout_sec + timeout_usec / 1000000;
            timeout.tv_usec=timeout_usec % 1000000;

            gettimeofday(&tbgn, 0); 
            if ((ret = select(sockfd+1, &fdrset, &fdwset, NULL, 
                            timeout.tv_sec+timeout.tv_usec > 0 ? &timeout : NULL)) == 0) {
                /* timeout */
                if (verbose) {
                    tlog_out("%s port %d user timeout.", ip, port);
                    timeout_count++;
                }
                close(sockfd);
                goto __next;
                return(-2);
            }
            gettimeofday(&tend, 0); 
            if (FD_ISSET(sockfd, &fdrset) || FD_ISSET(sockfd, &fdwset)) {
                errlen = sizeof(error);
                if ((ret=getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &errlen)) != 0) {
                    /* getsockopt error */
                    if (verbose)
                        tlog_out("error: %s:%d: getsockopt: %s", ip, port, strerror(errno));
                    close(sockfd);
                    return(-1);
                }
                if (error != 0) {
                    if (verbose) {
                        tlog_out("%s port %d closed.", ip, port);
                        timeout_count++;
                    }
                    close(sockfd);
                    goto __next;
                    return(-1);
                }
            } else {
                if (verbose)
                    tlog_out("error: select: sockfd not set");
                close(sockfd);
                return(-1);
            }
        }
        ftmp = (tend.tv_sec - tbgn.tv_sec) * 1000 + (float)(tend.tv_usec - tbgn.tv_usec)/1000;
        ftmp < fpin ? fpin = ftmp: 0 ;

        /* OK, connection established */
        close(sockfd);
        if (fpin > 99998)
            *tcping_ms = -1;
        else 
            *tcping_ms = fpin;
__next:
        count --;
    }

    return 0;
}

