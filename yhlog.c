#include "yhlog.h"

FILE *__yhlog_fp    =  0;  // log FILE *
int   __yhlog_fd    = -1;  // for ftruncate
long long  __yhlog_len =  0;  // for ftruncate size limited

#define Kbytes(kb)  (1024L * kb)
#define Mbytes(mb)  (Kbytes(1024L) * mb)

int open_log(const char *path, long MBlen)
{
    /* if ((fp_log = freopen(path, "a", stdout)) == 0) */
    if ((__yhlog_fp = fopen(path, "a")) == 0) {
        fprintf(stderr, "open_log() failed: %s: %s\n", path, strerror(errno));
        return -1;
    }
    setlinebuf(__yhlog_fp);
    __yhlog_fd    = fileno(__yhlog_fp);
    __yhlog_len = Mbytes(MBlen);
    return 0;
}

static inline
char * fmt_time(char *p, size_t len)
{
    time_t timee;
    struct tm tmm;

    timee = time(0);
    localtime_r(&timee, &tmm);
    strftime(p, len, "%Y-%m-%d %H:%M:%S", &tmm);
    return p;
}

void dolog(FILE *fp_log, int _errno, const char *fmt, va_list vap)
{
    char buff[4096];
    vsnprintf(buff, sizeof(buff), fmt, vap);
    if (_errno != 0)
        snprintf(buff+strlen(buff), sizeof(buff)-strlen(buff), 
                ": %s", strerror(_errno));
    strcat(buff, "\n"); // ****

    if (__yhlog_fd >= 0 && __yhlog_len > 0) {
        struct stat st;
        if (fstat(__yhlog_fd, &st) != -1) {
            if (st.st_size > __yhlog_len) 
                ftruncate(__yhlog_fd, 0);
        }
    } // end of if (__yhlog_fd >= ...

    if (fp_log)
        fputs(buff, fp_log);
    else
        fprintf(stdout, "%s", buff);
}
void dologn(FILE *fp_log, int _errno, const char *fmt, va_list vap)
{
    char buff[4096];
    vsnprintf(buff, sizeof(buff), fmt, vap);
    if (_errno != 0)
        snprintf(buff+strlen(buff), sizeof(buff)-strlen(buff), 
                ": %s", strerror(_errno));
    if (__yhlog_fd >= 0 && __yhlog_len > 0) {
        struct stat st;
        if (fstat(__yhlog_fd, &st) != -1) {
            if (st.st_size > __yhlog_len) 
                ftruncate(__yhlog_fd, 0);
        }
    } // end of if (__yhlog_fd >= ...

    if (fp_log)
        fputs(buff, fp_log);
    else
        fprintf(stdout, "%s", buff);
}

void logout(FILE *fp_log, int _errno, const char *fmt, ...)
{
    va_list vap;
    va_start(vap, fmt);
    dolog(fp_log, _errno, fmt, vap);
    va_end(vap);
}
void tlogout(FILE *fp_log, int _errno, const char *fmt, ...)
{
    va_list vap;
    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(fp_log, _errno, fmt, vap);
    va_end(vap);
}	

/*
 * output log
 */
void log_out(const char *fmt, ...)
{
    va_list vap;

    va_start(vap, fmt);
    dolog(__yhlog_fp, 0, fmt, vap);
    va_end(vap);
}

/*
 * output log appent time
 */
void tlog_out(const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(__yhlog_fp, 0, sfmt, vap);
    va_end(vap);
}
void tlog_outn(const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dologn(__yhlog_fp, 0, sfmt, vap);
    va_end(vap);
}

/*
 * output log and exit
 */
void log_out_exit(int exit_code, const char *fmt, ...)
{
    va_list vap;

    va_start(vap, fmt);
    dolog(__yhlog_fp, 0, fmt, vap);
    va_end(vap);
    exit(exit_code);
}

/*
 * output log ,time and exit
 */
void tlog_out_exit(int exit_code, const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(__yhlog_fp, 0, sfmt, vap);
    va_end(vap);
    exit(exit_code);
}

/*
 * output log and time
 */
void log_err_out(const char *fmt, ...)
{
    va_list vap;

    va_start(vap, fmt);
    dolog(__yhlog_fp, errno, fmt, vap);
    va_end(vap);
}

/*
 * output log, time, error message
 */
void tlog_err_out(const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(__yhlog_fp, errno, sfmt, vap);
    va_end(vap);
}

/*
 * output log and exit
 */
void log_err_out_exit(int exit_code, const char *fmt, ...)
{
    va_list vap;

    va_start(vap, fmt);
    dolog(__yhlog_fp, errno, fmt, vap);
    va_end(vap);
    exit(exit_code);
}

/*
 * output log, appent time, error info and exit
 */
void tlog_err_out_exit(int exit_code, const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(__yhlog_fp, errno, sfmt, vap);
    va_end(vap);
    exit(exit_code);
}

//////////////////

/*
 * output log and time
 * use nerr
 */
void log_nerr_out(int nerr, const char *fmt, ...)
{
    va_list vap;

    va_start(vap, fmt);
    dolog(__yhlog_fp, nerr, fmt, vap);
    va_end(vap);
}

/*
 * output log, time, error message
 * use nerr
 */
void tlog_nerr_out(int nerr, const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(__yhlog_fp, nerr, sfmt, vap);
    va_end(vap);
}

/*
 * output log and exit
 * use nerr
 */
void log_nerr_out_exit(int nerr, int exit_code, const char *fmt, ...)
{
    va_list vap;

    va_start(vap, fmt);
    dolog(__yhlog_fp, nerr, fmt, vap);
    va_end(vap);
    exit(exit_code);
}

/*
 * output log, appent time, error info and exit
 * use nerr
 */
void tlog_nerr_out_exit(int nerr, int exit_code, const char *fmt, ...)
{
    va_list vap;

    char stime[256];
    char sfmt[1024];

    va_start(vap, fmt);
    fmt_time(stime, sizeof(stime));
    snprintf(sfmt, sizeof(sfmt), "%s %s", stime, fmt);
    dolog(__yhlog_fp, nerr, sfmt, vap);
    va_end(vap);
    exit(exit_code);
}

