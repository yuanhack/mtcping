#ifndef __OPT_LOG_H_H
#define __OPT_LOG_H_H

#include <stdarg.h> 		/* for va_list */

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C"
{
#endif


    extern FILE *__log_fp; //log file

    int  open_log(const char *path, long MBlen);

    void dolog(FILE *fp_log, int _errno, const char *fmt, va_list vap);

    void logout(FILE *fp_log, int _errno, const char *fmt, ...);
    void tlogout(FILE *fp_log, int _errno, const char *fmt, ...);

    void log_out(const char *fmt, ...);
    void tlog_out(const char *fmt, ...);
    void log_out_exit(int exit_code, const char *fmt, ...);
    void tlog_out_exit(int exit_code, const char *fmt, ...);


    void tlog_outn(const char *fmt, ...);

    void log_err_out(const char *fmt, ...);
    void tlog_err_out(const char *fmt, ...);
    void log_err_out_exit(int exit_code, const char *fmt, ...);
    void tlog_err_out_exit(int exit_code, const char *fmt, ...);

    void log_nerr_out(int nerrno, const char *fmt, ...);
    void tlog_nerr_out(int nerrno, const char *fmt, ...);
    void log_nerr_out_exit(int nerrno, int exit_code, const char *fmt, ...);
    void tlog_nerr_out_exit(int nerrno, int exit_code, const char *fmt, ...);


#ifdef __cplusplus
}
#endif

#endif /* __OPT_LOG_H_H */
