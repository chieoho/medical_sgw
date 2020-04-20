/*
 * 从 md5 记录文件中，根据文件的路径名找到文件对应的 md5
 *
 * 存放文件路径名和对应的 md5 的文件中，一行的内容可能是完整的“路径名
 * md5”，或者“上一次没有写完的字符 路径名 md5”；“路径名 md5”在一行中是
 * 一个完整的字符串。程序的任务是在一行的内容中，根据文件路径名找到对
 * 应的 md5 值。
 *
 * 注意：程序假设不会有线程同时对一个文件进行读写操作。要么是多个线程
 * 同时追加写，要么是多个线程同时读一个文件。
 *
 * mike
 * 2019-11-19
 */

#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "mt_log.h"

int md5path(const char *abspath, char *out)
{
    int len = strlen(abspath);
    while (len >= 0) {
        if (abspath[len] == '/') {
            memmove(out, abspath, len);
            sprintf(out + len, "/%s", "md5sum.txt");
            return 0;
        } else {
            len = len - 1;
        }
    }
    return -1;
}

/* 注意！这个函数会有可能永远不能完成！ */
static void writeall(int fd, const char *buf, size_t count)
{
    ssize_t nwr = 0;
    while (nwr != (ssize_t)count) {
        /*
         * 每次写都是一个原子操作，如果之前写的不完全，就忽略之前部分
         * 写的内容，直到写入的字节数和要求的字节数一样，才算成功。
         */
        nwr = write(fd, buf, count);
    }
}

int savemd5(const char *filename, const char *md5)
{
    char buffer[2048];
    char md5_path[2048];
    int rc = md5path(filename, md5_path);
    if (rc == 0) {
        int fd = open(md5_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd >= 0) {
            /* sprintf() 的返回值不包括字符串的结束符 '\0' */
            /* 写入的字节数是文件名（200字节），md5校验值（32字节），换行符*/
            int npr = sprintf(buffer, "%s %s\n", filename, md5);
            writeall(fd, buffer, npr);
            close(fd);
            return 0;
        } else {
            log_warning("savemd5 failed: file %s, md5 %s", filename, md5);
            return -1;
        }
    } else {
        log_error("md5path failed: filename %s", filename);
        return -1;
    }
}

static int is_valid_md5(const char *md5)
{
    int i;
    for (i = 0; i < 32; i++) {
        if (isxdigit(md5[i])) {
            /* 检查的字符是 md5 校验值的合法字符，继续处理 */
        } else {
            /* 检测到不是合法的 md5 字符，返回告知调用者 */
            return 0;
        }
    }
    return 1;
}

int find_target(const char *line, const char *target, char *md5)
{
    int linelen = strlen(line);
    int targetlen = strlen(target);
    int end = linelen - targetlen - 32/*md5*/ - 2/*空格+换行符*/;
    int i;
    for (i = 0; i <= end; i++) {
        int rc = memcmp(line + i, target, targetlen);
        if (rc == 0) {
            /* 发现匹配的目标 */
            const char *target_md5 = line + i + targetlen + 1/* 跳过空格 */;
            int rc2 = is_valid_md5(target_md5);
            if (rc2 == 1) {
                /* 目标的 md5 位置字符合法，当作 md5 填充到输出 */
                /* printf("==> %s: %s", target, target_md5); */
                memmove(md5, target_md5, 32);
                return 0;
            } else {
                /* 目标的 md5 位置字符不是合法的 md5 值，继续匹配 */
                /* printf("skip %s: not a valid md5\n", target_md5); */
            }
        } else {
            /* 此次不匹配，继续进行下一次匹配 */
            /* printf("%s: index %d not match\n", line, i); */
        }
    }
    return -1;
}

/*
 * 从 filepath 的内容中，找到 target 对应的 md5 校验值
 *
 * filepath 的文件内容的一个例子：
 *
 * /a/b/c/111.txt 1234abcd
 * /a/b/c/222./a/b/d/222.txt 112233aabbcc
 *
 * 第二行的内容可能会出现在写入文件时调用 write() 没有能够完整地写入。
 *
 * 如果找到目标对应的 md5 值，则返回 0，同时设置传入参数 md5；如果没有
 * 找到目标对应的 md5，则返回 -1。
 */
int __look_for_md5(FILE *fp, const char *target, char *md5)
{
    char *line;
    size_t len;
    ssize_t n;
    int retcode;

    line = NULL, len = 0, n = 0, retcode = -1;
    while (n != -1) {
        n = getline(&line, &len, fp);
        if (n >= 35) {
            /*
             * 32个字节的md5，1个字节的空格，1个字节的换行符，至少1个
             * 字节的文件名
             */
            int rc = find_target(line, target, md5);
            if (rc == 0) {
                /*
                 * 找到了一个匹配的 md5，输出，同时结束处理。因为一行完整的“路径
                 * 名 md5”可能会出现在多行中（一个文件重复上传就会出现这种情况），
                 * 只认为最后出现的“路径名 md5”才是有效的值。
                 */
                retcode = 0;
            } else {
                /* 当前行没有匹配，继续处理下一行 */
            }
        } else {
            /* 读取到的不是一行合法值，忽略，继续处理下一行 */
        }
    }

    free(line);
    return retcode;
}

int look_for_md5(const char *filepath, const char *target, char *md5)
{
    FILE *fp = fopen(filepath, "r");
    if (fp) {
        int rc = __look_for_md5(fp, target, md5);
        if (rc == 0) {
            fclose(fp);
            return 0;
        } else {
            fclose(fp);
            return -1;
        }
    } else {
        log_error("fopen %s failed: %s", filepath, strerror(errno));
        return -1;
    }
}
