#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include <endian.h>

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

#include "pathops.h"
#include "public.h"
#include "mt_log.h"

extern char *default_md5sum_filename;

unsigned int crc32(const char *s, unsigned int len)
{
    /* 生成 CRC32 的查询表 */
    static unsigned int table[256];
    static bool needinit = true;
    if (needinit) {
        int i, j;
        unsigned int crc;
        for (i = 0; i < 256; i++) {
            crc = i;
            for (j = 0; j < 8; j++) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc = crc >> 1;
                }
            }
            table[i] = crc;
        }
        needinit = false;
    } else {
        /* 查询表已经初始化，无需再次初始化 */
    }
    /* 开始计算 CRC32 校验值 */
    unsigned int crc = 0xFFFFFFFF;
    unsigned int k;
    for (k = 0; k < len; k++) {
        int x = (crc & 0xff) ^ s[k];
        crc = (crc >> 8) ^ table[x];
    }
    crc = crc ^ 0xFFFFFFFF;
    return crc;
}

void map1(const char *s, int size, int *x1, int *x2)
{
    unsigned int crc = crc32(s, size);
    unsigned int key = crc % 4096;
    *x1 = key / 64;             /* 第一级的 0~63 */
    *x2 = key % 64;             /* 第二级的 0~63 */
}

void map2(const char *buf, unsigned int len, int *x3, int *x4)
{
    /* 第三级的 0~99 */
    unsigned int i;
    int v;
    v = 0;
    for (i = 0; i < len; i++) {
        v = v * 31 + buf[i];
    }
    v = v % 101;
    if (v < 0) {
        /* 发生了溢出 */
        v = v + 101;
    } else {
        /* 没有溢出 */
    }
    *x3 = v;

    /* 第四级的 0~99（可能回到103） */
    unsigned int j;
    int z;
    z = 0;
    for (j = 0; j < len; j++) {
        z = z * 37 + buf[j];
    }
    z = z % 103;
    if (z < 0) {
        /* 发生了溢出 */
        z = z + 103;
    } else {
        /* 没有溢出 */
    }
    *x4 = z;
}

void maplevel(const char *buf, unsigned int len,
              int *x1, int *x2, int *x3, int *x4)
{
    map1(buf, len, x1, x2);
    map2(buf, len, x3, x4);
}

/*
 * 根据给定的挂载点（mountpoiint），检查号（studyid），检查的系列号
 * （serial）计算出路径名。计算出来的结果放在用户传入的缓冲区中。
 *
 * 注意：挂载点要求是一个绝对路径。
 *
 * 如果计算成功，返回 0；如果出现错误，返回 -1。
 */
int calcpath(
    const char *mountpoint, const char *studyid, const char *serial,
    char *resultpath, int *resultlen)
{
    int studylen = strlen(studyid);
    int leftsize = *resultlen;
    char *unused = resultpath;

    int x1, x2, x3, x4;
    maplevel(studyid, studylen, &x1, &x2, &x3, &x4);
    char buf[1024];   /* XXX: 路径名不能超过 1024，暂时这样实现 */
    int pathlen;
    /* 挂载点 盘符 四级目录 检查号 系列号 */
    if (serial) {
        /* pathlen 不包括空字符 '\0' */
        pathlen = snprintf(
            buf, sizeof(buf), "%s/%d/%d/%d/%d/%s/%s",
            mountpoint, x1, x2, x3, x4, studyid, serial);
    } else {
        /* 没有系列号，直接查找检查号 */
        pathlen = snprintf(
            buf, sizeof(buf), "%s/%d/%d/%d/%d/%s",
            mountpoint, x1, x2, x3, x4, studyid);
    }
    if (leftsize >= pathlen + 1/*空字符'\0'*/) {
        if (!access(buf, F_OK)) {
            memmove(unused, buf, pathlen+1/*包括字符串结束符'\0'*/);
            unused = unused + pathlen + 1;
            leftsize = leftsize - pathlen - 1;
        } else {
            /* 路径不存在，忽略之，不填入缓冲区，继续下一次处理 */
            // log_info("skip %s: no such path", buf);
        }
    } else {
        /* 用户给出的缓冲区已经没有空间，不填充结果，返回错误 */
        return -1;
    }

    *resultlen =  *resultlen - leftsize;
    return 0;
}

void test1(void)
{
    const char *mountpoint = "/sgw.1";
    const char *studyid = "1.2.826.0.1.3680043.2.461.9701983.3645589902";
    const char *serial = "serial111";

    char buffer[16384];
    int buflen = 16384;
    calcpath(mountpoint, studyid, serial, buffer, &buflen);
    int leftsize = buflen;
    char *next = buffer;
    while (leftsize > 0) {
        int n = printf("%s", next);
        printf("\n");
        leftsize = leftsize - n - 1;
        next = next + n + 1;
    }
}

/*
 * 输入参数形如 "studyid/serial" 这样的字符串，返回指向 studyid 和
 * serial 的起始地址。
 */
void split_serial(char *s, char **studyid, char **serial)
{
    char *s1 = s;
    while (*s1 != '\0' && *s1 != '/') {
        s1 = s1 + 1;
    }
    if (*s1 == '\0') {
        /* 没有 serial */
        *studyid = s;
        *serial = NULL;
    } else {
        /* studyid/serial */
        *s1 = '\0';
        *studyid = s;
        char *s2 = s1 + 1;
        while (*s2 == '/') {
            s2 = s2 + 1;
        }
        if (*s2 == '\0') {
            *serial = NULL;
        } else {
            *serial = s2;
            /* 处理 studyid/serial////xxxx 这种情况，只保留 serial */
            while (*s2 != '/' && *s2 != '\0') {
                s2 = s2 + 1;
            }
            if (*s2 == '/') {
                *s2 = '\0';
            } else {
                /* 到了字符串末尾，不做处理 */
            }
        }
    }
}

/* 从绝对路径 abspath 中剪除挂载点路径 mountpoint */
void cut_mount_path(char *abspath, const char *mountpoint)
{
    int mountlen = strlen(mountpoint);
    char *mountpath = malloc(mountlen + 1);
    assert(mountpath);
    memmove(mountpath, mountpoint, mountlen + 1);
    char *c = mountpath + mountlen - 1;
    int n = 0;
    while (c >= mountpath && *c == '/') {
        *c = '\0';
        n = n + 1;
        c = c - 1;
    }
    mountlen = mountlen - n;
    if (!memcmp(abspath, mountpath, mountlen)) {
        int abslen = strlen(abspath);
        int reallen = abslen - mountlen;
        memmove(abspath, &abspath[mountlen], reallen);
        abspath[reallen] = '\0';
    } else {
        /* 挂载点不匹配，不做处理 */
    }
    free(mountpath);
}

/*
 * 2 个字节的文件名长度，因为一个字节最多表示 31 个字节的文件名长度。
 * 文件名长度如果是负数，则表示出错了。
 */
static int fill_file_list(char *out, char *end,
                           char *filename, int64_t filesize)
{
    int namelen = strlen(filename);
    if (out + 2/*文件名长度*/ + namelen + 8/*文件内容长度*/ <= end) {
        *((int16_t *)out) = htobe16(namelen);
        out = out + 2;
        (void) memmove(out, filename, namelen);
        out = out + namelen;
        *((int64_t *)out) = htobe64(filesize);
        return namelen + 10;
    } else {
        /* 缓冲区的长度不够，返回错误 */
        log_error("buffer is too small!");
        return -1;
    }
}

/*
 * 根据给出的目录路径名，找出目录下的所有文件名。每个文件获取大小，按
 * 照特定的格式输出。文件列表格式如下：
 *
 * (5 hello 14) (4 abcd 102) (3 123 4096)
 *
 * 注意：文件列表的路径是绝对路径，并且是剪除了挂载点目录后的绝对路径。
 * 如果挂载点在路径名不存在，则不做剪除操作。
 *
 * 返回值：
 *
 *  0 - 成功
 * -1 - 打开目录失败
 * -2 - 读取目录项失败
 * -3 - 获取子目录失败（已废弃）
 * -4 - 缓冲区太小
 */
int get_dir_list(
    const char *dirpath, const char *mountpath,
    char *list, int listlen,
    struct file_list_result *res)
{
    int rc;
    int retcode = 0;
    int32_t nr_files = 0;
    DIR *dp = opendir(dirpath);
    if (dp) {
        struct dirent entry;
        struct dirent *result;
        char *out = list;
        char *end = list + listlen;
        for (;;) {
            int rc = readdir_r(dp, &entry, &result);
            if (rc == 0) {
                if (!result) {
                    /* 读取目录项完毕，可以关闭目录 */
                    assert(nr_files >= 0);
                    res->nr_files = nr_files;
                    res->used_buflen = out - list;
                    retcode = 0;
                    goto out;
                } else {
                    /*
                     * 现在目录项中的一项内容已经读取到 entry 中，可以
                     * 在 entry.d_name 读取文件名
                     */
                    struct stat s;
                    char filepath[2048];
                    snprintf(filepath, sizeof(filepath), "%s/%s", dirpath, entry.d_name);
                    int rc = stat(filepath, &s);
                    if (rc == 0) {
                        if (S_ISREG(s.st_mode)) {
                            if (!strcmp(entry.d_name, default_md5sum_filename)) {
                                /* 不返回生成的，用来记录 md5 校验和的文件 */
                            } else {
                                nr_files = nr_files + 1;
                                // log_info("get_dir_list: %d: %s %lld", nr_files, filepath, (long long int)s.st_size);
                                cut_mount_path(filepath, mountpath);
                                int filllen = fill_file_list(
                                    out, end, filepath, s.st_size);
                                if (filllen > 0) {
                                    /* 缓冲区内容有填充，更新缓冲区指针 */
                                    out = out + filllen;
                                } else {
                                    /* 出错了，缓冲区太小了？ */
                                    log_error("fill %s failed", filepath);
                                    retcode = -4;
                                    goto out;
                                }
                            }
                        } else if (S_ISDIR(s.st_mode)) {
                            if (!strcmp(entry.d_name, ".") || !strcmp(entry.d_name, "..")) {
                                /* 跳过当前目录和上一级目录 */
                            } else {
                                struct file_list_result res;
                                int rc = get_dir_list(
                                    filepath, mountpath, out, end - out, &res);
                                if (rc != 0) {
                                    /* 嗯，递归调用出错了！提前终止 */
                                    log_error("get_dir_list: get %s file list failed", entry.d_name);
                                    retcode = rc;
                                    goto out;
                                } else {
                                    /*
                                     * 递归获取目录成功，文件列表已经放在
                                     * 缓冲区中，更新文件数量和缓冲区长度
                                     */
                                    nr_files = nr_files + res.nr_files;
                                    out = out + res.used_buflen;
                                }
                            }
                        } else {
                            /* 忽略其他文件 */
                            log_error("skip %s: is not a regular file or directory",
                                      entry.d_name);
                        }
                    } else {
                        /* 获取文件的元信息失败，继续处理下一个文件 */
                        int ec = errno;
                        log_error("skip %s: %s", filepath, strerror(ec));
                    }
                }
            } else {
                int ec = errno;
                log_error("readdir_r %s failed on %s: %s",
                          dirpath, entry.d_name, strerror(ec));
                retcode = -2;
                goto out;
            }
        }
        return 0;
    } else {
        int ec = errno;
        log_error("opendir %s failed: %s", dirpath, strerror(ec));
        return -1;
    }
    /* 程序不会运行到这里 */
    assert(0);

out:
    rc = closedir(dp);
    if (rc == -1) {
        int ec = errno;
        log_error("closedir %s failed: %s", dirpath, strerror(ec));
    } else {
        /* 关闭目录成功，不需要特别处理 */
    }
    return retcode;
}

/*
 * 依次每次获取目录下的文件列表，所有目录都处理完毕之后，再填充缓冲区
 * 头部的消息包长度和文件列表个数
 *
 * (256) (3) (5 hello 12) (3 abc 123) (4 1234 4096)
 */
char *fill_many_dir_list(
    const char *mountpath, const char *dirs[], int nr_dir,
    struct file_list_result *out)
{
    /* 一个系列最多 3000 个文件，假设一个检查最多 10 个系列，每个文件
     * 的路径名最多 256 字节，最多有 23 个盘符。
     *
     * 文件列表的最大大小=3000*10*256*23=176640000字节=172500KB=169MB
     *
     * 文件列表的最小大小=3000*1*256*1=768000字节=750KB
     */
    int nr_files = 0;           /* 记录获取的文件列表数量 */
    int used_buflen = 0;        /* 已使用的消息包缓冲区长度 */
    char *buffer = NULL;        /* 消息包缓冲区 */
    int maxbufsize = 170 * 1024 * 1024; /* 消息包的最大缓冲区大小：170MB */
    int bufsize = 2 * 1024 * 1024;      /* 首次尝试使用的消息包缓冲区是 2MB */
    int nr_scan = 0;                    /* 成功扫描的目录个数 */

    buffer = malloc(bufsize);
    if (!buffer) {
        log_error("malloc %d bytes failed", bufsize);
        return NULL;
    } else {
        /* 缓冲区分配成功，继续执行，获取目录下的文件列表 */
    }

    /*
     * 如果获取单个目录下的文件列表失败，就加大缓冲区重试，已获取的文
     * 件列表拷贝到新的缓冲区中。当获取某个目录下的文件列表超过了最大
     * 重试次数，则放弃这个目录，进行处理下一个目录。
     */
    int i;
    for (i = 0; i < nr_dir; i++) {
        const char *dirpath = dirs[i];
        struct file_list_result res;
        char *start;
        int buflen;
    again:
        /* 8字节的消息包长度，4字节的文件列表长度 */
        start = buffer + 8 + 4 + used_buflen;
        buflen = bufsize - 8 - 4 - used_buflen;
        int rc = get_dir_list(dirpath, mountpath, start, buflen, &res);
        if (rc != 0) {
            if (rc == -4) {
                if (bufsize < maxbufsize) {
                    /* 缓冲区太小，加大缓冲区重试 */
                    if (bufsize * 2 > maxbufsize) {
                        bufsize = maxbufsize;
                    } else {
                        bufsize = bufsize * 2;
                    }
                    buffer = realloc(buffer, bufsize);
                    if (!buffer) {
                        log_error("realloc %d bytes failed", bufsize);
                        free(buffer);
                        return NULL;
                    } else {
                        /* 重新分配缓冲区成功，重试 */
                        log_info("increase buffer to %d bytes", bufsize);
                        goto again;
                    }
                } else {
                    log_error("skip %s: max bufsize (%d bytes) reach",
                              dirpath, bufsize);
                    goto out;
                }
            } else {
                log_error("skip %s: get_dir_list failed", dirpath);
            }
        } else {
            /* 当前目录处理完毕，累计文件数量和缓冲区使用量 */
            nr_scan = nr_scan + 1;
            nr_files = nr_files + res.nr_files;
            used_buflen = used_buflen + res.used_buflen;
        }
    }

out: {
        /* 获取多个目录下的文件列表完成，最后填充消息包长度和文件列表个数 */
        int64_t *msglen = (int64_t *)(&buffer[0]);
        *msglen = htobe64(used_buflen + 12);
        int32_t *listlen = (int32_t *)(&buffer[8]);
        *listlen = htobe32(nr_files);
        out->nr_files = nr_files;
        out->used_buflen = 8/*消息包总长度*/ + 4/*文件列表长度*/ + used_buflen;
        log_info("%d directory, %d scan, %d files, %d bytes",
                 nr_dir, nr_scan, nr_files, used_buflen+12);
        return buffer;
    }
}

void print_file_list(const char *buffer)
{
    int64_t msglen = *((int64_t *)&buffer[0]);
    msglen = be64toh(msglen);
    printf("print_file_list: message length %lld\n", (long long int)msglen);
    int listlen = *((int *)(&buffer[8]));
    listlen = be32toh(listlen);
    printf("print_file_list: found %d files\n", listlen);
    const char *p = &buffer[12]; /* 8个字节表示消息长度，4个字节表示文件列表个数 */
    char filename[4096];
    int i;
    for (i = 0; i < listlen; i++) {
        int16_t namelen = *((int16_t *)p);
        namelen = be16toh(namelen);
        p = p + 2;
        (void) memmove(filename, p, namelen);
        filename[namelen] = '\0';
        p = p + namelen;
        int64_t contentlen = *((int64_t *)p);
        contentlen = be64toh(contentlen);
        p = p + 8;
        printf("%s: %lld\n", filename, (long long int)contentlen);
    }
}

off_t get_file_size(const char *filepath)
{
    struct stat s;
    int rc = stat(filepath, &s);
    if (rc == 0) {
        return s.st_size;
    } else {
        printf("get %s status failed: %s\n", filepath, strerror(errno));
        return -1;
    }
}
