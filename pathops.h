#ifndef PATHOPS_H
#define PATHOPS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

extern unsigned int crc32(const char *s, unsigned int len);
extern void map1(const char *s, int size, int *x1, int *x2);
extern void map2(const char *buf, unsigned int len, int *x3, int *x4);
extern void maplevel(const char *buf, unsigned int len,
              int *x1, int *x2, int *x3, int *x4);
extern int calcpath(
    const char *mountpoint, const char *studyid, const char *serial,
    char *resultpath, int *resultlen);

extern off_t get_file_size(const char *filepath);
extern void split_serial(char *s, char **studyid, char **serial);
extern void cut_mount_path(char *abspath, const char *mountpoint);

struct file_list_result {
    int nr_files;               /* 目录树下的文件个数 */
    int used_buflen;            /* 缓冲区实际使用的长度 */
};

extern int get_dir_list(
    const char *dirpath, const char *mountpath,
    char *list, int listlen,
    struct file_list_result *res);

extern char *fill_many_dir_list(
    const char *mountpath, const char *dirs[], int nr_dir,
    struct file_list_result *out);

#endif /* PATHOPS_H */
