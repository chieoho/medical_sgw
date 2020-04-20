#ifndef MD5OPS_H
#define MD5OPS_H

/*
 * 找到文件绝对路径下同级的记录 md5 校验和的文件路径。例如：
 *
 * /a/b/c/1.txt -> /a/b/c/md5sum.txt
 */
extern int md5path(const char *abspath, char *out);

/*
 * 保存文件名和对应的检验和。例如：
 *
 * /a/b/c/1.txt 1234567890abcdefg1234567890abcdefg
 */
extern int savemd5(const char *filename, const char *md5);

/*
 * 在记录文件校验和的文件中，找到目标文件的 md5 校验和
 *
 * 例如：从 /a/b/c/md5sum.txt 中找出 /a/b/c/1.txt 的校验和
 */
extern int look_for_md5(const char *filepath, const char *target, char *md5);

#endif  /* MD5OPS_H */
