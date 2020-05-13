//
// Created by wu on 2020/5/12.
//

#ifndef MEDICAL_SGW_MD5_H
#define MEDICAL_SGW_MD5_H

extern char * calculate_file_md5(const char *filename);
extern int check_md5(const char *filename, char * predefined_md5);

#endif //MEDICAL_SGW_MD5_H

