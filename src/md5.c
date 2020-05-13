#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <openssl/md5.h>

char * calculate_file_md5(const char *filename) {
    unsigned char c[MD5_DIGEST_LENGTH];
    int i;
    MD5_CTX mdContext;
    unsigned long bytes;
    unsigned char data[1024];
    char *filemd5 = (char*) malloc(33 * sizeof(char));

    FILE *inFile = fopen (filename, "rb");
    if (inFile == NULL) {
        perror(filename);
        return 0;
    }

    MD5_Init (&mdContext);

    while ((bytes = fread (data, 1, 1024, inFile)) != 0)

        MD5_Update (&mdContext, data, bytes);

    MD5_Final (c, &mdContext);

    for(i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(&filemd5[i * 2], "%02x", (unsigned int)c[i]);
    }

    fclose (inFile);
    return filemd5;
}

int check_md5(const char *filename, char * predefined_md5) {
    char *new_md5 = calculate_file_md5(filename);
    if (!strcmp(predefined_md5, new_md5)) {
        free(new_md5);
        return 0;
    } else {
        free(new_md5);
        return -1;
    }
}