
#ifndef PUBLIC_H
#define PUBLIC_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <endian.h>
#include <libgen.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <pthread.h>

#include <emmintrin.h>
#define	compiler_barrier() do {                 \
        asm volatile ("" : : : "memory");       \
} while(0)

#define NODE_TYPE_NULL  0
#define NODE_TYPE_CLNT  1
#define NODE_TYPE_SGW   3
#define NODE_TYPE_ASM   6

#define NODE_TYPE_PIPE  200

#define MAX_IP_LEN 15

extern char local_ip[MAX_IP_LEN+1];
extern uint16_t local_port;
extern uint32_t local_id;

#define MAX_BACK_END    5

#define MAX_WORKERS 255

/// nodes id range
/// 0x00000000 ~~ 0x7FFFFFFF    client
/// 0x80000000 ~~ 0x8FFFFFFF    metadata server
/// 0x90000000 ~~ 0xAFFFFFFF    storage gateway


/// command
#define CMD_HEARTBEART_REQ      0x00010001
#define CMD_HEARTBEART_RSP      0x00010002

#define CMD_START_UPLOAD_REQ    0x00020001
#define CMD_START_UPLOAD_RSP    0x00020002

#define CMD_UPLOAD_DATA_REQ     0x00020003
#define CMD_UPLOAD_DATA_RSP     0x00020004

#define CMD_UPLOAD_FINISH_REQ   0x00020005
#define CMD_UPLOAD_FINISH_RSP   0x00020006

#define CMD_START_DOWNLOAD_REQ  0x00020007
#define CMD_START_DOWNLOAD_RSP  0x00020008

#define CMD_DOWNLOAD_DATA_REQ   0x00020009
#define CMD_DOWNLOAD_DATA_RSP   0x0002000A

#define CMD_DOWNLOAD_FINISH_REQ 0x0002000B
#define CMD_DOWNLOAD_FINISH_RSP 0x0002000C

#define CMD_DELETE_REQ  0x0002000D
#define CMD_DELETE_RSP  0x0002000E

// 获取指定检查号（studyid）和系列号（studyid/serial）下的所有文件列表
#define CMD_GET_FILE_LIST_REQ 0x0002000F
#define CMD_GET_FILE_LIST_RSP 0x00020010

// 顺序下载文件请求，主要为了提高性能（sendfile?）
#define CMD_SEQ_DOWNLOAD_REQ 0x00020011
#define CMD_SEQ_DOWNLOAD_RSP 0x00020012

#define CMD_MIGRATION_START_REQ 0x00030001
#define CMD_MIGRATION_START_RSP 0x00030002

#define CMD_MIGRATION_STOP_REQ 0x00030003
#define CMD_MIGRATION_STOP_RSP 0x00030004

#define CMD_MIGRATION_FINISHED_REQ 0x00030005
#define CMD_MIGRATION_FINISHED_RSP 0x00030006

#define CMD_MIGRATION_CANCEL_REQ 0x00030007
#define CMD_MIGRATION_CANCEL_RSP 0x00030008

extern const char * command_string(uint32_t c);

typedef struct msg_
{
    uint32_t length;    // 总长度
    uint8_t major;      // 协议主版本号
    uint8_t minor;      // 协议次版本号

    uint8_t src_type;   // 源节点类型
    uint8_t dst_type;   // 目的节点类型
    uint32_t src_id;    // 源节点 ID
    uint32_t dst_id;    // 目的节点 ID

    uint64_t trans_id;  // 任务 ID
    uint64_t sequence;  // 消息序号

    uint32_t command;   // 命令字
    uint32_t ack_code;  // 响应码

    uint64_t total;     // 数据总量
    uint64_t offset;    // 偏移量
    uint32_t count;     // 数据量

    uint8_t pad_1[4];   // 填充到 64B 对齐

    uint8_t data[0];
} msg_t;

extern void header_dump_packed(msg_t *h);
extern void header_dump_unpack(msg_t *h);

static inline void encode_msg(msg_t * msg)
{
   msg->length = htobe32(msg->length);

   msg->src_id = htobe32(msg->src_id);
   msg->dst_id = htobe32(msg->dst_id);

   msg->trans_id = htobe64(msg->trans_id);
   msg->sequence = htobe64(msg->sequence);

   msg->command = htobe32(msg->command);
   msg->ack_code = htobe32(msg->ack_code);

   msg->total = htobe64(msg->total);
   msg->offset = htobe64(msg->offset);

   msg->count = htobe32(msg->count);
}

static inline void decode_msg(msg_t * msg)
{
   msg->length = be32toh(msg->length);

   msg->src_id = be32toh(msg->src_id);
   msg->dst_id = be32toh(msg->dst_id);

   msg->trans_id = be64toh(msg->trans_id);
   msg->sequence = be64toh(msg->sequence);

   msg->command = be32toh(msg->command);
   msg->ack_code = be32toh(msg->ack_code);

   msg->total = be64toh(msg->total);
   msg->offset = be64toh(msg->offset);

   msg->count = be32toh(msg->count);
}


#define MAX_MSG_DATA_LEN    (4 * 1024 * 1024)
#define MAX_MESSAGE_LEN     ((MAX_MSG_DATA_LEN) + sizeof(msg_t))
#define MAX_RING_DATA_LEN   (20 * 1024 * 1024) // 20MB

#define MD5_LEN     32
#define MAX_NAME_LEN    255
#define MAX_PATH_LEN    4095

typedef struct task_info_
{
    uint16_t operation;
    uint16_t region_id;
    uint32_t site_id;
    uint32_t app_id;
    uint32_t timestamp;

    uint16_t sgw_port;      // 落地服务的 port
    uint16_t proxy_port;    // 中转服务的 port
    uint32_t sgw_ip;        // 落地服务的 IP
    uint32_t proxy_ip;      // 中转服务的 IP
    uint32_t sgw_id;        // 落地服务的 ID
    uint32_t proxy_id;      // 中转服务的 ID
    uint8_t pad_1[4];       // 填充到 64B 对齐

    uint64_t file_len;
    char file_md5[MD5_LEN+1];
    char file_name[MAX_NAME_LEN+1];
    uint32_t metadata_len;
    char metadata[0];

} task_info_t;

extern void taskinfo_dump_unpack(task_info_t *t);
extern void taskinfo_dump_packed(task_info_t *t);

extern void pr_msg_packed(msg_t *m);
extern void pr_msg_unpack(msg_t *m);

static inline void encode_task_info(task_info_t * task_info)
{
   task_info->operation = htobe16(task_info->operation);
   task_info->region_id = htobe16(task_info->region_id);
   task_info->site_id = htobe32(task_info->site_id);
   task_info->app_id = htobe32(task_info->app_id);
   task_info->timestamp = htobe32(task_info->timestamp);

   task_info->sgw_port = htobe16(task_info->sgw_port);
   task_info->proxy_port = htobe16(task_info->proxy_port);

   task_info->sgw_id = htobe32(task_info->sgw_id);
   task_info->proxy_id = htobe32(task_info->proxy_id);

   task_info->file_len = htobe64(task_info->file_len);

   task_info->metadata_len = htobe16(task_info->metadata_len);
}

static inline void decode_task_info(task_info_t * task_info)
{
   task_info->operation = be16toh(task_info->operation);
   task_info->region_id = be16toh(task_info->region_id);
   task_info->site_id = be32toh(task_info->site_id);
   task_info->app_id = be32toh(task_info->app_id);
   task_info->timestamp = be32toh(task_info->timestamp);

   task_info->sgw_port = be16toh(task_info->sgw_port);
   task_info->proxy_port = be16toh(task_info->proxy_port);

   task_info->sgw_id = be32toh(task_info->sgw_id);
   task_info->proxy_id = be32toh(task_info->proxy_id);

   task_info->file_len = be64toh(task_info->file_len);

   task_info->metadata_len = be16toh(task_info->metadata_len);
}

#endif // PUBLIC_H
