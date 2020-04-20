
// conn_mgmt.h

#ifndef CONN_MGMT_H
#define CONN_MGMT_H

#include "ring.h"
#include "events_poll.h"

#ifndef MAX_TCP_BUF
#define MAX_TCP_BUF (8192)
#endif

#ifndef MAX_SO_SNDBUF
#define MAX_SO_SNDBUF (64*1024) // 64KB
#endif

#ifndef MAX_SO_RCVBUF
#define MAX_SO_RCVBUF (128*1024) // 128KB
#endif

#define CONN_STATUS_IDLE		0
#define CONN_STATUS_CONNECTING	1
#define CONN_STATUS_CONNECTED  	2
#define CONN_STATUS_CLOSING  	3

struct backend_file
{
    int fd; // 文件描述符
    int sndstate; // 发送状态
    // filesize, fileleft, filedone 主要用于 sendfile() 的文件顺序下载
    int64_t filesize; // 文件大小
    int64_t fileleft; // 文件需要传输的大小
    int64_t filedone; // 文件已经传输的大小
    char md5[MD5_LEN + 1]; // 经过 hash 处理后生成的散列值作为文件名
    char abs_file_name[MAX_PATH_LEN + MAX_NAME_LEN + 1];
};

typedef struct conn_info_
{
    uint32_t flags;
    
    int use_proxy;
    int next_sock_fd;
    
    int sock_fd;
    int status;
    int thread_id;
    int peer_type;
    char peer_ip[MAX_IP_LEN+1];
    uint16_t peer_port;
    uint32_t peer_id;

    struct backend_file befiles[MAX_BACK_END];
    
    uint64_t trans_id;
    uint64_t sequence;
    
    ring_t * recv;
    ring_t * send;
    
    void * priv;

    int debug_fd;
    int close_thread_id;
    int is_sequence; // 是否使用文件的顺序传输
    
} conn_info_t;

static inline void clear_conn_info(conn_info_t * conn_info)
{
    int i = 0;
    int close_thread_id;

    close_thread_id = conn_info->close_thread_id;
    memset(conn_info, 0, sizeof(conn_info_t));
    conn_info->close_thread_id = close_thread_id;
    conn_info->is_sequence = 0;
    
    conn_info->next_sock_fd = -1;
    conn_info->sock_fd = -1;
    for (i = 0; i < MAX_BACK_END; i++) {
        conn_info->befiles[i].fd = -1;
        conn_info->befiles[i].sndstate = -1;
        conn_info->befiles[i].filesize = 0;
        conn_info->befiles[i].fileleft = 0;
        conn_info->befiles[i].filedone = 0;
    }
}


extern conn_info_t conns_info[MAX_CONNS_CNT];

extern void tcp_setblocking(int fd);
extern void tcp_setnonblock(int fd);

int open_tcp_conn(events_poll_t * events_poll, char * peer_ip, uint16_t peer_port,
                  char * local_ip, uint16_t local_port, int noblock);
                  
void close_tcp_conn(events_poll_t * events_poll, int sock_fd);

int init_tcp_server(events_poll_t * events_poll, char * local_ip, uint16_t local_port);

int on_new_conn_arrived(int server_fd);

int on_can_recv(events_poll_t * events_poll, conn_info_t * conn_info);

int send_message(events_poll_t * events_poll, conn_info_t * conn_info, uint8_t * data, int len);

int send_message_internal(events_poll_t * events_poll, conn_info_t * conn_info);

#endif // CONN_MGMT_H
