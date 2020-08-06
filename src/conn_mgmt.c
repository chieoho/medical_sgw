
// conn_mgmt.c
#include "config.h"
#ifdef TLS
#include <openssl/ssl.h>
#include <openssl/err.h>
extern SSL_CTX *ssl_ctx;
#endif
#include "mt_log.h"
#include "public.h"
#include "conn_mgmt.h"


extern int backend_cnt;

static int setrcvbuf(int s, int v)
{
    socklen_t len = sizeof(v);
    int rc = setsockopt(s, SOL_SOCKET, SO_RCVBUF, &v, len);
    if (rc == 0) {
        return 0;
    } else {
        int ec = errno;
        log_error("set sockfd %d SO_RCVBUF failed: %s", s, strerror(ec));
        return -1;
    }
}

static int setsndbuf(int s, int v)
{
    socklen_t len = sizeof(v);
    int rc = setsockopt(s, SOL_SOCKET, SO_SNDBUF, &v, len);
    if (rc == 0) {
        return 0;
    } else {
        int ec = errno;
        log_error("set sockfd %d SO_SNDBUF to value %d failed: %s", s, v, strerror(ec));
        return -1;
    }
}

void tcp_setnonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    flags = flags | O_NONBLOCK;
    int rc = fcntl(fd, F_SETFL, flags);
    if (rc == -1) {
        int ec = errno;
        log_error("set fd %d O_NONBLOCK failed: %s", fd, strerror(ec));
    } else {
        // 设置成功
    }
}

void tcp_setblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    flags = flags & ~O_NONBLOCK;
    int rc = fcntl(fd, F_SETFL, flags);
    if (rc == -1) {
        int ec = errno;
        log_error("clear fd %d O_NONBLOCK failed: %s", fd, strerror(ec));
    } else {
        // 设置成功
    }
}

int open_tcp_conn(events_poll_t * events_poll, char * peer_ip, uint16_t peer_port, char * local_ip, uint16_t local_port, int noblock)
{
    int flags = 1;
    int errno_cached = 0;
	int sock_fd = -1;
    struct sockaddr_in local_address;
    struct sockaddr_in peer_address;
    int address_len = sizeof(struct sockaddr_in);
    conn_info_t * conn_info = NULL;
    int reconnect_times = 0;
    int ret_val = 0;

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    errno_cached = errno;
    if (sock_fd < 3)
    {
        log_error("create socket fail, peer{%s:%u} : %s ", peer_ip, peer_port, strerror(errno_cached));
        return -1;
    }

	if (sock_fd >= MAX_CONNS_CNT)
	{
		log_error("sock_fd:%d >= MAX_CONNS_CNT:%d, peer{%s:%u} ", sock_fd, MAX_CONNS_CNT, peer_ip, peer_port);
		close(sock_fd);
        log_error("> closed sock_fd:%d", sock_fd);
        return -1;
	}

    flags = 1;
    setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
    setsockopt(sock_fd, SOL_TCP, TCP_NODELAY, &flags, sizeof(flags));

    if (noblock == 1)
    {
        fcntl(sock_fd, F_SETFL, fcntl(sock_fd, F_GETFL, 0)|O_NONBLOCK);
    }

    if (local_port > 0 && local_ip != NULL)
    {
        memset(&local_address, 0, sizeof(struct sockaddr_in));
        local_address.sin_family = AF_INET;
        if (strlen(local_ip) > 0)
        {
            local_address.sin_addr.s_addr = inet_addr(local_ip);
        }
        else
        {
            local_address.sin_addr.s_addr = 0;
        }
        local_address.sin_port = htons(local_port);

        if (bind(sock_fd, (struct sockaddr *)&local_address, address_len) < 0)
        {
            errno_cached = errno;
            log_error("bind sock_fd:%d to local{%s:%u} failed, %s", sock_fd, local_ip, local_port, strerror(errno_cached));
            close(sock_fd);
            log_error("> closed sock_fd:%d", sock_fd);
            return -1;
        }
    }

    memset(&peer_address, 0, sizeof(struct sockaddr_in));
    peer_address.sin_family = AF_INET;
    peer_address.sin_addr.s_addr = inet_addr(peer_ip);
    peer_address.sin_port = htons(peer_port);

    conn_info = &conns_info[sock_fd];

    reconnect_times = 0;
label_reconnect:
    ret_val = connect(sock_fd, (struct sockaddr *)&peer_address, address_len);
    errno_cached = errno;
    if (ret_val < 0)
    {
        if (errno_cached == EINTR)
        {
            if (reconnect_times < 5)
            {
                reconnect_times++;
                goto label_reconnect;
            }
            else
            {
                log_error("sock_fd:%d connect to peer{%s:%u} failed : reconnect_times:%d %s ",
                          sock_fd, peer_ip, peer_port, reconnect_times, strerror(errno_cached));
                close(sock_fd);
                log_error("> closed sock_fd:%d", sock_fd);
                return -1;
            }
        }
        else if (errno_cached != EINPROGRESS || noblock != 1)
        {
            log_error("sock_fd:%d connect to peer{%s:%u} failed : %s ", sock_fd, peer_ip, peer_port, strerror(errno_cached));
            close(sock_fd);
            log_error("> closed sock_fd:%d", sock_fd);
            return -1;
        }
    }

    memset(conn_info, 0, sizeof(conn_info_t));

    conn_info->recv = create_ring(MAX_RING_DATA_LEN);
    if (conn_info->recv == NULL)
    {
        log_error("create recv ring for sock_fd:%d failed, %s", sock_fd, strerror(errno_cached));
        close_tcp_conn(events_poll, sock_fd);
        return -1;
    }
    conn_info->send = create_ring(MAX_RING_DATA_LEN);
    if (conn_info->send == NULL)
    {
        log_error("create send ring for sock_fd:%d failed, %s", sock_fd, strerror(errno_cached));
        close_tcp_conn(events_poll, sock_fd);
        return -1;
    }

    if (add_to_events_poll(events_poll, sock_fd, EPOLLIN|EPOLLOUT) != 1)
	{
		log_error("add sock_fd:%d to events_poll fail, peer{%s:%u}", sock_fd, peer_ip, peer_port);
        close(sock_fd);
        log_error("> closed sock_fd:%d", sock_fd);
		return -1;
	}

    if (noblock == 1)
    {
        conn_info->status = CONN_STATUS_CONNECTING;
    }
    else
    {
        conn_info->status = CONN_STATUS_CONNECTED;
    }

    conn_info->sock_fd = sock_fd;
    strcpy(conn_info->peer_ip, peer_ip);
    conn_info->peer_port = peer_port;

    // log_info("> open sock_fd:%d, peer %s:%d", sock_fd, peer_ip, peer_port);
    return sock_fd;
}

extern uint64_t accepts;
extern uint64_t connections;
extern uint64_t concurrents[MAX_WORKERS+1];

extern int get_thread_id(void);

void close_tcp_conn(events_poll_t * events_poll, int sock_fd)
{
    conn_info_t * conn_info = NULL;
    conn_info_t * next_conn_info = NULL;
    int next_sock_fd = -1;
    int i = 0;
    int tid;

    if (sock_fd < 3 || sock_fd >= MAX_CONNS_CNT)
    {
        return;
    }

    conn_info = &conns_info[sock_fd];
    tid = get_thread_id();
    if (tid != conn_info->thread_id)
    {
        log_error("race condition! Thread %d trys to close sock_fd:%d, which belongs to thread %d",
                  tid, sock_fd, conn_info->thread_id);
        sleep(1); // 让日志打印
        assert(0);
    }
    if (sock_fd != conn_info->sock_fd)
    {
        log_error("something wrong! sock_fd:%d doesn't consistent with conn_info sock_fd:%d",
                  sock_fd, conn_info->sock_fd);
        sleep(1); // 让日志有时间打印
        assert(0);
    }
    conn_info->debug_fd = sock_fd;
    conn_info->close_thread_id = tid;

    if (events_poll != NULL && (conn_info->status == CONN_STATUS_CONNECTING || conn_info->status == CONN_STATUS_CONNECTED))
    {
        delete_from_events_poll(events_poll, sock_fd);
    }

    if (conn_info->recv != NULL)
    {
        destroy_ring(conn_info->recv);
    }

    if (conn_info->send != NULL)
    {
        destroy_ring(conn_info->send);
    }

    if (conn_info->thread_id > 0 && concurrents[conn_info->thread_id] > 0)
    {
        concurrents[conn_info->thread_id]--;
        connections = connections - 1;
    }

    if (conn_info->use_proxy == 1)
    {
        next_sock_fd = conn_info->next_sock_fd;
        if (next_sock_fd >= 3 && next_sock_fd < MAX_CONNS_CNT)
        {
            next_conn_info = &conns_info[next_sock_fd];

            next_conn_info->use_proxy = 0;
            next_conn_info->next_sock_fd = -1;

            close_tcp_conn(events_poll, next_sock_fd);
        }
    }
    else
    {
        for (i=0; i<backend_cnt; i++)
        {
            if (conn_info->befiles[i].fd >= 3)
            {
                close(conn_info->befiles[i].fd);
                // log_info("> close backend_fd %d in connection %d", conn_info->befiles [i].fd, conn_info->sock_fd);
            }
        }
    }

    // 设置清空标志，主要是将 sock_fd 设置为 -1，主要是为了在定时器中删除不合法
    // 的 sock_fd。注意，清空操作必须在 close() 之前，不然将会出现对同一个
    // conn_info 做读写的情况：工作线程关闭之后，主线程马上打开同一个 sock_fd，
    // 此时主线程会填充这个 conn_info，同时，工作线程也会对同一个 conn_info 做清
    // 空操作。
    clear_conn_info(conn_info);

    close(sock_fd);
    // log_info("> closed sock_fd %d: %lu accepts, %lu connections, %lu concurrent", sock_fd, accepts, connections, concurrents[conn_info->thread_id]);
}

int init_tcp_server(events_poll_t * events_poll, char * local_ip, uint16_t local_port)
{
	int flags = 1;
    int errno_cached = 0;
	int sock_fd = -1;
    struct sockaddr_in local_address;
    int address_len = sizeof(struct sockaddr_in);

    if (strlen(local_ip) == 0 || local_port == 0)
    {
        log_error("local_ip:%s local_port:%u", local_ip, local_port);
		return -1;
    }

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    errno_cached = errno;
    if (sock_fd < 0)
    {
        log_error("create socket fail, local{%s:%u} : %s ", local_ip, local_port, strerror(errno_cached));
        return -1;
    }

	if (sock_fd >= MAX_CONNS_CNT)
	{
		log_error("sock_fd:%d >= MAX_CONNS_CNT:%d", sock_fd, MAX_CONNS_CNT);
		close(sock_fd);
        log_error("> closed sock_fd:%d", sock_fd);
        return -1;
	}

    flags = 1;
    setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
    setsockopt(sock_fd, SOL_TCP, TCP_NODELAY, &flags, sizeof(flags));
    fcntl(sock_fd, F_SETFL, fcntl(sock_fd, F_GETFL, 0)|O_NONBLOCK);
    (void) setsndbuf(sock_fd, MAX_SO_SNDBUF);
    (void) setrcvbuf(sock_fd, MAX_SO_RCVBUF);

	memset(&local_address, 0, sizeof(struct sockaddr_in));
	local_address.sin_family = AF_INET;
    local_address.sin_addr.s_addr = inet_addr(local_ip);
	local_address.sin_port = htons(local_port);

	if (bind(sock_fd, (struct sockaddr *)&local_address, address_len) < 0)
	{
		errno_cached = errno;
		log_error("bind sock_fd:%d to local{%s:%u} failed, %s", sock_fd, local_ip, local_port, strerror(errno_cached));
		close(sock_fd);
        log_error("> closed sock_fd:%d", sock_fd);
		return -1;
	}

    if (listen(sock_fd, 512) < 0)
    {
        errno_cached = errno;
        log_error("listen on sock_fd:%d failed, local{%s:%u} %s", sock_fd, local_ip, local_port, strerror(errno_cached));
        close(sock_fd);
        log_error("> closed sock_fd:%d", sock_fd);
        return -1;
    }

    if (add_to_events_poll(events_poll, sock_fd, EPOLLIN) != 1)
	{
		log_error("add sock_fd:%d to events_poll fail, local{%s:%u}", sock_fd, local_ip, local_port);
        close(sock_fd);
        log_error("> closed sock_fd:%d", sock_fd);
		return -1;
	}

    log_info("init local tcp server ok ");

	return sock_fd;
}

int send_message(events_poll_t *events_poll, conn_info_t *conn_info,
                 uint8_t *data, int len)
{
    int res = write_ring(conn_info->send, data, len);
    if (res == len) {
        start_monitoring_send(events_poll, conn_info->sock_fd);
        return len;
    } else {
        log_error("write %d bytes to sock_fd:%d send buffer failed",
                  len, conn_info->sock_fd);
        return -1;
    }
}

int send_message_internal(events_poll_t * events_poll, conn_info_t * conn_info)
{
    ring_t * send_ring = conn_info->send;
    int want_len = 0;
    uint8_t * data = NULL;
    int len = 0;
    int send_len = 0;
    int send_times = 0;
    int errno_cached = 0;

    want_len = get_ring_data_size(send_ring);
    if (want_len <= 0)
    {
        stop_monitoring_send(events_poll, conn_info->sock_fd);
        return 0;
    }

    len = send_ring->size - send_ring->read;
    if (len > want_len)
    {
        len = want_len;
    }
    data = &(send_ring->data[send_ring->read]);

label_send:
#ifdef TLS
    if(conn_info->peer_type == NODE_TYPE_ASM)
        send_len = send(conn_info->sock_fd, data, len, 0);
    else
        send_len = SSL_write(conn_info->ssl, data, len);
#else
    send_len = send(conn_info->sock_fd, data, len, 0);
#endif
    errno_cached = errno;

    //    log_debug("sock_fd:%d len:%d send_len:%d ", conn_info->sock_fd, len, send_len);

    if (send_len > 0)
    {
        send_ring->read = (send_ring->read + send_len) % send_ring->size;
        send_ring->len = send_ring->len - send_len;
        return send_len;
    }
    else if (send_len == 0 && send_times < 5)
    {
        send_times++;
        log_warning("sock_fd:%d send_len:0 send_times:%d ", conn_info->sock_fd, send_times);
        goto label_send;
    }
    else
    {
        if (errno_cached == EAGAIN)
        {
            return 0;
        }
        else if (errno_cached == EINTR && send_times < 5)
        {
            send_times++;
            log_warning("sock_fd:%d errno_cached == EINTR, send_times:%d ", conn_info->sock_fd, send_times);
            goto label_send;
        }
        else
        {
            log_error("sock_fd:%d send failed: %s", conn_info->sock_fd, strerror(errno_cached));
            return -1;
        }
    }
}

// 返回值的说明：
//     >0 : 实际接收的字节数
//      0 : 对端关闭连接
//     -1 : 出错
//     -2 : 非法参数
//     -3 : 超过重试次数，仍然没有数据可读
//     -4 : 数据尚未可读
int recv_message_internal(conn_info_t * conn_info, uint8_t * buffer, int want_len)
{
    int sock_fd = -1;
#ifdef TLS
    SSL *ssl;
#endif
    int recv_len = 0;
    int recv_times = 0;
    int errno_cached = 0;

    if (want_len <= 0) {
        // log_info("invalid want_len %d, do nothing", want_len);
        return -2;
    }

    sock_fd = conn_info->sock_fd;

label_recv:
#ifdef TLS
    ssl = conn_info->ssl;
    recv_len = SSL_read(ssl, buffer, want_len);
#else
    recv_len = recv(sock_fd, buffer, want_len, 0);
#endif
    errno_cached = errno;
    if (recv_len > 0) {
        return recv_len;
    } else if (recv_len == 0) {
        // log_info("peer{%s:%u} close the connection %d", conn_info->peer_ip, conn_info->peer_port, sock_fd);
        return 0;
    } else {
        if (errno_cached == EINTR) {
            if (recv_times < 5) {
                // log_info("sock_fd:%d interrupt, try again", sock_fd);
                recv_times++;
                goto label_recv;
            } else {
                // log_info("sock_fd:%d interrupt 5 times, give up", sock_fd);
                return -3;
            }
        } else if (errno_cached == EAGAIN) {
            // log_info("sock_fd:%d is nonblock and no message arrived", sock_fd);
            return -4;
        } else {
            log_error("recv error : sock_fd:%d %s ", sock_fd, strerror(errno_cached));
            return -1;
        }
    }
}

extern int deal_message(events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg);
extern int get_thread_id(void);
extern int dispatch_work(int sock_fd);
extern int workers;

int on_new_conn_arrived(int server_fd)
{
    struct sockaddr_in peer_address;
    socklen_t address_len = sizeof(struct sockaddr_in);
    int sock_fd = -1;
    int retry_times = 0;
    int errno_cached = 0;
    conn_info_t * conn_info = NULL;
    int flags = 1;
    
    while (1)
    {
        sock_fd = accept(server_fd, (struct sockaddr *)&peer_address, &address_len);
#ifdef TLS
        SSL *ssl;
        ssl = SSL_new(ssl_ctx);              /* get new SSL state with context */
        SSL_set_fd(ssl, sock_fd);
        if (SSL_accept(ssl) == -1){   /* do SSL-protocol accept */
            log_error("ssl accept failed");
            ERR_print_errors_fp(stderr);
        }
#endif
        errno_cached = errno;
        if (sock_fd < 0)
        {
            if (errno_cached == EINTR)
            {
                if (retry_times < 5)
                {
                    retry_times++;
                    continue;
                }
                else
                {
                    return 0;
                }
            }
            else if (errno_cached == EAGAIN)
            {
                return 0;
            }
            else
            {
                log_error("accept client connecting fail : %s ", strerror(errno_cached));
                return -1;
            }
        }
        else if (sock_fd >= MAX_CONNS_CNT)
        {
            log_error("sock_fd:%d >= MAX_CONNS_CNT:%d", sock_fd, MAX_CONNS_CNT);
            close(sock_fd);
            log_error("> closed sock_fd:%d", sock_fd);
            return -1;  
        }
        else
        {
            flags = 1;
            setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags));
            // 设置马上发送似乎在传输数据量多的时候会降低性能，暂时取消设置
            // setsockopt(sock_fd, SOL_TCP, TCP_NODELAY, &flags, sizeof(flags));
            fcntl(sock_fd, F_SETFL, fcntl(sock_fd, F_GETFL, 0)|O_NONBLOCK);
            (void) setsndbuf(sock_fd, MAX_SO_SNDBUF);
            (void) setrcvbuf(sock_fd, MAX_SO_RCVBUF);
            
            conn_info = &conns_info[sock_fd];
            clear_conn_info(conn_info);

            strcpy(conn_info->peer_ip, inet_ntoa(peer_address.sin_addr));
            conn_info->peer_port = ntohs(peer_address.sin_port);
            conn_info->status = CONN_STATUS_CONNECTED;
            conn_info->sock_fd = sock_fd;
#ifdef TLS
            conn_info->ssl = ssl;
#endif
            conn_info->recv = create_ring(MAX_RING_DATA_LEN);
            if (conn_info->recv == NULL)
            {
                log_error("create recv ring for sock_fd:%d failed, %s", sock_fd, strerror(errno_cached));
                close_tcp_conn(NULL, sock_fd);
                return -1;
            }
            conn_info->send = create_ring(MAX_RING_DATA_LEN);
            if (conn_info->send == NULL)
            {
                log_error("create send ring for sock_fd:%d failed, %s", sock_fd, strerror(errno_cached));
                close_tcp_conn(NULL, sock_fd);
                return -1;
            }

            accepts = accepts + 1;
            connections = connections + 1;

            // log_info("> accept peer %s:%u on sock_fd %d: %lu accepts, %lu connections", conn_info->peer_ip, conn_info->peer_port, sock_fd, accepts, connections);

            // 将接收到的客户端分发到当前工作者线程。工作者线程的标识从
            // 1~workers，主线程的标识是 0
            int wid = dispatch_work(sock_fd);
            if (1 <= wid && wid <= workers)
            {
                // log_info("> dispatch sock_fd %d to worker:%d success", sock_fd, wid);
                return sock_fd;
            }
            else
            {
                log_error("> dispatch sock_fd %d to worker:%d failed",
                          sock_fd, wid);
                close_tcp_conn(NULL, sock_fd);
                return -1;
            }
        }
    }
}

static int handle_incoming_message(events_poll_t * e, conn_info_t * c)
{
    ring_t * ring = c->recv;
    uint32_t recvtotal = ring->write;
    uint32_t offset = 0;
    while (recvtotal >= sizeof(msg_t)) {
        msg_t * msg = (msg_t *)(&ring->data[offset]);
        uint32_t msglen = ntohl(msg->length);
        if (msglen <= MAX_MESSAGE_LEN) {
            if (recvtotal >= msglen) {
                decode_msg(msg);
                int command = msg->command;
                int64_t seq = msg->sequence;
                int ret = deal_message(e, c, msg);
                if (ret < 0) {
                    log_error("%s:%lu: handle_incoming_message failed",
                              command_string(command), seq);
                    return -1;
                } else {
                    recvtotal = recvtotal - msglen;
                    offset = offset + msglen;
                }
            } else {
                // 缓冲区剩下的数据不是一个完整的消息，
                // 处理结束，等待下一次接收
                break;
            }
        } else {
            log_error("sock_fd:%d recv too large message: length %u > MAX_MESSAGE_LEN %lu",
                      c->sock_fd, msglen, MAX_MESSAGE_LEN);
            return -1;
        }
    }

    if (recvtotal > 0) {
        if (recvtotal != ring->write) {
            memmove(ring->data, &ring->data[offset], recvtotal);
            ring->len = recvtotal;
            ring->write = recvtotal;
            ring->read = 0;
        } else {
            // 什么都没处理
        }
    } else {
        ring->len = 0;
        ring->write = 0;
    }
    return 0;
}

// 这个函数不关闭套接字
int on_can_recv(events_poll_t * events_poll, conn_info_t * conn_info)
{
    ring_t * ring = conn_info->recv;
    if (!ring) {
        log_error("conn_info receive buffer is NULL: sock_fd:%d, close_thread_id:%d",
                  conn_info->debug_fd, conn_info->close_thread_id);
        sleep(1); // 让日志打印
        assert(0);
    }
    // 每次处理完毕都会移动尚未处理的数据到 recv->data[0] 处，所以这样计
    // 算缓冲区可接收的长度是正确的
    int recvleft = ring->size - ring->write;
    int recvlen = recv_message_internal(
        conn_info, &ring->data[ring->write], recvleft);
    if (recvlen > 0) {
        ring->write = ring->write + recvlen;
        ring->len = ring->len + recvlen;
        int ret = handle_incoming_message(events_poll, conn_info);
        if (ret == 0) {
            return 0; // 处理消息没有发生错误
        } else {
            log_error("handle_incoming_message failed");
            return -1;
        }
    } else if (recvlen == 0) {
        close_tcp_conn(events_poll, conn_info->sock_fd);
        return 0; // 对端关闭是正常现象，不作为错误处理
    } else {
        if (recvlen == -3 || recvlen == -4) {
            return 0;
        } else {
            log_error("recv_message_internal failed: return %d", recvlen);
            return -1;
        }
    }
}
