
// events_poll.c

#include <assert.h>

#include "config.h"
#include "mt_log.h"
#include "public.h"
#include "conn_mgmt.h"
#include "events_poll.h"
#include "md5ops.h"

#ifndef EPOLLRDHUP
#define EPOLLRDHUP 0x2000
#endif

extern char *default_md5sum_filename;

static char * get_events_string(uint32_t events)
{
    static char events_string[256];
    int index = 0;

    memset(events_string, 0, sizeof(events_string));

    if (events & EPOLLERR)
    {
        strcpy(&events_string[index], "EPOLLERR");
        index += strlen("EPOLLERR");
    }

    if (events & EPOLLHUP)
    {
        if (index > 0)
        {
            events_string[index++] = '|';
        }

        strcpy(&events_string[index], "EPOLLHUP");
        index += strlen("EPOLLHUP");
    }

    if (events & EPOLLRDHUP)
    {
        if (index > 0)
        {
            events_string[index++] = '|';
        }

        strcpy(&events_string[index], "EPOLLRDHUP");
        index += strlen("EPOLLRDHUP");
    }

    if (events & EPOLLIN)
    {
        if (index > 0)
        {
            events_string[index++] = '|';
        }

        strcpy(&events_string[index], "EPOLLIN");
        index += strlen("EPOLLIN");
    }

    if (events & EPOLLOUT)
    {
        if (index > 0)
        {
            events_string[index++] = '|';
        }

        strcpy(&events_string[index], "EPOLLOUT");
        index += strlen("EPOLLOUT");
    }

    return events_string;
}


int setup_events_poll(events_poll_t * events_poll)
{
	int i = 0;

	memset(events_poll, 0, sizeof(events_poll_t));
	for (i=0; i<MAX_CONNS_CNT; i++)
    {
        events_poll->fds_info_array[i].fd = -1;
    }

    events_poll->epoll_fd = epoll_create(MAX_CONNS_CNT);
    if (events_poll->epoll_fd < 0)
    {
        log_error("epoll_create fail : %s ", strerror(errno));
        return -1;
    }

    return events_poll->epoll_fd;
}


int add_to_events_poll(events_poll_t * events_poll, int sock_fd, uint32_t events)
{
    struct epoll_event event_obj;
    fd_info_t * p_fd_info = NULL;

	p_fd_info = &(events_poll->fds_info_array[sock_fd]);
	p_fd_info->fd = sock_fd;
	p_fd_info->events = events;

    event_obj.events = events;
    event_obj.data.fd = sock_fd;
    if (epoll_ctl(events_poll->epoll_fd, EPOLL_CTL_ADD, sock_fd, &event_obj) < 0)
    {
        if (errno == EEXIST)
        {
            // log_info("sock_fd:%d events:%s existing in event loop ", sock_fd, get_events_string(events));
			epoll_ctl(events_poll->epoll_fd, EPOLL_CTL_MOD, sock_fd, &event_obj);
        }
        else
        {
            log_error("add sock_fd:%d to event loop fail : %s ", sock_fd, strerror(errno));
            return -1;
        }
    }

    return 1;
}


int delete_from_events_poll(events_poll_t * events_poll, int sock_fd)
{
    struct epoll_event event_obj;
	fd_info_t * p_fd_info = NULL;

	p_fd_info = &(events_poll->fds_info_array[sock_fd]);
	p_fd_info->fd = -1;
	p_fd_info->events = 0;

    event_obj.events = 0;
    event_obj.data.fd = sock_fd;
    epoll_ctl(events_poll->epoll_fd, EPOLL_CTL_DEL, sock_fd, &event_obj);

    return 1;
}


int start_monitoring_events(events_poll_t * events_poll, int sock_fd, uint32_t events)
{
    struct epoll_event event_obj;
    fd_info_t * p_fd_info = NULL;
	int errno_cache = 0;

	//    log_info("events_poll:%p sock_fd:%d events:%s ", events_poll, sock_fd, get_events_string(events));

	p_fd_info = &(events_poll->fds_info_array[sock_fd]);
	p_fd_info->fd = sock_fd;
	p_fd_info->events |= events;

	event_obj.events = p_fd_info->events;
	event_obj.data.fd = p_fd_info->fd;
    if (epoll_ctl(events_poll->epoll_fd, EPOLL_CTL_MOD, sock_fd, &event_obj) < 0)
    {
        errno_cache = errno;
		if (errno_cache == ENOENT)
        {
            if (epoll_ctl(events_poll->epoll_fd, EPOLL_CTL_ADD, sock_fd, &event_obj) < 0)
            {
                errno_cache = errno;
                log_error("add sock_fd:%d to event loop fail : %s ", sock_fd, strerror(errno_cache));
                return -1;
            }
        }
        else
        {
            log_error("modify monitoring events of sock_fd:%d fail : %s ", sock_fd, strerror(errno_cache));
            return -1;
        }
    }

    return 1;
}

int start_monitoring_send(events_poll_t * events_poll, int sock_fd)
{
	return start_monitoring_events(events_poll, sock_fd, EPOLLOUT);
}

int start_monitoring_recv(events_poll_t *e, int s)
{
    return start_monitoring_events(e, s, EPOLLIN);
}

int stop_monitoring_events(events_poll_t * events_poll, int sock_fd, uint32_t events)
{
    struct epoll_event event_obj;
    fd_info_t * p_fd_info = NULL;
	int errno_cache = 0;

	//    log_info("events_poll:%p sock_fd:%d events:%s ", events_poll, sock_fd, get_events_string(events));

	p_fd_info = &(events_poll->fds_info_array[sock_fd]);
	p_fd_info->fd = sock_fd;
	p_fd_info->events &= ~events;

	//    log_info("events_poll:%p sock_fd:%d p_fd_info->events:%s ", events_poll, sock_fd, get_events_string(p_fd_info->events));

	event_obj.events = p_fd_info->events;
	event_obj.data.fd = p_fd_info->fd;
    if (epoll_ctl(events_poll->epoll_fd, EPOLL_CTL_MOD, sock_fd, &event_obj) < 0)
    {
        errno_cache = errno;
		log_error("modify monitoring events of sock_fd:%d fail : %s ", sock_fd, strerror(errno_cache));
		return -1;
    }

    return 1;
}

int stop_monitoring_recv(events_poll_t * events_poll, int sock_fd)
{
	return stop_monitoring_events(events_poll, sock_fd, EPOLLIN);
}

int stop_monitoring_send(events_poll_t * events_poll, int sock_fd)
{
	return stop_monitoring_events(events_poll, sock_fd, EPOLLOUT);
}



extern char local_ip[MAX_IP_LEN+1];
extern uint16_t local_port;
extern int listen_fd;

extern int on_can_recv(events_poll_t * events_poll, conn_info_t * conn_info);

static int __recreate_server_fd(
    events_poll_t * events_poll,
    int old_server_fd)
{
	delete_from_events_poll(events_poll, old_server_fd);
    // log_info("> delete old_server_fd:%d from events poll", old_server_fd);
	close(old_server_fd);
    // log_info("> closed old_server_fd:%d", old_server_fd);

	sleep(1);

	int new_server_fd = init_tcp_server(events_poll, local_ip, local_port);
	if (new_server_fd < 0)
	{
        return -1;
	}
	else
	{
		return new_server_fd;
	}
}

static int recreate_server_fd(
    events_poll_t * events_poll,
    int old_server_fd)
{
    int new_server_fd = __recreate_server_fd(events_poll, old_server_fd);
    if (new_server_fd < 0)
    {
        log_crit("recreate server listen fd failed, terminated");
        return -1;
    }
    else
    {
        listen_fd = new_server_fd;
        return 0;
    }
}

static int create_client_fd(int server_fd)
{
    int client_fd = on_new_conn_arrived(server_fd);
    if (client_fd >= 3)
    {
        return client_fd;
    }
    else
    {
        log_error("create client fd failed: invalid fd %d", client_fd);
        return -1;
    }
}

static void deal_server_socket_events(
    events_poll_t * events_poll,
    int server_fd,
    uint32_t events)
{
    if (events & EPOLLERR)
    {
        int ret = recreate_server_fd(events_poll, server_fd);
        if (ret < 0)
        {
            log_crit("handle EPOLLERR failed, terminated");
            exit(EXIT_FAILURE);
        }
    }
    else if (events & EPOLLIN)
    {
        int client_fd = create_client_fd(server_fd);
        if (client_fd < 0)
        {
            log_error("handle EPOLLIN on server fd %d failed", server_fd);
        }
    }
    else
    {
        log_error("invalid events: %s", get_events_string(events));
    }
}


static void on_data_socket_error(
    events_poll_t * events_poll,
    conn_info_t * conn_info)
{
    int sock_fd = conn_info->sock_fd;
    if (sock_fd >= 0)
    {
        if (conn_info->peer_type != NODE_TYPE_PIPE)
        {
            log_error("error on sock_fd:%d, peer %s:%d",
                      sock_fd, conn_info->peer_ip, conn_info->peer_port);
            close_tcp_conn(events_poll, sock_fd);
        }
        else
        {
            // XXX: 管道损坏，应当重建管道
            log_error("pipe_fd:%d was broken!", sock_fd);
        }
    }
    else
    {
        log_error("error on invalid sock_fd:%d", sock_fd);
    }
}

static int send_file_blob(int sd, struct backend_file *f)
{
    off_t offset;
    while (f->fileleft > 0) {
        size_t blocksize;
        if (f->fileleft < MAX_TCP_BUF) {
            blocksize = f->fileleft;
        } else {
            blocksize = MAX_TCP_BUF;
        }
        offset = f->filedone;
        ssize_t sendlen = sendfile(sd, f->fd, &offset, blocksize);
        if (sendlen >= 0) {
            f->fileleft = f->fileleft - sendlen;
            f->filedone = f->filedone + sendlen;
        } else {
            int ec = errno;
            if (ec == EAGAIN) {
                return 0;
            } else {
                log_error("sendfile failed: %s: out(%d) <- in(%d)",
                          strerror(ec), sd, f->fd);
                return -1;
            }
        }
    }
    return 1;
}

extern int get_thread_id(void);

static int deal_data_socket_epollout(
    events_poll_t * e,
    conn_info_t * c)
{
    int sock_fd = c->sock_fd;
    if (sock_fd >= 0) {
        if (c->peer_type == NODE_TYPE_PIPE) {
            int ret = stop_monitoring_send(e, sock_fd);
            if (ret >= 0) {
                return 0;
            } else {
                log_error("stop_monitoring_send failed");
                return -1;
            }
        } else {
            if (c->is_sequence == 0) {
                int write_len = send_message_internal(e, c);
                if (write_len >= 0) {
                    return write_len;
                } else {
                    log_error("send_message_internal failed");
                    close_tcp_conn(e, sock_fd);
                    return -1;
                }
            } else {
                assert(c->is_sequence == 1);
                struct backend_file *f;
                int i;
                f = NULL;
                for (i = 0; i < MAX_BACK_END; i++) {
                    f = &c->befiles[i];
                    if (f->fd >= 0) {
                        break;
                    } else {
                        // 继续查找下一个文件
                    }
                }
                if (f && f->fd >= 0) {
                    if (f->sndstate == 0) {
                        char md5_path[2048];
                        int rc1 = md5path(f->abs_file_name, md5_path);
                        if (rc1 == 0) {
                            // 获取文件上传时的 md5
                            char md5[32];
#if HAVE_CHECK_MD5
                            int rc2 = look_for_md5(
                                md5_path, f->abs_file_name, md5);
#else
                            int rc2 = 0;
#endif
                            if (rc2 == 0) {
                                // 8字节的消息长度，32字节的md5长度
                                int64_t msglen = htobe64(40 + f->filesize);
                                char buffer[40];
                                memmove(buffer, &msglen, 8);
                                memmove(buffer+8, md5, 32);
                                int sendlen = send(sock_fd, buffer, 40, MSG_MORE);
                                if (sendlen == 40) {
                                    // 发送消息前缀和md5校验和成功，继续发送文件内容
                                    f->sndstate = 1;
                                    goto send_blob;
                                } else {
                                    log_error("send msglen and md5 failed");
                                    close_tcp_conn(e, sock_fd);
                                }
                            } else {
                                log_error("%s: look_for_md5 failed", f->abs_file_name);
                                close_tcp_conn(e, sock_fd);
                            }
                        } else {
                            log_error("find file %s md5 path failed", f->abs_file_name);
                            close_tcp_conn(e, sock_fd);
                            return -1;
                        }
                    } else if (f->sndstate == 1) {
                        // 已经发送了消息前缀和校验和md5
                        int rc3;
                    send_blob:
                        // 可以发送文件内容
                        rc3 = send_file_blob(sock_fd, f);
                        if (rc3 == 0) {
                            // 连接暂时不可写，等待下次继续发送
                            return 0;
                        } else if (rc3 == 1) {
                            // 文件内容已经发送完毕，可以关闭文件，开
                            // 启监听客户端的可读事件
                            log_debug("%s successfully downloaded (%lld bytes)",
                                      f->abs_file_name,
                                      (long long int)f->filedone);
                            close(f->fd);
                            f->fd = -1;
                            f->sndstate = 2;
                            start_monitoring_recv(e, sock_fd);
                             stop_monitoring_send(e, sock_fd);
                            return 0;
                        } else {
                            // 发送文件内容失败
                            log_error("send_file_block failed: sock_fd:%d", sock_fd);
                            close_tcp_conn(e, sock_fd);
                            return -1;
                        }
                    } else {
                        // log_info("%s already send completed", f->abs_file_name);
                         stop_monitoring_send(e, sock_fd);
                        start_monitoring_recv(e, sock_fd);
                        return 0;
                    }
                } else {
                    log_error("no backend file in sock_fd:%d", sock_fd);
                    close_tcp_conn(e, sock_fd);
                    return -1;
                }
                return 0;
            }
        }
    } else {
        log_error("invalid sock_fd:%d", sock_fd);
        return 0;
    }
}

extern uint64_t concurrents[MAX_WORKERS+1];

static int receive_client_fd(
    events_poll_t * e,
    int pipe_fd)
{
    int client_fd;
    int size = sizeof(client_fd);
    int ret = read(pipe_fd, &client_fd, size);
    int errno_cached = errno;
    if (ret == size && 3 <= client_fd && client_fd <= MAX_CONNS_CNT)
    {
        // log_info("receive client_fd:%d success", client_fd);

        conn_info_t * c = &conns_info[client_fd];
        assert(c->recv != NULL);
        assert(c->send != NULL);
        assert(c->sock_fd == client_fd);
        c->thread_id = get_thread_id();
        concurrents[c->thread_id] += 1;

        // log_info("worker:%d is serving %lu concurrents now", c->thread_id, concurrents[c->thread_id]);

        int ret = add_to_events_poll(e, client_fd, EPOLLIN);
        if (ret == 1)
        {
            // log_info("add client_fd:%d to EPOLLIN events poll success", client_fd);
            return 0;
        }
        else
        {
            log_error("add client_fd:%d to events poll failed", client_fd);
            close_tcp_conn(NULL, client_fd);
            return -1;
        }
    }
    else
    {
        if (ret == -1)
        {
            log_error("read %d bytes from pipe_fd:%d failed: %s",
                      size, pipe_fd, strerror(errno_cached));
            return -1;
        }
        else if (ret != size)
        {
            // 只有很小的概率不能从管道读取到完整的 4 个字节的数据。如果这种情况
            // 频繁发生，那么需要考虑分多次读取。目前工作者线程只从管道读取分发
            // 者分发的客户端连接。
            log_error("read pipd_fd:%d uncompleted: %d want, %d read",
                      pipe_fd, size, ret);
            return -1;
        }
        else
        {
            assert(ret == size);
            log_error("recv invalid client_fd:%d", client_fd);
            return -1;
        }
    }
}

static int deal_data_socket_epollin(
    events_poll_t * e,
    conn_info_t * c)
{
    int sock_fd = c->sock_fd;
    if (sock_fd >= 0)
    {
        if (c->peer_type == NODE_TYPE_PIPE)
        {
            // 工作者线程从主线程收到已连接的客户端套接字，设置好相关的套
            // 接字上下文
            int ret = receive_client_fd(e, sock_fd);
            if (ret < 0)
            {
                log_error("receive client_fd on pipe_fd:%d failed", sock_fd);
                return -1;
            }
            else
            {
                return 0;
            }
        }
        else
        {
            // 工作者线程从客户端接收数据，然后进行处理
            int ret = on_can_recv(e, c);
            if (ret < 0)
            {
                log_error("on_can_recv failed");
                close_tcp_conn(e, c->sock_fd);
            }
            return ret;
        }
    }
    else
    {
        log_error("invalid sock_fd:%d", sock_fd);
        return 0;
    }
}

static void deal_data_socket_events(
    events_poll_t *events_poll,
    int sock_fd, uint32_t events)
{
    int current_thread_id = get_thread_id();
    conn_info_t * conn_info = &conns_info[sock_fd];

    if (current_thread_id == conn_info->thread_id &&
                  sock_fd == conn_info->sock_fd)
    {
        if ((events & EPOLLIN) || (events & EPOLLOUT)) {
            if (events & EPOLLOUT) {
                int wlen = deal_data_socket_epollout(
                    events_poll, conn_info);
                if (wlen >= 0) {
                    if (events & EPOLLIN) {
                        // 除了 EPOLLOUT 事件，还有 EPOLLIN 事件需要处理
                        goto handle_epollin;
                    } else {
                        // 没有 EPOLLIN 事件处理，本次处理完毕
                    }
                } else {
                    log_error("deal_data_socket_epollout failed on sock_fd:%d", sock_fd);
                }
            } else {
                // EPOLLIN
                int rlen;
            handle_epollin:
                rlen = deal_data_socket_epollin(events_poll, conn_info);
                if (rlen >= 0) {
                    // 正常处理完成，无需进一步处理
                } else {
                    log_error("deal_data_socket_epollin failed on sock_fd:%d", sock_fd);
                }
            }
        } else {
            log_warning("unhandle events %s occurs", get_events_string(events));
            // 保守处理，关闭连接
            on_data_socket_error(events_poll, conn_info);
        }
    } else { // current_thread_id != conn_info->thread_id || sock_fd != conn_info->sock_fd
        if (current_thread_id != conn_info->thread_id) {
            log_warning("sock_fd:%d belong to worker:%d, not worker:%d",
                        sock_fd, conn_info->thread_id, current_thread_id);
            delete_from_events_poll(events_poll, sock_fd);
            // log_info("> remove sock_fd:%d from worker:%d events poll", sock_fd, current_thread_id);
        } else {
            log_warning("sock_fd:%d has closed, ignore", sock_fd);
        }
    }
}

static void handle_one_event(events_poll_t * events_poll, uint32_t id)
{
    struct epoll_event * events_obj = &(events_poll->events_array[id]);
    int sock_fd = events_obj->data.fd;
    fd_info_t * fd_info = &(events_poll->fds_info_array[sock_fd]);

    if (sock_fd == fd_info->fd) {
        if (sock_fd == listen_fd) {
            deal_server_socket_events(
                events_poll, sock_fd, events_obj->events);
        } else {
            deal_data_socket_events(
                events_poll, sock_fd, events_obj->events);
        }
    } else {
        log_notice("skip %s: sock_fd:%d already deleted from events poll",
                   get_events_string(events_obj->events), sock_fd);
        // 不能关闭 sock_fd，因为之前已经关闭了，此时 accept() 可能已经生成了一
        // 个相同的 sock_fd 分发到其他的线程。如果强行关闭 sock_fd，就会发生多个
        // 线程同时读写同一个 conn_info 的情况，会发生错误。
    }
}

int run_events_poll(events_poll_t * events_poll, uint32_t wait_time) // wait_time 的单位是毫秒
{
    // 当前状态下只可能有 EINTR 的错误
    int events_cnt = epoll_wait(events_poll->epoll_fd,
                                events_poll->events_array, MAX_EVENTS_CNT,
                                wait_time);
    if (events_cnt > 0) {
        int i = 0;
        for (i = 0; i < events_cnt; i++) {
            handle_one_event(events_poll, i);
        }
    }
    return events_cnt;
}
