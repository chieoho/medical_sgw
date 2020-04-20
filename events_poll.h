
// events_poll.h

#ifndef EVENTS_POLL_H
#define EVENTS_POLL_H

#include <stdint.h>
#include <sys/epoll.h>

// 最高的静态分配的最大连接数设置为一千。没有必要设置为四十万，这样很容易使静态
// 分配的数据在链接时超出最大的限制（>2G），从而引发链接错误。
//
// 例如，下面的链接错误是在 conn_info[400000] 时出现的：
//
// /usr/bin/x86_64-linux-gnu-ld: failed to convert GOTPCREL relocation; relink with --no-relax

#define MAX_CONNS_CNT 50000

typedef struct fd_info
{
	int fd;
    uint32_t events;
} fd_info_t;

#define MAX_EVENTS_CNT		256

typedef struct events_poll_
{
	uint32_t flags;
	int epoll_fd;
	struct epoll_event events_array[MAX_EVENTS_CNT];
	fd_info_t fds_info_array[MAX_CONNS_CNT];
}events_poll_t;


int setup_events_poll(events_poll_t * events_poll);

int add_to_events_poll(events_poll_t * events_poll, int sock_fd, uint32_t events);

int delete_from_events_poll(events_poll_t * events_poll, int sock_fd);

int start_monitoring_send(events_poll_t * events_poll, int sock_fd);

int start_monitoring_recv(events_poll_t *events_poll, int sock_fd);

int stop_monitoring_recv(events_poll_t * events_poll, int sock_fd);

int stop_monitoring_send(events_poll_t * events_poll, int sock_fd);

int run_events_poll(events_poll_t * events_poll, uint32_t wait_time);



#endif


