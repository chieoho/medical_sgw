
// handler.c

#define _GNU_SOURCE

#include <assert.h>
#include <unistd.h>
#include "config.h"
#include "mt_log.h"
#include "public.h"
#include "timer_set.h"
#include "events_poll.h"
#include "conn_mgmt.h"
#include "pathops.h"
#include "md5ops.h"
#include "version.h"
#include "md5.h"

uint32_t region_id = 0;
uint32_t system_id = 0;
uint32_t group_id = 0;
char local_ip[MAX_IP_LEN+1] = {0};
uint16_t local_port = 0;
uint32_t local_id = 0;
char connect_ip[MAX_IP_LEN+1] = {0};
uint16_t connect_port = 0;
char asm_ip[MAX_IP_LEN+1] = {0};
uint16_t asm_port = 0;
uint32_t asm_id = 0;
extern char log_file[MAX_NAME_LEN+1];
extern int is_specified_log_file;
int backend_cnt = 0;
char backend_dirs[MAX_BACK_END][MAX_NAME_LEN+1] = {{0}};
char *default_md5sum_filename = "md5sum.txt";

conn_info_t conns_info[MAX_CONNS_CNT] = {{0}};

static int close_and_check_md5(conn_info_t * c);


// 数据迁移时，存储网关内部状态

enum migration_state {
    S_NORMAL = 1, // 未进行数据迁移
    S_MIGRATING,  // 正在迁移
    S_MIGRATED    // 数据迁移完成
};

static const char *state_string(enum migration_state s)
{
    if (s == S_NORMAL) return "S_NORMAL";
    if (s == S_MIGRATING) return "S_MIGRATING";
    if (s == S_MIGRATED) return "S_MIGRATED";
    return "UNKNOWN";
}

pthread_spinlock_t migstate_lock;
enum migration_state migstate = S_NORMAL; // 全局的数据迁移状态

void migstate_init(void)
{
    int ret = pthread_spin_init(&migstate_lock, PTHREAD_PROCESS_PRIVATE);
    assert(ret == 0);
}

enum migration_state migstate_get(void)
{
    int ret;

    ret = pthread_spin_lock(&migstate_lock); assert(ret == 0);
    enum migration_state retstate = migstate;
    ret = pthread_spin_unlock(&migstate_lock); assert(ret == 0);

    return retstate;
}

void migstate_set(enum migration_state state)
{
    int ret;

    ret = pthread_spin_lock(&migstate_lock); assert(ret == 0);
    migstate = state;
    ret = pthread_spin_unlock(&migstate_lock); assert(ret == 0);
}

int workers = 4;
int curr_worker = 1;
int pipefd[MAX_WORKERS+1][2] = {{-1}};  // [.][0] : read endpoint, [.][1] : write endpoint
int epoll_fds[MAX_WORKERS+1] = {-1};
events_poll_t events_polls[MAX_WORKERS+1] = {{0}};
timer_set_t * timer_sets[MAX_WORKERS+1] = {NULL};

static int init_pipefd(int fd, int wid)
{
    int flags = fcntl(fd, F_GETFL, 0);
    int errno_cached = errno;
    if (flags != -1)
    {
        flags |= O_NONBLOCK;
        int ret = fcntl(fd, F_SETFL, flags);
        int errno_cached2 = errno;
        if (ret != -1)
        {
            conns_info[fd].peer_type = NODE_TYPE_PIPE;
            conns_info[fd].sock_fd = fd;
            conns_info[fd].thread_id = wid;
            return 0;
        }
        else
        {
            char buffer[1024];
            snprintf(buffer, sizeof(buffer),
                     "fcnctl(%d, F_SETFL, %d) failed: %s",
                     fd, flags, strerror(errno_cached2));
            return -1;
        }
    }
    else
    {
        char buffer[1024];
        snprintf(buffer, sizeof(buffer),
                 "fcntl(fd=%d, F_GETFL, 0) failed: %s",
                 fd, strerror(errno_cached));
        printf("%s", buffer);
        log_crit("%s", buffer);
        return -1;
    }
}

static int worker_create_pipe(int wid)
{
    int ret = pipe(pipefd[wid]);
    int errno_cached = errno;
    if (ret == 0)
    {
        int rdend = pipefd[wid][0];
        int wrend = pipefd[wid][1];
        ret = init_pipefd(rdend, wid);
        if (ret == 0)
        {
            ret = init_pipefd(wrend, wid);
            if (ret == 0)
            {
                return 0;
            }
            else
            {
                log_error("init_pipefd wrend:%d failed", wrend);
                goto error;
            }
        }
        else
        {
            log_error("init_pipefd rdend:%d failed", rdend);
            goto error;
        }
    error:
        close(rdend);
        fprintf(stderr, "> closed rdend:%d", rdend);
        log_error("> closed rdend:%d", rdend);
        close(wrend);
        fprintf(stderr, "> closed wrend:%d", wrend);
        log_error("> closed wrend:%d", wrend);
        return -1;
    }
    else
    {
        char buffer[1024];
        snprintf(buffer, sizeof(buffer),
                 "create pipe failed: %s", strerror(errno_cached));
        fprintf(stderr, "%s", buffer);
        log_crit("%s", buffer);
        return -1;
    }
}

int init_dispatch_tunnel(void)
{
    int i = 0;
    int j;
    for (i = 1; i <= workers; i++)
    {
        int ret = worker_create_pipe(i);
        if (ret != 0)
        {
            log_crit("worker:%d create pipe failed", i);
            goto error;
        }
    }
    return 0;

error:
    for (j = 0; j < i; j++)
    {
        close(pipefd[j][0]);
        log_error("> closed read end pipefd:%d", pipefd[j][0]);
        close(pipefd[j][1]);
        log_error("> closed write end pipefd:%d", pipefd[j][1]);
    }
    return -1;
}

// 将套接字描述符发送到工作者处理
// 返回值：
//            -1 - 分发套接字描述符失败
//     worker_id - 分发到的工作者 id
int dispatch_work(int sock_fd)
{
    size_t size = sizeof(sock_fd);
    ssize_t ret = write(pipefd[curr_worker][1], &sock_fd, size);
    int errno_cached = errno;
    if (ret != (ssize_t)size)
    {
        log_error("dispatch sock_fd %d to worker %d failed: %u expect, %d write: %s",
                  sock_fd, curr_worker,
                  (uint32_t)size, (int)ret, strerror(errno_cached));
        if (errno_cached == EPIPE)
        {
            sleep(3);
            abort(); // 管道的读端被关闭，等待日志打印完毕，生成 core 文件分析
        }
        return -1;
    }
    else
    {
        int wid = curr_worker;
        curr_worker = curr_worker == workers ? 1 : curr_worker + 1;
        return wid;
    }
}


typedef struct thread_info_
{
    int thread_id;
} thread_info_t;

thread_info_t threads_info[MAX_WORKERS+1] = {{0}};

pthread_key_t thread_key;
pthread_once_t thread_once = PTHREAD_ONCE_INIT;

void make_thread_key(void)
{
    pthread_key_create(&thread_key, NULL);
}

void init_mt_cntt(int thread_id)
{
    thread_info_t * thread_info = NULL;

    pthread_once(&thread_once, make_thread_key);
    thread_info = (thread_info_t *)pthread_getspecific(thread_key);
    if (thread_info == NULL)
    {
        thread_info = &threads_info[thread_id];
        pthread_setspecific(thread_key, thread_info);
    }

    thread_info->thread_id = thread_id;
}

int get_thread_id(void)
{
    thread_info_t * thread_info = (thread_info_t *)pthread_getspecific(thread_key);
    return thread_info->thread_id;
}

// 这里不关闭 curr_conn_info->sock_fd
// 由 deal_data_socket_epollin() 在处理消息失败后统一关闭
int forward_message(events_poll_t * events_poll, conn_info_t * curr_conn_info, msg_t * msg)
{
    conn_info_t * next_conn_info = NULL;
    int next_sock_fd = -1;
    uint32_t command = 0;
    int len = 0;

    len = msg->length;

    if (curr_conn_info->use_proxy != 1)
    {
        log_error("sock_fd:%d peer:{%s, %u} use_proxy:%u",
                  curr_conn_info->sock_fd, curr_conn_info->peer_ip, curr_conn_info->peer_port, curr_conn_info->use_proxy);
        return -1;
    }

    next_sock_fd = curr_conn_info->next_sock_fd;
    if (next_sock_fd < 3 || next_sock_fd >= MAX_CONNS_CNT)
    {
        log_error("sock_fd:%d peer:{%s, %u} next_sock_fd:%u",
                  curr_conn_info->sock_fd, curr_conn_info->peer_ip, curr_conn_info->peer_port, curr_conn_info->next_sock_fd);
        return -1;
    }

    next_conn_info = &conns_info[next_sock_fd];

    msg->src_type = NODE_TYPE_SGW;
    msg->src_id = local_id;
    msg->dst_type = next_conn_info->peer_type;
    msg->dst_id = next_conn_info->peer_id;

    command = msg->command;

    encode_msg(msg);
    if (send_message(events_poll, next_conn_info, (uint8_t *)msg, len) != len)
    {
        log_error("forward message:%08X to %s:%d fail",
                  command, next_conn_info->peer_ip, next_conn_info->peer_port);
        close_tcp_conn(events_poll, next_conn_info->sock_fd);
        return -1;
    }
    else
    {
        return len;
    }
}

int send_response_message(events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg, int len)
{
    uint32_t command = 0;

    msg->length = len;
    msg->src_type = NODE_TYPE_SGW;
    msg->dst_type = conn_info->peer_type;
    msg->dst_id = conn_info->peer_id;
    msg->src_id = local_id;

    command = msg->command;
    msg->command = command + 1;

    encode_msg(msg);

    if (send_message(events_poll, conn_info, (uint8_t *)msg, len) != len)
    {
        log_error("%s:%lu: send %d bytes to client {%s:%d} failed",
                  command_string(command+1), msg->sequence, msg->length,
                  conn_info->peer_ip, conn_info->peer_port);
        return -1;
    }
    else
    {
        // pr_msg_packed(msg);
        return len;
    }
}

static int __write_data(int file_fd, uint8_t * data, int len)
{
    int errno_cached;
    int index, loop, ret;

    loop = 16;
    index = 0;
    while (index < len && loop > 0)
    {
        ret = write(file_fd, &data[index], len-index);
        errno_cached = errno;
        if (ret < 0)
        {
            if (errno_cached == EINTR)
            {
                loop -= 1;
            }
            else
            {
                log_error("write %d bytes to fd %d failed: %s",
                          len-index, file_fd, strerror(errno_cached));
                return -1;
            }
        }
        else
        {
            index += ret;
            loop -= 1;
        }
    }

    if (index == len)
    {
        return len;
    }
    else
    {
        log_error("write fd %d failed: want=%d, write=%d",
                  file_fd, len, index);
        return -1;
    }
}

int write_data(int file_fd, uint64_t offset, uint8_t * data, int len)
{
    off_t off = lseek(file_fd, offset, SEEK_SET);
    int errno_cached = errno;
    if (off == (off_t)-1)
    {
        log_error("lseek(%d, %lu, SEEK_SET) failed: %s",
                  file_fd, offset, strerror(errno_cached));
        return -1;
    }
    else
    {
        return __write_data(file_fd, data, len);
    }
}

static int __read_data(int file_fd, uint8_t * data, int len)
{
    int errno_cached;
    int index, max_loop, ret;

    max_loop = 16;
    index = 0;
    while (index < len && max_loop > 0)
    {
        ret = read(file_fd, &data[index], len - index);
        errno_cached = errno;
        if (ret < 0)
        {
            if (errno_cached == EINTR)
            {
                max_loop -= 1;
            }
            else
            {
                log_error("read %d bytes from fd %d failed: %s",
                          len - index, file_fd, strerror(errno_cached));
                return -1;
            }
        }
        else
        {
            index += ret;
            max_loop -= 1;
        }
    }

    return index;
}

int read_data(int file_fd, uint64_t offset, uint8_t * data, int len)
{
    off_t off = lseek(file_fd, offset, SEEK_SET);
    int errno_cached = errno;
    if (off == (off_t)-1)
    {
        log_error("lseek(%d, %lu, SEEK_SET) failed: %s",
                  file_fd, offset, strerror(errno_cached));
        return -1;
    }
    else
    {
        return __read_data(file_fd, data, len);
    }
}

int connect_to_next_sgw(events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    task_info_t * task_info = (task_info_t *)(msg->data);
    conn_info_t * next_conn_info = NULL;
    char sgw_ip[MAX_IP_LEN+1];
    int next_sock_fd = -1;

    inet_ntop(AF_INET, &(task_info->sgw_ip), sgw_ip, sizeof(sgw_ip));

    next_sock_fd = open_tcp_conn(events_poll, sgw_ip, task_info->sgw_port, NULL, 0, 0);
    if (next_sock_fd < 3 || next_sock_fd >= MAX_CONNS_CNT)
    {
        log_error("try connecting to sgw:{%s:%d} fail",
                  sgw_ip, task_info->sgw_port);
        return -1;
    }

    conn_info->use_proxy = 1;
    conn_info->next_sock_fd = next_sock_fd;

    next_conn_info = &conns_info[next_sock_fd];
    next_conn_info->peer_type = NODE_TYPE_SGW;
    next_conn_info->peer_id = task_info->sgw_id;
    next_conn_info->trans_id = msg->trans_id;
    next_conn_info->sequence = msg->sequence;
    next_conn_info->thread_id = get_thread_id();

    next_conn_info->use_proxy = 1;
    next_conn_info->next_sock_fd = conn_info->sock_fd;

    return next_sock_fd;
}

static int is_local_ip(uint32_t sgw_ip)
{
    struct in_addr local_addr;
    int ret;

    ret = inet_aton(local_ip, &local_addr);
    if (ret == 1)
    {
        return sgw_ip == local_addr.s_addr;
    }
    else
    {
        log_debug("> inet_aton failed: convert local_ip %s failed",
                  local_ip);
        return 0;
    }
}

static int is_connect_ip(uint32_t sgw_ip)
{
    struct in_addr connect_addr;
    int ret;

    ret = inet_aton(connect_ip, &connect_addr);
    if (ret == 1)
    {
        return sgw_ip == connect_addr.s_addr;
    }
    else
    {
        log_debug("> inet_aton failed: convert connect_ip %s failed",
                  connect_ip);
        return 0;
    }
}

// 1: 给出的 sgw_ip 是本地监听的 IP 或者对外发布的连接 IP；
// 0: 不是本地监听的 IP，也不是对外发布的连接 IP
static int is_listening_ip(uint32_t sgw_ip)
{
    int ret;

    ret = is_connect_ip(sgw_ip);
    if (ret == 1)
    {
        return 1;
    }
    else
    {
        ret = is_local_ip(sgw_ip);
        if (ret == 1)
        {
            return 1;
        }
        else
        {
            char sgw_ip_str[MAX_IP_LEN+1];
            const char * ptr = inet_ntop(AF_INET, &sgw_ip, sgw_ip_str, sizeof(sgw_ip_str));
            if (ptr)
            {
                log_debug(
                    "= sgw_ip %s is not local_ip(%s) or connect_ip(%s)",
                    sgw_ip_str, local_ip, connect_ip);
            }
            else
            {
                log_debug("> inet_ntop failed: convert 32 bit ip 0x%x failed",
                          sgw_ip);
            }
            return 0;
        }
    }
}

/*
 * 只在上传时使用这个函数。打开路径名的文件，成功返回文件描述符，失败返回 -1。如
 * 果文件已存在，则首先会截断。
 */
int open_path(char *path)
{
    char tmp[2048];
    int i;
    int pathlen;
    int fd;
    char *fullpath;

    /* 0. 首先尝试打开文件，如果存在则直接返回 */
//    fd = open(path, O_RDWR | O_TRUNC);
    /*医疗版本现在没有更新，所以这里不用截断，以后有更新需求时，另外增加更新命令字*/
    fd = open(path, O_RDWR);
    if (fd >= 0) {
        return fd;
    } else /* 文件不存在，创建文件 */ {
        pathlen = strlen(path);
        if (path[0] != '/') {
            /* 路径名是相对路径，加上当前目录的路径 */
            char *current_dir = get_current_dir_name();
            int enumber = errno;
            if (current_dir) {
                int dirlen = strlen(current_dir);
                fullpath = malloc(dirlen + pathlen + 2); /* 2 = 1(/) + 1(\0) */
                int enumber = errno;
                if (fullpath) {
                    pathlen = sprintf(fullpath, "%s/%s", current_dir, path);
                    /* sprintf() 返回的值不包括 \0，fullpath[pathlen] == '\0' */
                } else {
                    log_error("malloc failed: %s\n", strerror(enumber));
                    return -1;
                }
            } else {
                log_error("get_current_dir_name failed: %s\n", strerror(enumber));
                return -1;
            }
        } else {
            /* 路径名是绝对路径，不需要处理 */
            fullpath = malloc(pathlen+1);
            if (fullpath) {
                memmove(fullpath, path, pathlen+1);
            } else {
                int enumber = errno;
                log_error("malloc failed: %s\n", strerror(enumber));
                return -1;
            }
        }

        /* 1. 从路径名中找到已存在的目录 */
        i = 0;
        int j;
        for (j = pathlen-1; j >= 0; j--) {
            if (fullpath[j] == '/') {
                /*
                 * 遇到了一个目录，我们看看这个目录存不存在，如果存在则继
                 * 续向上找一个不存在的目录
                 */
                if (i > 0) {
                    if (tmp[i-1] != '/') {
                        tmp[i] = fullpath[j];
                        i = i + 1;
                    } else {
                        /* 连续两个目录分隔符，只保存一个 */
                    }
                } else {
                    tmp[i] = fullpath[j];
                    i = i + 1;
                }
                fullpath[j] = '\0';
                // log_info("j: %d, fullpath: %s", j, fullpath);
                struct stat s;
                int rc = stat(fullpath, &s);
                int enumber = errno;
                if (rc == 0) {
                    /* 如果路径名已存在，则需要判断是否目录 */
                    if (S_ISDIR(s.st_mode)) {
                        /*
                         * 找到了第一个目录，不用再向上查找；同时为了
                         * 方便处理，临时路径缓存保持以非目录分隔符为
                         * 第一个扫描字符。现在找到了第一个存在的目录，
                         * 终止循环。
                         */
                        // log_info("%s already exists, is a directory", fullpath);
                        i = i - 1;
                        fullpath[j] = tmp[i];
                        j = j + 1;
                        break;
                    } else {
                        log_error("%s is not a directory\n", fullpath);
                        goto out;
                    }
                } else {
                    if (enumber == ENOENT) {
                        /*
                         * 当前的目录不存在，继续向上检查；没有特殊操
                         * 作，继续下一次循环。
                         */
                    } else {
                        log_error("%s: %s\n", fullpath, strerror(enumber));
                        goto out;
                    }
                }
            } else {
                /* 非目录分隔符，保存起来 */
                tmp[i] = fullpath[j];
                i = i + 1;
                fullpath[j] = '\0';
                // log_info("%c, %s", tmp[i-1], fullpath);
            }
        }
        assert(i < (int)sizeof(tmp));

        /* 2. 创建不存在的目录 */
        int k;
        for (k = i-1; k >= 0; k--) {
            // log_info("k: %d, fullpath: %s", k, fullpath);
            fullpath[j] = tmp[k];
            j = j + 1;
            if (tmp[k] == '/') {
                /* 遇到了目录，创建之 */
                int rc = mkdir(fullpath, 0755);
                int enumber = errno;
                if (rc != 0) {
                    if (enumber != EEXIST) {
                        log_error("create directory %s failed: %s\n",
                                  fullpath, strerror(enumber));
                        goto out;
                    } else {
                        /* 目录已存在，继续处理 */
                        // log_info("skip exists directory %s", fullpath);
                    }
                } else {
                    /* 目录创建成功，继续处理 */
                    // log_info("created directory %s", fullpath);
                }
            } else {
                /* 非目录分隔符，回放到路径名中 */
                // log_info("put back %c to path", tmp[k]);
            }
        }
        /* 我们没有对原路径名的结束符做过改动，保留原样 */

        /* 3. 创建文件 */
        int fd = open(fullpath, O_CREAT|O_CLOEXEC|O_RDWR, 0644);
        if (fd >= 0) {
            // log_info("created file %s", fullpath);
            return fd;
        } else {
            int ec1 = errno;
            log_error("create file %s failed: %s", fullpath, strerror(ec1));
            goto out;
        }
    out:
        free(fullpath);
        return -1;
    }
}

static void setup_abs_file_name(
    char *abspath, size_t pathlen,
    msg_t *msg, char *basedir_name)
{
    task_info_t * t = (task_info_t *)(msg->data);
    if (t->file_name[0] == '/') {
        snprintf(abspath, pathlen, "%s%s", basedir_name, t->file_name);
    } else {
        snprintf(abspath, pathlen, "%s/%s", basedir_name, t->file_name);
    }
}

static int execute_command(const char *input)
{
    char command[8192];
    snprintf(command, sizeof(command), "%s", input);
    int status = system(command);
    if (status == -1)
    {
        int errno_cached = errno;
        log_error("system(%s) failed: %s",
                  command, strerror(errno_cached));
        return -1;
    }
    else
    {
        int exit_normal = WIFEXITED(status);
        if (exit_normal)
        {
            int exit_code = WEXITSTATUS(status);
            if (exit_code == 0)
            {
                return 0;
            }
            else
            {
                log_error("[%s] failed: exit code %d", command, exit_code);
                return -1;
            }
        }
        else
        {
            log_error("[%s] exit abnormal, status: 0x%x", command, status);
            return -1;
        }
    }
}

static int handle_fd_error(char *abs_file_name, int fd, int errno_cached)
{
    if (3 <= fd && fd < MAX_CONNS_CNT)
    {
        return 0;
    }
    else if (fd == -1)
    {
        log_error("> error on %s: %s",
                  abs_file_name, strerror(errno_cached));
        return -1;
    }
    else if (fd >= MAX_CONNS_CNT)
    {
        log_error("> error on %s: reach limits %d",
                  abs_file_name, MAX_CONNS_CNT);
        int ret = close(fd);
        errno_cached = errno;
        if (ret == -1)
        {
            log_crit("> close fd %d failed: %s",
                     fd, strerror(errno_cached));
        }
        else
        {
            log_error("> closed fd:%d", fd);
        }
        return -1;
    }
    else
    {
        log_error("> unexpected fd %d", fd);
        int ret = close(fd);
        errno_cached = errno;
        if (ret == -1)
        {
            log_crit("> close fd %d failed: %s",
                     fd, strerror(errno_cached));
        }
        else
        {
            log_error("> closed fd:%d", fd);
        }
        return -1;
    }
}

void save_backend_file_struct(
    struct backend_file * f,
    msg_t * m, int fd, char * abs_file_name)
{
    task_info_t * ti = (task_info_t *)m->data;
    snprintf(f->md5, sizeof(f->md5), "%s", ti->file_md5);
    snprintf(f->abs_file_name, sizeof(f->abs_file_name), "%s", abs_file_name);
    f->fd = fd;
    f->filesize = m->total;
    f->fileleft = m->total;
    f->filedone = 0;
}

static void print_hex(const char *mem, int len)
{
    char buffer[4096] = {'\0'};
    int i;
    for (i = 0; i < len; i++) {
        sprintf(&buffer[i], "%c", mem[i]);
    }
    // log_info("buffer: %s", buffer);
}

static int create_one_backend_fd(conn_info_t * conn_info, msg_t * msg, int index)
{
    char abs_file_name[MAX_PATH_LEN + MAX_NAME_LEN + 1];
    setup_abs_file_name(abs_file_name, sizeof(abs_file_name), msg, backend_dirs[index]);
    int errno_cached;
    int fd = open_path(abs_file_name);
    errno_cached = errno;
    int ret = handle_fd_error(abs_file_name, fd, errno_cached);
    if (ret == 0)
    {
        save_backend_file_struct(&conn_info->befiles[index],
                                 msg, fd, abs_file_name);
        task_info_t *t = (task_info_t *)msg->data;
        int md5len = 0;
        char *p = t->file_md5;
        while (p < t->file_md5 + 32) {
            if (*p == '\0') {
                break;
            } else {
                md5len = md5len + 1;
            }
            p = p + 1;
        }
        if (md5len == 32 && t->file_md5[32] == '\0') {
            return 0;
        } else {
            log_error("invalid filemd5 length %d", md5len);
            print_hex(t->file_md5, 33);
            return -1;
        }
    }
    else
    {
        log_error("create %s failed", abs_file_name);
        return -1;
    }
}

static int create_backend_fds(conn_info_t * conn_info, msg_t * msg)
{
    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        int ret = create_one_backend_fd(conn_info, msg, i);
        if (ret == -1)
        {
            return -1;
        }
    }

    return 0;
}

/* 成功接收到一个完整的包返回 0，没有接收到完整的包返回 -1 */
static ssize_t recv_packet(int sd, char *buf, ssize_t buflen)
{
    ssize_t recvsize;

    recvsize = recv(sd, buf, 4, MSG_WAITALL);
    if (recvsize == 4) {
        // log_debug("recv msglen from sock_fd:%d finished ...", sd);
    } else {
        log_error("recv msglen from sock_fd:%d failed: %d recv, %d expect",
                  sd, (int)recvsize, 4);
        return -1;
    }

    uint32_t msglen = be32toh(*((int *)(&buf[0])));
    if (sizeof(msg_t) <= msglen && msglen <= buflen) {
        // log_debug("good msglen %u from sock_fd:%d", msglen, sd);
    } else {
        log_error("recv invalid msglen %u from sock_fd:%d!",
                  msglen, sd);
        return -1;
    }

    ssize_t leftsize = msglen - 4;
    ssize_t donesize = 4;
    while (leftsize > 0) {
        recvsize = recv(sd, buf + donesize, leftsize, MSG_WAITALL);
        if (recvsize > 0) {
            leftsize = leftsize - recvsize;
            donesize = donesize + recvsize;
        } else if (recvsize == 0) {
            if (leftsize == 0) {
                // 对端关闭了连接，也接收到完整的消息包：这种情况应该不会出现。
                log_error("recv_packet completed, but sock_fd:%d peer closed", sd);
            } else {
                log_error("sock_fd:%d recv_packet uncompleted: %lld bytes left, peer closed",
                          sd, (long long int)leftsize);
            }
            return -1;
        } else {
            int ec = errno;
            if (ec == EINTR) {
                /* 系统调用被中断，继续接收 */
            } else {
                log_error("%d want, %d recv, %d left: %s",
                        (int)msglen, (int)donesize, (int)leftsize, strerror(ec));
                return -1;
            }
        }
    }

    return 0;
}

static int sendall(int sd, const char *buf, int len)
{
    int left = len;
    int done = 0;
    while (left > 0) {
        int n = send(sd, buf, left, MSG_WAITALL);
        if (n > 0) {
            left = left - n;
            done = done + n;
        } else if (n == 0) {
            /* 没有发送到数据，继续重试 */
        } else {
            int ec = errno;
            if (ec == EINTR) {
                /* 系统调用被中断，继续重试 */
            } else {
                log_error("send %d bytes failed: %s",
                          len, strerror(ec));
                return -1;
            }
        }
    }
    return done;
}

struct upctx {
    events_poll_t *ep;
    conn_info_t *ci;
    msg_t *msg;
    uint64_t filesize;
    char buffer[MAX_MESSAGE_LEN];
    char filename[MAX_NAME_LEN];
};

/* 处理上传开始请求（CMD_START_UPLOAD_REQ） */
static int workrq0(struct upctx *ctx)
{
    assert(ctx);
    assert(ctx->ci);
    assert(ctx->msg);

    conn_info_t *c = ctx->ci;
    msg_t *m = ctx->msg;
    int rc = create_backend_fds(c, m);
    if (rc == 0) {
        ctx->filesize = m->total;
        m->ack_code = 200;
        return 0;
    } else {
        log_error("create_backend_fds failed");
        m->ack_code = 404;
        return -1;
    }
}

static int sendrs0(struct upctx *ctx)
{
    assert(ctx);
    assert(ctx->ci);
    assert(ctx->msg);

    msg_t *m = ctx->msg;
    m->command = m->command + 1;
    m->src_type = NODE_TYPE_SGW;
    m->src_id = local_id;
    m->dst_type = ctx->ci->peer_type;
    m->dst_id = ctx->ci->peer_id;
    m->length = sizeof(msg_t) + sizeof(task_info_t);
    encode_msg(m);

    task_info_t *t = (task_info_t *)(m->data);
    encode_task_info(t);

    int msglen = sizeof(msg_t) + sizeof(task_info_t);
    int sendlen = sendall(ctx->ci->sock_fd, (const char *)m, msglen);
    if (sendlen == msglen) {
        return 0;
    } else {
        log_error("sendrs0 failed: %d send, %d want", sendlen, msglen);
        return -1;
    }
}

static int __recvrq(struct upctx *ctx)
{
    assert(ctx);
    assert(ctx->ci);
    assert(ctx->ci->sock_fd >= 0);

    int sd = ctx->ci->sock_fd;
    char *buffer = ctx->buffer;
    int buflen = sizeof(ctx->buffer);
    int rc = recv_packet(sd, buffer, buflen);
    if (rc == 0) {
        ctx->msg = (msg_t *)(ctx->buffer);
        return 0;
    } else {
        log_error("recv packet from sock_fd:%d failed!", sd);
        return -1;
    }
}

static int recvrq1(struct upctx *ctx)
{
    return __recvrq(ctx);
}

static int chkmsg1(msg_t *m)
{
    if (m->command == CMD_UPLOAD_DATA_REQ) {
        if (m->length == m->count + sizeof(msg_t)) {
            if (0 < m->count && m->count <= MAX_MESSAGE_LEN) {
                return 1;
            } else {
                log_error("%s: invalid payload length %u",
                          command_string(m->command),
                          m->count);
                return 0;
            }
        } else {
            log_error("%s: invalid length %u",
                      command_string(m->command),
                      m->length);
            return 0;
        }
    } else {
        log_error("command expect: %s, command recv: %s",
                  command_string(CMD_UPLOAD_DATA_REQ),
                  command_string(m->command));
        return 0;
    }
}

int check_stub_dir(const char *dirpath)
{
    struct stat s;
    int rc = stat(dirpath, &s);
    if (rc == 0) {
        if (S_ISDIR(s.st_mode)) {
            return 0;
        } else {
            log_error("dirpath %s is not a directory", dirpath);
            return -1;
        }
    } else {
        if (errno == ENOENT) {
            log_error("no %s: mount point disappear?", dirpath);
        } else {
            log_error("check %s failed: %s", dirpath, strerror(errno));
        }
        return -1;
    }
}

static int workrq1(struct upctx *ctx)
{
    assert(ctx);
    assert(ctx->ep);
    assert(ctx->ci);
    assert(ctx->msg);

    conn_info_t *c = ctx->ci;
    msg_t *m = ctx->msg;
    int i;
    for (i = 0; i < backend_cnt; i++) {
        char checkpoint[MAX_PATH_LEN];
        sprintf(checkpoint, "%s/%s", backend_dirs[i], MNTDIRNAME);
        int rc = check_stub_dir(checkpoint);
        if (rc == 0) {
            /* pass */
        } else {
            log_error("check_stub_dir failed");
            return -1;
        }

        int n = write_data(
            c->befiles[i].fd, m->offset, m->data, m->count);
        if (n == (int)m->count) {
            // 写入文件成功，继续写入下一个后端文件
        } else {
            // 写入文件失败，立刻返回，不再写入其他的后端文件
            log_error("write %s:%lld failed: %d want, %d write",
                      c->befiles[i].abs_file_name,
                      (long long int)m->offset,
                      (int)m->count, n);
            return -1;
        }
    }
    m->ack_code = 200;
    return 0;
}

// 响应消息只有消息头部
static int sendrs1(struct upctx *ctx)
{
    assert(ctx);
    assert(ctx->ep);
    assert(ctx->ci);
    assert(ctx->ci->sock_fd >= 0);
    assert(ctx->msg);

    msg_t *m = ctx->msg;
    m->command = m->command + 1;
    m->src_type = NODE_TYPE_SGW;
    m->src_id = local_id;
    m->dst_type = ctx->ci->peer_type;
    m->dst_id = ctx->ci->peer_id;
    m->length = sizeof(msg_t);
    encode_msg(m);

    int msglen = sizeof(msg_t);
    int sd = ctx->ci->sock_fd;
    int sendlen = sendall(sd, (const char *)m, msglen);
    if (sendlen == msglen) {
        return 0;
    } else {
        log_error("sendrs1 failed: %d send, %d want", sendlen, msglen);
        return -1;
    }
}

static int recvrq2(struct upctx *ctx)
{
    return __recvrq(ctx);
}

static int chkmsg2(msg_t *m)
{
    if (m->command == CMD_UPLOAD_FINISH_REQ) {
        if (m->length == sizeof(msg_t)) {
            return 1;
        } else {
            log_error("%s: invalid length %u",
                      command_string(m->command), m->length);
            return 0;
        }
    } else {
        log_error("command expect: %s, command recv: %s",
                  command_string(CMD_UPLOAD_FINISH_REQ),
                  command_string(m->command));
        log_error("message content: length=%lld, total=%lld, count=%lld, offset=%lld",
                  (long long int)m->length,
                  (long long int)m->total,
                  (long long int)m->count,
                  (long long int)m->offset);
        return 0;
    }
}

static int workrq2(struct upctx *ctx)
{
    assert(ctx);
    assert(ctx->ci);
    assert(ctx->msg);

    conn_info_t *c = ctx->ci;
    msg_t *m = ctx->msg;
    int rc = close_and_check_md5(c);
    if (rc == 0) {
        struct backend_file *f = &c->befiles[0];
#if HAVE_CHECK_MD5
        rc = savemd5(f->abs_file_name, f->md5);
#else
        rc = 0;
#endif
        if (rc == 0) {
            m->ack_code = 200;
            return 0;
        } else {
            m->ack_code = 404;
            log_error("savemd5 failed: filename %s, md5 %s",
                      f->abs_file_name, f->md5);
            return -1;
        }
    } else {
        m->ack_code = 404;
        log_error("close_and_check_md5 on sock_fd:%d failed", c->sock_fd);
        return -1;
    }
}

static int sendrs2(struct upctx *ctx)
{
    return sendrs1(ctx);
}

static int handle_start_upload_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    // log_debug("enter start upload request");

    struct upctx ctx;
    ctx.ep = events_poll;
    ctx.ci = conn_info;
    task_info_t *t = (task_info_t *)msg->data;
    snprintf(ctx.filename, sizeof(ctx.filename), "%s", t->file_name);

    // 上传开始请求。这个消息已经接收完成，并且完成了消息检查
    ctx.msg = msg;
    int rc0 = workrq0(&ctx);
    if (rc0 == 0) {
        // 处理上传开始请求成功
        // log_debug("workrq0 finished ...");
    } else {
        log_error("workrq0 failed: filename %s, retcode %d", ctx.filename, rc0);
        goto failed;
    }
    rc0 = sendrs0(&ctx);
    if (rc0 == 0) {
        // 发送上传开始响应结束，继续处理上传数据请求
        // log_debug("sendrs0 finished ...");
    } else {
        log_error("sendrs0 failed: filename %s, retcode %d", ctx.filename, rc0);
        goto failed;
    }

    // 上传数据请求
    tcp_setblocking(conn_info->sock_fd);
    int64_t leftsize = ctx.filesize;
    // log_debug("filesize: %lld", (long long int)leftsize);
    while (leftsize > 0) {
        int rc1 = recvrq1(&ctx);
        if (rc1 == 0) {
            // 因为操作逻辑多，会导致代码缩进层次增多。
            // 临时变换为平坦模式的编码风格，阅读起来方便一点。
            // log_debug("recvrq1 finished ...");
        } else {
            log_error("recvrq1 failed: %s (%lld / %lld)",
                      ctx.filename,
                      (long long int)leftsize,
                      (long long int)ctx.filesize);
            goto failed;
        }

        msg_t *m = ctx.msg;
        decode_msg(m);
        int goodmsg = chkmsg1(m);
        if (goodmsg) {
            // 接收到的消息是正常的，继续处理
            // log_debug("chkmsg1 finished ...");
            leftsize = leftsize - m->count;
            // log_debug("sock_fd:%d: %d recv, %lld left", ctx.ci->sock_fd, (int)m->count, (long long int)leftsize);
        } else {
            log_error("chkmsg1 failed: %s (%lld / %lld)",
                      ctx.filename,
                      (long long int)leftsize,
                      (long long int)ctx.filesize);
            goto failed;
        }

        rc1 = workrq1(&ctx);
        if (rc1 == 0) {
            // 处理消息成功，继续处理
            // log_debug("workrq1 finished ...");
        } else {
            log_error("workrq1 failed: %s (%lld / %lld)",
                      ctx.filename,
                      (long long int)leftsize,
                      (long long int)ctx.filesize);
            goto failed;
        }

        rc1 = sendrs1(&ctx);
        if (rc1 == 0) {
            // 发送响应成功，继续接收下一个上传数据的消息
            // log_debug("sendrs1 finished ...");
        } else {
            log_error("sendrs1 failed: %s (%lld / %lld)",
                      ctx.filename,
                      (long long int)leftsize,
                      (long long int)ctx.filesize);
            goto failed;
        }
    }

    // 上传结束请求
    int rc2 = recvrq2(&ctx);
    if (rc2 == 0) {
        // 接收上传结束请求成功
        // log_debug("recvrq2 finished ...");
    } else {
        log_error("recvrq2 failed: filename %s", ctx.filename);
        goto failed;
    }
    msg_t *m = ctx.msg;
    decode_msg(m);
    int goodmsg = chkmsg2(m);
    if (goodmsg) {
        // 接收到的消息是正常的
        // log_debug("chkmsg2 finished ...");
    } else {
        log_error("chkmsg2 failed: filename %s", ctx.filename);
        goto failed;
    }
    rc2 = workrq2(&ctx);
    if (rc2 == 0) {
        // 处理上传结束请求成功
        // log_debug("workrq2 finished ...");
    } else {
        log_error("workrq2 failed: filename %s", ctx.filename);
        goto failed;
    }
    rc2 = sendrs2(&ctx);
    if (rc2 == 0) {
        // 发送上传结束响应成功
        // log_debug("sendrs2 finished ...");
    } else {
        log_error("sendrs2 failed: filename %s", ctx.filename);
        goto failed;
    }

    // 处理成功，连接保留不关闭，以便可以继续使用同一个连接传输
    tcp_setnonblock(conn_info->sock_fd);
    return 0;

failed:
    close_tcp_conn(events_poll, conn_info->sock_fd);
    return -1;
}

static int check_one_backend_file(msg_t * msg, char *basedir_name)
{
    char abs_file_name[MAX_PATH_LEN + MAX_NAME_LEN + 1];
    setup_abs_file_name(abs_file_name, sizeof(abs_file_name), msg, basedir_name);

    struct stat path_stat;
    int ret = stat(abs_file_name, &path_stat);
    int errno_cached = errno;
    if (ret == -1)
    {
        log_error("> check file %s failed: %s",
                  abs_file_name, strerror(errno_cached));
        return -1;
    }
    else
    {
        return 0;
    }
}

/* 返回后端文件正常的数量 */
static int check_backend_files(msg_t * msg)
{
    int goodfiles = 0;
    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        int ret = check_one_backend_file(msg, backend_dirs[i]);
        if (!ret)
        {
            goodfiles = goodfiles + 1;
        }
    }
    return goodfiles;
}

static int open_one_backend_fd(conn_info_t * conn_info, msg_t * msg, int index)
{
    char abs_file_name[MAX_PATH_LEN + MAX_NAME_LEN + 1];
    setup_abs_file_name(abs_file_name, sizeof(abs_file_name), msg, backend_dirs[index]);

    int errno_cached;
    int fd = open(abs_file_name, O_RDONLY);
    errno_cached = errno;

    int ret = handle_fd_error(abs_file_name, fd, errno_cached);
    if (ret == 0)
    {
        save_backend_file_struct(&conn_info->befiles[index],
                                 msg, fd, abs_file_name);
        // log_info("> open backend_fd %d(%s) in connection %d", fd, abs_file_name, conn_info->sock_fd);
        return 0;
    }
    else
    {
        return -1;
    }
}

static int open_backend_fds(conn_info_t * conn_info, msg_t * msg)
{
    int nr_opens = 0;
    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        int ret = open_one_backend_fd(conn_info, msg, i);
        if (ret == 0)
        {
            nr_opens = nr_opens + 1;
        }
    }
    return nr_opens;
}

static int setup_start_download_reponse_message(
    conn_info_t * conn_info, msg_t * msg)
{
    (void) conn_info;
    char abs_file_name[MAX_PATH_LEN + MAX_NAME_LEN + 1];
    struct stat file_stat;
    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        setup_abs_file_name(abs_file_name, sizeof(abs_file_name),
                            msg, backend_dirs[i]);
        memset(&file_stat, 0, sizeof(file_stat));
        int ret = stat(abs_file_name, &file_stat);
        if (ret == 0)
        {
            task_info_t * task_info = (task_info_t *)(msg->data);
            task_info->file_len = file_stat.st_size;
            encode_task_info(task_info);
            msg->total = file_stat.st_size;
            msg->offset = 0UL;
            msg->count = 0UL;
            msg->ack_code = 200;
            return 0;
        }
    }
    return -1;
}

static int handle_start_download_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    int nr_files = check_backend_files(msg);
    if (nr_files > 0)
    {
        int nr_opens = open_backend_fds(conn_info, msg);
        if (nr_opens > 0)
        {
            int ret = setup_start_download_reponse_message(conn_info, msg);
            if (ret == 0)
            {
                return send_response_message(events_poll, conn_info,
                                             msg, msg->length);
            }
            else
            {
                log_error("setup response message failed");
                return -1;
            }
        }
        else
        {
            log_error("open backend files failed");
            return -1;
        }
    }
    else
    {
        log_error("no backend file");
        return -1;
    }
}

static int remove_one_backend_file(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg,
    char * basedir_name)
{
    (void) events_poll;
    (void) conn_info;

    char abs_file_name[MAX_PATH_LEN + MAX_NAME_LEN + 1];
    setup_abs_file_name(abs_file_name, sizeof(abs_file_name), msg, basedir_name);

    int ret = remove(abs_file_name);
    int errno_cached = errno;
    if (ret == 0)
    {
        log_info("> remove file %s ok", abs_file_name);
        return 0;
    }
    else
    {
        log_error("> remove file %s failed: %s", abs_file_name, strerror(errno_cached));
        return -1;
    }
}

static int remove_backend_files(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    int nr_removes = 0;
    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        int ret = remove_one_backend_file(events_poll, conn_info, msg, backend_dirs[i]);
        if (ret == 0)
        {
            nr_removes = nr_removes + 1;
        }
    }
    return nr_removes;
}

static int handle_delete_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    int ack_code;
    int nr_files = check_backend_files(msg);
    if (nr_files > 0)
    {
        int nr_removes = remove_backend_files(events_poll, conn_info, msg);
        if (nr_removes == nr_files)
        {
            ack_code = 200;
        }
        else
        {
            ack_code = 404;
            log_warning("> check %d files, but removed %d files",
                        nr_files, nr_removes);
        }
    }
    else
    {
        // 删除的文件不存在，效果和删除操作一样，也返回成功给客户端
        ack_code = 200;
    }

    task_info_t * task_info = (task_info_t *)(msg->data);
    encode_task_info(task_info);
    msg->ack_code = ack_code;
    return send_response_message(events_poll, conn_info, msg, msg->length);
}

static int handle_common1(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    task_info_t * task_info = (task_info_t *)(msg->data);
    int ret;

    conn_info->peer_id = msg->src_id;
    conn_info->peer_type = msg->src_type;

    decode_task_info(task_info);

    ret = is_listening_ip(task_info->sgw_ip);
    if (ret == 1)
    {
        if (msg->command == CMD_START_UPLOAD_REQ)
        {
            return handle_start_upload_request(events_poll, conn_info, msg);
        }
        else if (msg->command == CMD_START_DOWNLOAD_REQ)
        {
            return handle_start_download_request(events_poll, conn_info, msg);
        }
        else if (msg->command == CMD_DELETE_REQ)
        {
            return handle_delete_request(events_poll, conn_info, msg);
        }
        else
        {
            log_error("unknown command 0x%x", msg->command);
            return -1;
        }
    }
    else
    {
        // 接收到的消息，它要连接的 sgw_ip 不是本节点的监听地址，将这个消息转发
        // 到目标 sgw
        int next_sock_fd = connect_to_next_sgw(events_poll, conn_info, msg);
        if (next_sock_fd > 0)
        {
            encode_task_info(task_info);
            return forward_message(events_poll, conn_info, msg);
        }
        else
        {
            char sgw_ip[MAX_IP_LEN + 1];
            const char *ptr = inet_ntop(AF_INET, &(task_info->sgw_ip), sgw_ip, sizeof(sgw_ip));
            if (ptr)
            {
                log_error("> connect to sgw:{%s:%d} failed", sgw_ip, task_info->sgw_port);
            }
            return -1;
        }
    }
}

static void backend_file_close_fd(struct backend_file *f)
{
    close(f->fd);
    // log_info("> closed fd:%d", f->fd);
    f->fd = -1;
}

static int backend_file_check_md5(struct backend_file *f)
{
#if HAVE_CHECK_MD5
    /*
    // 打开上传文件的 md5 校验
    char command[8192];
    snprintf(command, sizeof(command),
             "echo %s %s | md5sum --check --status -",
             f->md5, f->abs_file_name);
    return execute_command(command);
     */
    return check_md5(f->abs_file_name, f->md5);
#else
    // 关闭上传文件的 md5 校验
    (void) f;
    return 0;
#endif
}

static int close_and_check_md5(conn_info_t * c)
{
    int ret = 0;

    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        struct backend_file * f = &c->befiles[i];
        int rc1;

        backend_file_close_fd(&c->befiles[i]);
        // log_info("> closed %s", c->befiles[i].abs_file_name);

        rc1 = backend_file_check_md5(f);
        if (rc1 == 0) {
            log_debug("%s successfully uploaded (%lld bytes)",
                      c->befiles[i].abs_file_name,
                      (long long int)c->befiles[i].filesize);
        } else {
            log_error("%s check md5 failed", c->befiles[i].abs_file_name);
        }

        ret |= rc1;
    }

    return ret;
}

static int __handle_upload_data_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    if (msg->length == msg->count + sizeof(msg_t))
    {
        int i;
        for (i = 0; i < backend_cnt; i++)
        {
            char checkpoint[MAX_PATH_LEN];
            sprintf(checkpoint, "%s/%s", backend_dirs[i], MNTDIRNAME);
            int rc = check_stub_dir(checkpoint);
            if (rc == 0) {
                /* pass */
            } else {
                log_error("check_stub_dir failed");
                return -1;
            }

            int nwrite = write_data(conn_info->befiles[i].fd,
                                    msg->offset, msg->data, msg->count);
            if (nwrite != (int)msg->count)
            {
                log_error("> write %s failed: %d want, %d write",
                          conn_info->befiles[i].abs_file_name,
                          (int)msg->count, nwrite);
                return -1;
            }
        }
        msg->ack_code = 200;
        return send_response_message(events_poll, conn_info, msg, sizeof(msg_t));
    }
    else
    {
        log_error("recv corrupt message payload from peer %s:%u",
                  conn_info->peer_ip, conn_info->peer_port);
        return -1;
    }
}

static int __handle_download_data_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    uint8_t msg_buffer[MAX_MESSAGE_LEN];
    msg_t * new_msg = (msg_t *)msg_buffer;

    *new_msg = *msg;
    if (new_msg->count > MAX_MSG_DATA_LEN)
    {
        new_msg->count = MAX_MSG_DATA_LEN;
    }

    // 从第一个后端文件中读取数据
    int i;
    for (i = 0; i < backend_cnt; i++)
    {
        int nread = read_data(conn_info->befiles[i].fd,
                              new_msg->offset, new_msg->data, new_msg->count);
        if (nread > 0)
        {
            uint32_t totallen = sizeof(msg_t) + nread;
            new_msg->ack_code = 200;
            return send_response_message(events_poll, conn_info,
                                         new_msg, totallen);
        }
        else
        {
            log_warning("read %s failed: %d want, %d read, try next file",
                        conn_info->befiles[i].abs_file_name,
                        (int)new_msg->count, nread);
        }
    }

    log_error("read data failed: no backend file");
    return -1;
}

static int handle_common2(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    if (conn_info->use_proxy == 1)
    {
        return forward_message(events_poll, conn_info, msg);
    }
    else
    {
        if (conn_info->peer_id != 0)
        {
            if (msg->command == CMD_UPLOAD_DATA_REQ)
            {
                return __handle_upload_data_request(events_poll, conn_info, msg);
            }
            else
            {
                return __handle_download_data_request(events_poll, conn_info, msg);
            }
        }
        else
        {
            log_error("peer %s:%u has invalid peer_id 0",
                      conn_info->peer_ip, conn_info->peer_port);
            return -1;
        }
    }
}

static int __handle_upload_or_download_finish_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    if (msg->command == CMD_UPLOAD_FINISH_REQ) {
        int ret = close_and_check_md5(conn_info);
        if (ret == 0) {
            // log_info("%s uploading: 4/4", conn_info->befiles[0].md5);
            struct backend_file *f = &conn_info->befiles[0];
#if HAVE_CHECK_MD5
            int rc = savemd5(f->abs_file_name, f->md5);
#else
            int rc = 0;
#endif
            if (rc == 0) {
                msg->ack_code = 200;
            } else {
                msg->ack_code = 404;
                log_error("savemd5 failed: filename %s, md5 %s",
                          f->abs_file_name, f->md5);
            }
        } else {
            msg->ack_code = 404;
            log_error("close_and_check_md5 on sock_fd:%d failed", conn_info->sock_fd);
        }
    } else {
        int i;
        for (i = 0; i < backend_cnt; i++) {
            struct backend_file *f = &conn_info->befiles[i];
            backend_file_close_fd(f);
        }
        log_info("%s successfully downloaded", conn_info->befiles[0].abs_file_name);
        msg->ack_code = 200;
    }

    int len = sizeof(msg_t);
    return send_response_message(events_poll, conn_info, msg, len);
}

static int handle_upload_or_download_finish_request(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    if (msg->command == CMD_UPLOAD_FINISH_REQ)
    {
        // log_info("%s uploading: 3/4", conn_info->befiles[0].md5);
    }

    if (conn_info->use_proxy == 0)
    {
        if (conn_info->peer_id != 0)
        {
            return __handle_upload_or_download_finish_request(
                events_poll, conn_info, msg);
        }
        else
        {
            log_error("invalid peer_id %d: peer %s:%u, sock_fd %d\n",
                      conn_info->peer_id, conn_info->peer_ip, conn_info->peer_port,
                      conn_info->sock_fd);
            return -1;
        }
    }
    else
    {
        return forward_message(events_poll, conn_info, msg);
    }

    return 0;
}

static int handle_common3(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    int ret = forward_message(events_poll, conn_info, msg);
    if (ret < 0)
    {
        log_error("%s: forward_message failed", command_string(msg->command));
        return -1;
    }
    else
    {
        conn_info_t * next_conn_info = &conns_info[conn_info->next_sock_fd];
        next_conn_info->use_proxy = 0;
        next_conn_info->next_sock_fd = -1;

        conn_info->use_proxy = 0;
        conn_info->next_sock_fd = -1;

        return ret;
    }
}

static void advise_sequential(int fd)
{
    int rc1 = posix_fadvise(
        fd, 0/*偏移*/, 0/*文件的所有内容*/, POSIX_FADV_SEQUENTIAL);
    assert(rc1 == 0);
}

#ifdef CONFIG_WILLNEED
static void advise_willneed(int fd)
{
    int rc1 = posix_fadvise(
        fd, 0/*偏移*/, 0/*文件的所有内容*/, POSIX_FADV_WILLNEED);
    assert(rc1 == 0);
}
#endif

#ifdef CONFIG_DONTNEED
static void advise_dontneed(int fd)
{
    int rc1 = posix_fadvise(
        fd, 0/*偏移*/, 0/*文件的所有内容*/, POSIX_FADV_DONTNEED);
    assert(rc1 == 0);
}
#endif

static void advise_fitness(int fd)
{
#if defined(CONFIG_WILLNEED)
    // log_info("CONFIG_WILLNEED");
    advise_willneed(fd);
#elif defined(CONFIG_DONTNEED)
    // log_info("CONFIG_DONTNEED");
    advise_dontneed(fd);
#else
    // log_info("SEQUENTIAL");
    advise_sequential(fd);
#endif
}

/*
 * 处理客户端在一个连接内顺序下载文件的请求。
 *
 * 请求下载的文件名放在消息包的载荷 taskinfo_t.file_name 处。在同一个连接内，由
 * 客户端控制是否下载多个文件。接收到客户端的顺序下载请求后，打开文件，获取文件
 * 大小，发送出去，然后再发送内容。顺序下载文件的响应消息格式：
 *
 * (filesize) (data)
 * (8个字节) (...)
 *
 * 如果客户端连接一直可写，就一直往客户端连接发送数据。发送数据按照内存页的整数
 * 倍发送（8192），使用 sendfile() 避免用户空间的缓冲区拷贝，使用 posix_advise()
 * 告知内核文件将会以顺序的方式访问，同时，在接受客户端连接时，设置一个大的发送
 * 缓冲区。
 */
static int handle_seq_download_request(
    events_poll_t *e, conn_info_t *c, msg_t *m)
{
    char abs_file_name[4096];
    setup_abs_file_name(abs_file_name, sizeof(abs_file_name), m, backend_dirs[0]);

    // task_info_t *t = (task_info_t *)m->data;
    // log_info("message: %d bytes length, file_name: %s", m->length, t->file_name);

    int bfd = open(abs_file_name, O_RDWR);
    if (bfd >= 0) {
        struct stat s;
        int rc1 = fstat(bfd, &s);
        if (rc1 == 0) {
            advise_fitness(bfd); // 提前告知内核文件的访问方式
            c->is_sequence = 1;
            struct backend_file *f;
            f = &c->befiles[0];
            f->fd = bfd;
            f->sndstate = 0; // 可以发送顺序文件消息的长度
            f->filesize = s.st_size;
            f->fileleft = s.st_size;
            f->filedone = 0;
            snprintf(f->abs_file_name, sizeof(f->abs_file_name), "%s", abs_file_name);
            // 暂时停止接收消息事件，开始处理发送事件
            stop_monitoring_recv(e, c->sock_fd);
            start_monitoring_send(e, c->sock_fd);
            return 0;
        } else {
            log_error("get %s file size failed: %s",
                      abs_file_name, strerror(errno));
            return -1;
        }
    } else {
        log_error("open %s failed: %s",
                  abs_file_name, strerror(errno));
        return -1;
    }
}

/*
 * 消息包中的 filename 字段包含了 studyid/serial，根据 studyid 计算出路径名，然
 * 后遍历所有的可能的盘符路径目录，每一个目录路径名都获取一次文件列表。最后将获
 * 取的文件列表发送回客户端
 */
static int handle_get_file_list_request(
    events_poll_t *e, conn_info_t *c, msg_t *m)
{
    /* 分割 studyid/serial */
    char *studyid, *serial;
    task_info_t *t = (task_info_t *)(m->data);
    // log_info("filename: %s", t->file_name);
    split_serial(t->file_name, &studyid, &serial);

    int i, j;
    j = 0;
    for (i = 0; i < backend_cnt; i++) {
        if (!access(backend_dirs[i], F_OK)) {
            /* 只处理找到的第一个后端目录，因为其他的都是镜像备份 */
            const char *mountpoint = backend_dirs[i];
            char pathbuf[65536];
            int buflen = 65536;
            calcpath(mountpoint, studyid, serial, pathbuf, &buflen);
            // log_info("studyid: %s, serial: %s, buflen: %d", studyid, serial, buflen);

            /* 对每一个目录路径名，获取文件列表 */
            char dirsbuf[4096];
            const char **dirs = (const char **)&dirsbuf[0];
            int leftsize = buflen;
            char *next = pathbuf;
            while (leftsize > 0) {
                int n = strlen(next);
                dirs[j] = next;
                j = j + 1;
                leftsize = leftsize - n - 1;
                next = next + n + 1;
            }
            int k;
            for (k = 0; k < j; k++) {
                // log_info("dirpath: %s", dirs[k]);
            }

            struct file_list_result res;
            char *file_list_buffer = fill_many_dir_list(mountpoint, dirs, j, &res);
            if (file_list_buffer) {
                int rc = send_message(e, c, (uint8_t *)file_list_buffer, res.used_buflen);
                if (rc != res.used_buflen) {
                    log_error("send_message failed: sendlen %d", rc);
                } else {
                    // 发送缓冲区完成，继续处理
                }
                free(file_list_buffer);
                return rc;
            } else {
                log_error("fill_many_dir_list failed: file_list_buffer is NULL!");
                return -1;
            }
        } else {
            log_warning("skip %s: not exists", backend_dirs[i]);
        }
    }
    /* 如果后端目录都不存在了，就是严重的错误 */
    log_error("no backend directory!");
    return -1;
}

struct migoption
{
    uint8_t old_sgw_ip[64];
    uint16_t old_sgw_port;
    uint8_t new_sgw_ip[64];
    uint16_t new_sgw_port;

    uint8_t old_mds_ip[64];
    uint16_t old_mds_port;
    uint8_t new_mds_ip[64];
    uint16_t new_mds_port;
};

#define MAXCHILDS 10

struct childs
{
    pid_t pids[MAXCHILDS];
    int next;
};

static struct childs child = {{-1}, 0};

static void gencfg(struct migoption *opt)
{
    char *filepath = "/opt/storage_gateway/src/client2/config.ini";

    FILE *fp = fopen(filepath, "w");
    if (fp)
    {
        fprintf(fp,
                "[client]\n"
                "old_sgw_ipv4=%s\n"
                "old_sgw_port=%d\n"
                "new_sgw_ipv4=%s\n"
                "new_sgw_port=%d\n"
                "old_mds_ipv4=%s\n"
                "old_mds_port=%d\n"
                "new_mds_ipv4=%s\n"
                "new_mds_port=%d\n"
                "backend_directory=%s\n",
                opt->old_sgw_ip, ntohs(opt->old_sgw_port),
                opt->new_sgw_ip, ntohs(opt->new_sgw_port),
                opt->old_mds_ip, ntohs(opt->old_mds_port),
                opt->new_mds_ip, ntohs(opt->new_mds_port),
                backend_dirs[0]);
        // log_info("generate %s success", filepath);
        fclose(fp);
    }
    else
    {
        log_error("open %s failed", filepath);
    }
}

static void spawn_transfer_process(struct migoption *opt)
{
    // 启动传输进程
    if (child.next < MAXCHILDS)
    {
        gencfg(opt);

        pid_t pid = fork();
        if (pid == 0)
        {
            char dirname[128];
            char *ret = getcwd(dirname, 128);
            if (ret)
            {
                char progpath[256];
                snprintf(progpath, 256, "/opt/storage_gateway/src/client2/main.py");
                int ret2 = execlp("/usr/bin/python3", "/usr/bin/python3",
                                  progpath, "/opt/storage_gateway/src/client2/config.ini",
                                  (char *)NULL);
                int errno_cached = errno;
                if (ret2 == -1)
                {
                    printf("execute transfer process failed: %s\n",
                           strerror(errno_cached));
                    exit(EXIT_FAILURE);
                }
                else
                {
                    // 此时已执行了传输进程了，代码不会再执行到这里
                    exit(0);
                }
            }
            else
            {
                // 父进程应该能够监听信号，回收子进程。否则，就会产生过多的僵尸
                // 进程，占用系统资源。
                printf("getcwd failed\n");
                exit(EXIT_FAILURE);
            }
        }
        else
        {
            // 父进程
            child.pids[child.next] = pid;
            child.next = child.next + 1;
        }
    }
    else
    {
        log_error("failed to spawn process: too many childs");
    }
}

static int handle_migration_start(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    enum migration_state state = migstate_get();

    if (state == S_NORMAL)
    {
        // 开始进行数据迁移，启动数据传输进程。传输进程启动完成之后，这个函数就
        // 算处理完毕了。当传输完成之后，传输进程会发送传输完成的消息，这个消息
        // 的处理在消息处理循环中统一进行。
        migstate_set(S_MIGRATING);
        struct migoption *opt = (struct migoption *)msg->data;
        spawn_transfer_process(opt);
        return 0;
    }
    else if (state == S_MIGRATING)
    {
        log_debug("sgw in S_MIGRATING, ignore CMD_MIGRATION_START");
        return -1;
    }
    else // state == S_MIGRATED
    {
        // 数据迁移已完成，不再接受客户端连接
        log_debug("sgw in S_MIGRATED, stop serve client");
        close_tcp_conn(events_poll, conn_info->sock_fd);
        return 0;
    }
}

// XXX: 这里的处理会引起安全问题，未经验证的客户端可以发送迁移完成消息来结束 sgw
// 进程的执行
static int handle_migration_finished(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    (void) events_poll;
    (void) conn_info;

    int ack_code = msg->ack_code;
    if (ack_code == 200)
    {
        // log_info("migration finished! step down!");
        msg->ack_code = 200;
        migstate_set(S_MIGRATED);

        sleep(1);
        exit(0);  // 子进程会由 init 进程回收
        return 0; // 避免编译警告
    }
    else
    {
        log_error("migration failed!");
        return -1;
    }
}

static int handle_common4(
    events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    if (msg->command == CMD_MIGRATION_START_REQ)
    {
        return handle_migration_start(events_poll, conn_info, msg);
    }
    else if (msg->command == CMD_MIGRATION_FINISHED_REQ)
    {
        return handle_migration_finished(events_poll, conn_info, msg);
    }
    else
    {
        return 0;
    }
}

int deal_client_message(events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    // pr_msg_unpack(msg);

    switch (msg->command) {
        // 上传请求中处理一个文件的完整上传，包括开始上传请求，上传数据请求，上
        // 传完成请求
    case CMD_START_UPLOAD_REQ:
        return handle_common1(events_poll, conn_info, msg);
        break;

        // 下载请求仍然保留原样
    case CMD_START_DOWNLOAD_REQ:
    case CMD_DELETE_REQ:
        return handle_common1(events_poll, conn_info, msg);
        break;
    case CMD_DOWNLOAD_DATA_REQ:
        return handle_common2(events_poll, conn_info, msg);
        break;
    case CMD_DOWNLOAD_FINISH_REQ:
        return handle_upload_or_download_finish_request(
            events_poll, conn_info, msg);
        break;

        case CMD_START_UPLOAD_RSP:
        case CMD_UPLOAD_DATA_RSP:
        case CMD_START_DOWNLOAD_RSP:
        case CMD_DOWNLOAD_DATA_RSP:
        {
            return forward_message(events_poll, conn_info, msg);
        }
        case CMD_UPLOAD_FINISH_RSP:
        case CMD_DOWNLOAD_FINISH_RSP:
        case CMD_DELETE_RSP:
        {
            return handle_common3(events_poll, conn_info, msg);
            break;
        }
        case CMD_MIGRATION_START_REQ:
        case CMD_MIGRATION_STOP_REQ:
        case CMD_MIGRATION_FINISHED_REQ:
        case CMD_MIGRATION_CANCEL_REQ:
        {
            return handle_common4(events_poll, conn_info, msg);
        }
    case CMD_GET_FILE_LIST_REQ:
    {
        return handle_get_file_list_request(events_poll, conn_info, msg);
    }
    case CMD_SEQ_DOWNLOAD_REQ:
    {
        return handle_seq_download_request(events_poll, conn_info, msg);
    }
        default:
        {
            log_warning("recv command:0x%08X from client{%s:%d} ", msg->command, conn_info->peer_ip, conn_info->peer_port);
            return -1;
        }
    }
}

int deal_message(events_poll_t * events_poll, conn_info_t * conn_info, msg_t * msg)
{
    conn_info->peer_id = msg->src_id;
    conn_info->peer_type = msg->src_type;

    if (conn_info->peer_type == NODE_TYPE_CLNT ||
        conn_info->peer_type == NODE_TYPE_SGW)
    {
        enum migration_state state = migstate_get();

        if (state == S_NORMAL)
        {
            return deal_client_message(events_poll, conn_info, msg);
        }
        else if (state == S_MIGRATING || state == S_MIGRATED)
        {
            int c = msg->command;
            if (c == CMD_START_DOWNLOAD_REQ ||
                c == CMD_DOWNLOAD_DATA_REQ ||
                c == CMD_DOWNLOAD_FINISH_REQ ||
                c == CMD_MIGRATION_STOP_REQ ||
                c == CMD_MIGRATION_FINISHED_REQ)
            {
                return deal_client_message(events_poll, conn_info, msg);
            }
            else
            {
                log_warning("reject command:%s in state:%s",
                            command_string(c), state_string(state));
                return -1;
            }
        }
        else
        {
            // 当正在进行数据迁移时，拒绝客户端的所有消息，如果客户端正在进行数
            // 据传输，也一样关闭连接。如果允许客户端继续传输数据，代码的复杂度
            // 就会急剧上升。所以，目前如果正在进行数据迁移，就关闭所有的连接。
            log_warning("unknown state:%s: reject message",
                        state_string(state));
            return -1;
        }
    }
    else
    {
        log_warning("unknown peer type %d", conn_info->peer_type);
        return -1;
    }
}

static void run_events_loop(int thread_id);

void * worker_thread(void * argv)
{
    int thread_id = (int)(uint64_t)argv;

    init_mt_cntt(thread_id);    // 线程一启动就要初始化执行环境 !!!
    log_info("init_mt_cntt success");

    timer_sets[thread_id] = create_timer_set(MS_PER_TICK);
    if (timer_sets[thread_id] == NULL)
    {
        log_crit("create thread:%d timer set fail, exit !!! ", thread_id);
        return NULL;
    }
    log_info("create_timer_set success");

    epoll_fds[thread_id] = setup_events_poll(&events_polls[thread_id]);
    if (epoll_fds[thread_id] < 3)
    {
        log_crit("setup thread:%d events poll fail, exit!!!", thread_id);
        return NULL;
    }
    log_info("setup_events_poll success");

    if (add_to_events_poll(&events_polls[thread_id], pipefd[thread_id][0], EPOLLIN) != 1)
	{
		log_crit("add pipefd[%d][0] to events_poll fail ", thread_id);
		return NULL;
	}
    log_info("add_to_events_poll success");

    run_events_loop(thread_id);

    return NULL;
}

uint64_t accepts = 0UL;
uint64_t connections = 0UL;
uint64_t concurrents[MAX_WORKERS+1] = {0UL};

static inline int is_ipv4_addr(char * src)
{
    struct in_addr addr;

    if (inet_aton(src, &addr) == 0)
    {
        return 0;
    }
    else
    {
        return 1;
    }
}

static inline int __split_addr(char * src, char * ip, uint16_t * port)
{
    char * tmp = strchr(src, ':');
    if (tmp)
    {
        int len = tmp - src;
        memcpy(ip, src, len);
        ip[len] = '\0';

        int valid = is_ipv4_addr(ip);
        if (valid)
        {
            tmp++;
            uint32_t port1 = atoi(tmp);
            if (0 < port1 && port1 < 65536)
            {
                *port = port1;
                return 0;
            }
            else
            {
                printf("invalid port: %u\n", port1);
                return -1;
            }
        }
        else
        {
            printf("%s is not a valid IP address\n", ip);
            return -1;
        }
    }
    else
    {
        printf("invalid format: %s\n", src);
        return -1;
    }
}

// src 的形式类似于： 192.168.66.80:7788:0x80000003
static inline int split_addr(char * src, char * ip, uint16_t * port, uint32_t * id)
{
    int rc = __split_addr(src, ip, port);

    if (!rc)
    {
        char *tmp = strrchr(src, ':');
        if (tmp)
        {
            tmp++;
            *id = strtol(tmp, NULL, 16);
            return 0;
        }
        else
        {
            printf("invalid format: %s\n", src);
            return -1;
        }
    }
    else
    {
        printf("split ip and port from %s failed\n", src);
        return -1;
    }
}

static inline int split_back_end(char * src)
{
    char * prev = src;
    char * curr = NULL;
    int len = 0;

    if (prev == NULL)
    {
        return -1;
    }

    while (backend_cnt < MAX_BACK_END && prev[0] == '/')
    {
        curr = strchr(prev, ',');

        if (curr == NULL)
        {
            len = strlen(prev);
        }
        else
        {
            len = curr - prev;
        }

        if (len > MAX_NAME_LEN || len < 1)
        {
            backend_cnt = 0;
            return -1;
        }

        memcpy(backend_dirs[backend_cnt], prev, len);
        backend_dirs[backend_cnt][len] = 0;

        backend_cnt++;

        if (curr == NULL)
        {
            return backend_cnt;
        }

        prev = curr + 1;
    }

    return backend_cnt;
}

static void signal_init_daemon(void)
{
    int ret;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN; // ignore signal
    ret = sigaction(SIGHUP,  &sa, NULL); assert(ret == 0);
    ret = sigaction(SIGINT,  &sa, NULL); assert(ret == 0);
    ret = sigaction(SIGQUIT, &sa, NULL); assert(ret == 0);
    // 其他信号按照系统默认的行为处理，或者是终止程序，或者是产生 core 文件
}

// 以下列出参数的意义
//
// -r region_id
// -s system_id
// -g group_id
// -l local_ip:local_port:id
// -c connect_ip:connect_port
// -a asm_ip:asm_port:asm_id
// -b backend_dirs_list
// -w workers
// -d
//
// 这里还没有初始化日志模块，所以不能使用日志模块来打印日志到文件中。所以，使用
// printf() 来打印错误信息。

static int global_init(int argc, char ** argv)
{
    char * option = (char *)"r:s:g:l:c:a:b:w:p:d";
    int result = 0;
    int noerror = 1;
    int rc;

    while ((result = getopt(argc, argv, option)) > 0 && noerror)
    {
        if (result == 'r')
        {
            region_id = atoi(optarg);
        }
        else if (result == 's')
        {
            system_id = atoi(optarg);
        }
        else if (result == 'g')
        {
            group_id = atoi(optarg);
        }
        else if (result == 'l')
        {
            rc = split_addr(optarg, local_ip, &local_port, &local_id);
            if (rc == -1)
            {
                noerror = 0;
            }
        }
        else if (result == 'c')
        {
            rc = __split_addr(optarg, connect_ip, &connect_port);
            if (rc == -1)
            {
                noerror = 0;
            }
        }
        else if (result == 'a')
        {
            rc = split_addr(optarg, asm_ip, &asm_port, &asm_id);
            if (rc == -1)
            {
                noerror = 0;
            }
        }
        else if (result == 'b')
        {
            rc = split_back_end(optarg);
            if (rc == -1)
            {
                noerror = 0;
            }
        }
        else if (result == 'w')
        {
            workers = atoi(optarg);
        }
        else if (result == 'd')
        {
            int errno_cached;
            // nochdir=0: 切换到根目录；nochdir=1: 保留当前目录
            // noclose=0: 重定向 stdin stdout stderr 到 /dev/null
            // noclose=1: 不重定向 stdin stdout stderr 到 /dev/null
            rc = daemon(1/*nochdir*/, 1/*noclose*/);
            errno_cached = errno;
            if (rc == 0)
            {
                signal_init_daemon();
            }
            else // error: ret == -1
            {
                printf("daemon(1, 1) failed: %s\n", strerror(errno_cached));
                noerror = 0;
            }
        }
        else if (result == 'p')
        {
            snprintf(log_file, MAX_NAME_LEN, "%s", optarg);
            is_specified_log_file = 1;
        }
        else
        {
            printf("invalid option: %c\n", result);
            return -1;
        }
    }

    // getopt() return -1 if all command-line options have been parsed
    if (noerror == 1 && result == -1)
    {
        return 0;
    }
    else
    {
        return -1;
    }
}

struct asm_hb {
    uint32_t totallen;
    uint32_t command;
    uint8_t body[0];
};

static int setup_asm_hb(uint8_t *buffer, int buflen, conn_info_t * conn_info)
{
    (void) conn_info;

    struct asm_hb *m = (struct asm_hb *)buffer;
    m->command = htonl(0x00080001);
    int bodylen = snprintf(
        (char *)&m->body[0], buflen-sizeof(struct asm_hb),
        "{\"region_id\": %u, \"system_id\": %u, "
        "\"group_id\": %u, \"conn_state\": %lu, \"conn_dealed\": %lu, "
        "\"connect_ip\": \"%s\", \"connect_port\": %u}",
        region_id, system_id, group_id, connections, accepts,
        connect_ip, connect_port);
    m->totallen = htonl(8 + bodylen);
    return sizeof(struct asm_hb) + bodylen;
}

static int send_hb_to_asm(conn_info_t * c)
{
    uint8_t buffer[1024];
    memset(buffer, 0, sizeof(buffer));

    int bufflen = setup_asm_hb(buffer, sizeof(buffer), c);
    int sendlen = send_message(&events_polls[0], c, buffer, bufflen);
    if (sendlen == bufflen)
    {
        return 0;
    }
    else
    {
        log_error("send heartbeat message to asm fail");
        close_tcp_conn(&events_polls[0], c->sock_fd);
        return -1;
    }
}

static void asm_init_conn_info(int fd)
{
    conn_info_t * c = &conns_info[fd];
    c->sock_fd = fd;
    c->peer_type = NODE_TYPE_ASM;
    c->peer_id = asm_id;
    c->trans_id = 1;
    c->sequence = 1;
    c->thread_id = 0;
    log_info("> init asm connection %d context completed", fd);
}

// 在连接关闭时，也需要设置 to_asm_fd 为 -1
static int to_asm_fd = -1;

// 向 asm 发送心跳消息，如果和 asm 没有建立连接，首先建立连接。
static int on_hb_to_asm(void * timer)
{
    (void) timer;

    if (to_asm_fd == -1) {
        to_asm_fd = open_tcp_conn(
            &events_polls[0], asm_ip, asm_port,
            NULL/*local_ip*/, 0/*local_port*/, 0/*noblock*/);
        if (to_asm_fd >= 3) {
            asm_init_conn_info(to_asm_fd);
        }
    }

    if (to_asm_fd >= 3) {
        conn_info_t * c = &conns_info[to_asm_fd];
        if (c->sock_fd >= 3 && c->sock_fd == to_asm_fd) {
            int ret = send_hb_to_asm(c);
            if (ret == 0) {
                return 0;
            } else /* ret == -1 */ {
                to_asm_fd = -1;
                return -1;
            }
        } else {
            if (c->sock_fd == -1) {
                log_info("asm connection %d disconnect, try reconnect on next time",
                         to_asm_fd);
                to_asm_fd = -1;
                return -1;
            } else {
                log_warning("invalid sock_fd:%d", c->sock_fd);
                close_tcp_conn(&events_polls[0], c->sock_fd);
                return -1;
            }
        }
    } else /* to_asm_fd < 3 */ {
        if (to_asm_fd >= 0) {
            close(to_asm_fd);
            log_info("> closed invalid to_asm_fd:%d", to_asm_fd);
        } else /* to_asm_fd < 0 */ {
            // log_error("> connect to asm %s:%d failed", asm_ip, asm_port);
        }
        to_asm_fd = -1;
        return -1;
    }
}

static int asm_init_timer(void)
{
    user_timer_t t;
    memset(&t, 0, sizeof(user_timer_t));
    t.loop_cnt = 0xFFFFFFFF;
    t.hold_time = 1000; // 1s
    t.call_back = on_hb_to_asm;
    t.pv_param1 = t.pv_param2 = t.pv_param3 = t.pv_param4 = t.pv_param5 = NULL;
    int hb_timer_id = create_one_timer(timer_sets[0], &t);
    if (hb_timer_id > 0) {
        log_info("> create timer %d for asm success", hb_timer_id);
        return 0;
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "create heartbeat timer for to_asm_fd %d failed", to_asm_fd);
        printf("%s\n", msg);
        log_crit("%s", msg);

        sleep(1);
        return -1;
    }
}

static int asm_init(void)
{
    return asm_init_timer();
}

int listen_fd = -1;

static void usage(const char *progname)
{
    printf("VERSION: %s\n", VERSION);
    printf("please input like this: \r\n\r\n");
    printf("    %s -r 10001 -s 100001 -g 1 -l 192.168.120.70:7788:0x90000001 -c 212.77.88.99:55555 -a 192.168.120.80:8899:0x80000001 -b /back_end_ufs1,/back_end_ufs2,/back_end_ufs3 -w 4 -d \r\n", progname);
    printf("      -r : region id \r\n");
    printf("      -s : system id \r\n");
    printf("      -g : group id \r\n");
    printf("      -l : local address \r\n");
    printf("      -c : connect address \r\n");
    printf("      -a : asm server address \r\n");
    printf("      -b : back_end dirs list \r\n");
    printf("      -w : workers count \r\n");
    printf("      -d : daemon \r\n\r\n");
}

static void init0(int argc, char **argv)
{
    if (argc < 17)
    {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    int ret = global_init(argc, argv);
    assert(ret == 0);

    assert(region_id != 0);
    assert(system_id != 0);
    assert(group_id != 0);
    assert(local_port != 0);
    assert(connect_port != 0);
    assert(local_id != 0);
    assert(asm_port != 0);
    assert(asm_id != 0);
    assert(backend_cnt > 0);

    struct in_addr in_addr = {0};
    ret = inet_aton(local_ip, &in_addr);
    if (ret == 0)
    {
        printf("local_ip:%s illegal, ret:%d s_addr:%08X, exit !!!",
               local_ip, ret, in_addr.s_addr);
        exit(EXIT_FAILURE);
    }

    ret = inet_aton(asm_ip, &in_addr);
    if (ret == 0 || in_addr.s_addr == 0)
    {
        printf("asm_ip:%s illegal, ret:%d s_addr:%08X, exit !!!",
               asm_ip, ret, in_addr.s_addr);
        exit(EXIT_FAILURE);
    }
}

static void signal_init_base(void)
{
    int ret;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN; // ignore signal
    ret = sigaction(SIGPIPE, &sa, NULL); assert(ret == 0);
    // 守护进程还会进一步忽略一些信号

    sigset_t sset;
    ret = sigemptyset(&sset);                     assert(ret == 0);
    ret = sigaddset(&sset, SIGSEGV);              assert(ret == 0);
    ret = sigaddset(&sset, SIGBUS);               assert(ret == 0);
    ret = sigaddset(&sset, SIGABRT);              assert(ret == 0);
    ret = sigaddset(&sset, SIGILL);               assert(ret == 0);
    ret = sigprocmask(SIG_UNBLOCK, &sset, &sset); assert(ret == 0);
}

static void init1(char *progpath)
{
    signal_init_base();
    init_mt_cntt(0);
    int ret = init_log(basename(progpath), 0x400000);
    if (ret < 0)
    {
        printf("init_log fail, exit !!!");
        exit(EXIT_FAILURE);
    }
    else
    {
        log_info("-------- Storage Gateway start (version %s) --------", VERSION);
    }
}

static void init2(void)
{
    int ret = init_dispatch_tunnel();
    if (ret < 0) {
        printf("init_dispatch_tunnel fail, exit !!! \r\n");
        log_crit("init_dispatch_tunnel fail, exit !!! ");
        sleep(1);
        exit(EXIT_FAILURE);
    }
    log_info("init_dispatch_tunnel success");

    timer_sets[0] = create_timer_set(MS_PER_TICK);
    if (timer_sets[0] == NULL) {
        printf("create main thread timer set fail, exit !!! \r\n");
        log_crit("create main thread timer set fail, exit !!! ");
        sleep(1);
        exit(EXIT_FAILURE);
    }
    log_info("create_timer_set success");

    epoll_fds[0] = setup_events_poll(&events_polls[0]);
    if (epoll_fds[0] < 3) {
        printf("setup main events poll fail, exit !!! \r\n");
        log_crit("setup main events poll fail, exit !!! ");
        sleep(1);
        exit(EXIT_FAILURE);
    }
    log_info("setup_events_poll success");

    listen_fd = init_tcp_server(&events_polls[0], local_ip, local_port);
    if (listen_fd < 3) {
        printf("init local tcp server fail \r\n");
        log_crit("init local tcp server fail ");
        sleep(1);
        exit(EXIT_FAILURE);
    }
    log_info("init_tcp_server success");
}

static void init3(void)
{
    migstate_init();

    if (workers < 4)
    {
        workers = 4;
    }
    else if (workers > MAX_WORKERS)
    {
        workers = MAX_WORKERS;
    }
    else
    {
        // workers remains
    }

    int i;
    for (i = 1; i <= workers; i++)
    {
        pthread_t thread_id;
        int ret = pthread_create(
            &thread_id, NULL, worker_thread, (void *)(uint64_t)i);
        if (ret != 0)
        {
            printf("create thread:%d fail \r\n", i);
            log_crit("create thread:%d fail ", i);
            sleep(1);
            exit(EXIT_FAILURE);
        }
        log_info("worker%d: create success", i);
    }
}

static void init4(void)
{
    int ret = asm_init();
    if (ret < 0) {
        log_error("asm_init failed");
        exit(EXIT_FAILURE);
    } else {
        log_info("asm_init success");
    }
}

static void init_or_die(int argc, char **argv)
{
    init0(argc, argv);
    init1(argv[0]);
    init2();
    init3();
    init4();
}

static void run_events_loop(int thread_id)
{
    uint64_t curr = get_curr_time();
    uint64_t next = curr + MS_PER_TICK;
    while (1) {
        // 处理监听事件
        run_events_poll(&events_polls[thread_id], MS_PER_TICK);

        // 定时器任务
        // 可以处理时间往回跳变的情况
        curr = get_curr_time();
        if (next - 2*MS_PER_TICK <= curr && curr <= next + 2*MS_PER_TICK) {
            while (next < curr) {
                next = next + MS_PER_TICK;
                run_timer_set(timer_sets[thread_id]);
            }
        } else {
            next = curr + MS_PER_TICK;
            run_timer_set(timer_sets[thread_id]);
        }
    }
}

void init_and_run(int argc, char * argv[])
{
    init_or_die(argc, argv);
    run_events_loop(0); // main thread_id=0
}