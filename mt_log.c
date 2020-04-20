
#include "mt_log.h"
#include "public.h"

#define TRANSFER_LEN    0x10000 // 64KB

char log_file[MAX_NAME_LEN+1];
int is_specified_log_file;
int exit_log_thread = 0;

volatile uint64_t log_sequence = 0;

typedef struct headtail_ {
    volatile uint32_t head;
    volatile uint32_t tail;
} headtail_t;

typedef struct log_ring_
{
    uint32_t size;
    headtail_t prod;
    headtail_t cons;
    uint8_t data[0];
} log_ring_t;

static inline void update_tail(headtail_t *ht, uint32_t old_val, uint32_t new_val)
{
    while (ht->tail != old_val)
    {
        _mm_pause();
    }
    
    ht->tail = new_val;
}

static inline uint32_t get_free_entries(uint32_t head, uint32_t tail, uint32_t size)
{
    if (head >= tail)
    {
        return (size - head + tail);
    }
    else
    {
        return tail - head;
    }
}

uint32_t move_prod_head(log_ring_t * ring, uint32_t len, uint32_t * old_head, uint32_t * new_head, uint32_t * free_entries)
{
    uint32_t cons_tail = 0;
    int success = 0;

    while (success == 0)
    {
        *old_head = ring->prod.head;

        /* add rmb barrier to avoid load/load reorder in weak
         * memory model. It is noop on x86
         */
        compiler_barrier();

        cons_tail = ring->cons.tail;
        
        *free_entries = get_free_entries(*old_head, cons_tail, ring->size);

        /* check that we have enough room in ring */
        if (*free_entries < len)
        {
            return 0;
        }
        
        *new_head = (*old_head + len) % ring->size;
        
        success = __sync_bool_compare_and_swap(&(ring->prod.head), *old_head, *new_head);
    }
    
    return len;
}

static inline void copy_to_ring(uint8_t * dst, uint32_t start, uint32_t size, uint8_t * src, uint32_t len)
{
    uint32_t copy_len = size - start;
    
    if (copy_len > len)
    {
        copy_len = len;
    }
    
    memcpy(&dst[start], src, copy_len);
    
    if (copy_len < len)
    {
        memcpy(dst, &src[copy_len], len - copy_len);  
    }
}

static inline uint32_t do_enqueue(log_ring_t * ring, uint8_t * data, uint32_t len, uint32_t * free_space)
{
    uint32_t prod_head, prod_next;
    uint32_t free_entries;

    if (exit_log_thread != 0)
    {
        return 0;
    }
    
    len = move_prod_head(ring, len, &prod_head, &prod_next, &free_entries);
    if (len == 0)
    {
        return 0;
    }
    
    copy_to_ring(ring->data, prod_head, ring->size, data, len);
    
    compiler_barrier();

    update_tail(&(ring->prod), prod_head, prod_next);
    
    if (free_space != NULL)
    {
        *free_space = free_entries - len;
    }
    
    return len;
}

static inline uint32_t get_data_entries(uint32_t head, uint32_t tail, uint32_t size)
{
    if (head >= tail)
    {
        return head - tail;
    }
    else
    {
        return (size - tail + head);
    }
}

uint32_t move_cons_head(log_ring_t * ring, uint32_t len, uint32_t *old_head, uint32_t *new_head, uint32_t *entries)
{
    uint32_t prod_tail = 0;
    int success = 0;

    /* move cons.head atomically */
    while (success == 0)
    {
        *old_head = ring->cons.head;

        /* add rmb barrier to avoid load/load reorder in weak
         * memory model. It is noop on x86
         */
        compiler_barrier();

        prod_tail = ring->prod.tail;
        
        *entries = get_data_entries(prod_tail, *old_head, ring->size);

        /* Set the actual entries for dequeue */
        if (*entries < len)
        {
            return 0;
        }
        
        *new_head = (*old_head + len) % ring->size;
        
        success = __sync_bool_compare_and_swap(&(ring->cons.head), *old_head, *new_head);
    }
    
    return len;
}

static inline void copy_from_ring(uint8_t * src, uint32_t start, uint32_t size, uint8_t * dst, uint32_t len)
{
    uint32_t copy_len = size - start;
    
    if (copy_len > len)
    {
        copy_len = len;
    }
    
    memcpy(dst, &src[start], copy_len);
    
    if (copy_len < len)
    {
        memcpy(&dst[copy_len], src, len - copy_len);  
    }
}

static inline uint32_t do_dequeue(log_ring_t * ring, uint8_t * data, uint32_t len, uint32_t * available)
{
    uint32_t cons_head, cons_next;
    uint32_t entries;

    if (exit_log_thread != 0)
    {
        return 0;
    }
    
    len = move_cons_head(ring, len, &cons_head, &cons_next, &entries);
    if (len == 0)
    {
        return 0;
    }
    
    copy_from_ring(ring->data, cons_head, ring->size, data, len);
    
    compiler_barrier();

    update_tail(&ring->cons, cons_head, cons_next);
    
    if (available != NULL)
    {
        *available = entries - len;
    }
    
    return len;
}


int log_level = LOG_DEBUG;
log_ring_t * log_ring = NULL;

volatile uint64_t enqueue_error = 0;


#define MAX_TIME_STRING  27  // strlen("2015-10-23 09:15:59.737940") + 1

static inline char * get_time_string(char * buffer, int buffer_len)
{
    struct timeval tv;
    struct tm tm;
    
    if (buffer_len < MAX_TIME_STRING)
    {
        return NULL;
    }
    
    gettimeofday(&tv, NULL);
    localtime_r(&tv.tv_sec, &tm);
    
    tm.tm_year += 1900;
    tm.tm_mon += 1;
    
    snprintf(buffer, buffer_len, "%4d-%02d-%02d %02d:%02d:%02d.%06d",
             tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, (uint32_t)(tv.tv_usec));
    
    return buffer;
}

#define MAX_LOG_LINE    8191

extern int get_thread_id(void);

static char *g_level[8] = {
    [LOG_EMERG] = "EMERG",
    [LOG_ALERT] = "ALTER",
    [LOG_CRIT] = "CRIT",
    [LOG_ERROR] = "ERROR",
    [LOG_WARNING] = "WARNING",
    [LOG_NOTICE] = "NOTICE",
    [LOG_INFO] = "INFO",
    [LOG_DEBUG] = "DEBUG"
};

int write_log(int level, const char *file_name, const char *func_name, const int line_num, const char *format, ...)
{
    char time_string[MAX_TIME_STRING];
    int thread_id = 0;
    int curr_index = 0;
    char log_buffer[MAX_LOG_LINE+1];
    va_list ap;
    int len = 0;
    pid_t log_pid = getpid();
    
    if (level > log_level)
    {
        level = log_level;
    }
    else if (level < LOG_EMERG)
    {
        level = LOG_EMERG;
    }
    else
    {
        // level = level
    }
    
    get_time_string(time_string, MAX_TIME_STRING);
    thread_id = get_thread_id();
#if 0
    char * base_name = basename((char *)file_name);
    curr_index += snprintf(&log_buffer[curr_index], MAX_LOG_LINE-curr_index,
                           "<%s>[%d][T%d][%s][%s:%d:%s]: ",
                           g_level[level], (int)log_pid, thread_id,
                           time_string, base_name, line_num, func_name);
#else
    (void) file_name;
    (void) func_name;
    (void) line_num;
    curr_index += snprintf(
        &log_buffer[curr_index], MAX_LOG_LINE - curr_index,
        "[%s][%d.T%d]<%s>: ",
        time_string, (int)log_pid, thread_id, g_level[level]);
#endif
    
    va_start(ap, format);
    curr_index += vsnprintf(&log_buffer[curr_index], MAX_LOG_LINE-curr_index-2, format, ap);
    va_end(ap);
    
    log_buffer[curr_index++] = '\n';
    log_buffer[curr_index] = 0;
    
    len = do_enqueue(log_ring, (uint8_t *)log_buffer, curr_index, NULL);
    
    if (len != curr_index)
    {
        return 0;
    }
    else
    {
        return curr_index;
    }
}

void * log_thread(void * arg)
{
    (void) arg;

    uint8_t transfer[TRANSFER_LEN];
    int log_fd = -1;
    uint32_t len = 0;
    struct stat curr_stat;
    
    memset(transfer, 0, TRANSFER_LEN);
    
    log_fd = open(log_file, O_CREAT|O_RDWR|O_APPEND, 0644);
    if (log_fd < 0)
    {
        printf("open %s fail, log_thread exit !!! \r\n", log_file);
        return NULL;
    }
    
    while (!exit_log_thread)
    {
        if (log_fd < 0 || stat(log_file, &curr_stat) != 0)
        {
            if (log_fd > 0)
            {
                close(log_fd);
                printf("> closed log_fd:%d", log_fd);
                log_fd = -1;
            }
            
            usleep(5000);
            
            log_fd = open(log_file, O_CREAT|O_RDWR|O_APPEND, 0644);
            if (log_fd < 0)
            {
                continue;
            }
        }
        
        len = get_data_entries(log_ring->prod.tail, log_ring->cons.head, log_ring->size);
        if (len == 0)
        {
            usleep(5000);
            continue;
        }
        else if (len > TRANSFER_LEN)
        {
            len = TRANSFER_LEN;
        }
        
        len = do_dequeue(log_ring, transfer, len, NULL);
        if (write(log_fd, transfer, len) != len)
        {
            printf("transfer log fail, log:%s \r\n", transfer);
        }
        transfer[len] = 0;
        
        // printf("\r\n len:%d log_ring->cons{%u,%u} transfer:\r\n%s\r\n",
        //        len, log_ring->cons.head, log_ring->cons.tail, transfer);
    }
    
    close(log_fd);
    printf("> closed log_fd:%d", log_fd);
    
    return NULL;
}

int init_log(char * app_name, int buffer_size)
{
    if (!is_specified_log_file) {
        snprintf(log_file, sizeof(log_file), "/var/log/%s.log", app_name);
    }

    if (buffer_size < 0x100000)
    {
        buffer_size = 0x100000; // 小于一兆按一兆算
    }
    else if (buffer_size > 0x1000000)
    {
        buffer_size = 0x1000000; // 大于十六兆按十六兆算
    }
    else
    {
        // 在一兆到十六兆范围内，保持不变
    }

    int ringlen = sizeof(log_ring_t) + buffer_size;
    log_ring = calloc(1, ringlen);
    if (log_ring == NULL)
    {
        printf("init_log: calloc %d bytes failed\n", ringlen);
        return -1;
    }
    log_ring->size = buffer_size;

    exit_log_thread = 0;
    pthread_t thread_id;
    int ret = pthread_create(&thread_id, NULL, &log_thread, NULL);
    if (ret != 0)
    {
        printf("init_log: create log_thread failed\n");
        return -1;
    }
    else
    {
        printf("logs go to %s\n", log_file);
        return 1;
    }
}
