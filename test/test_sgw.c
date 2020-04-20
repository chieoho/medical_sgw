// sgw unit test

#define _GNU_SOURCE

#include <assert.h>

#include "mt_log.h"
#include "public.h"
#include "timer_set.h"
#include "events_poll.h"
#include "conn_mgmt.h"

#undef log_debug
#undef log_info
#undef log_notice
#undef log_warning
#undef log_error
#undef log_crit
#undef log_alert
#undef log_emerg

#define log_debug(format, args...)      printf(format "\n", ##args)
#define log_info(format, args...)       printf(format "\n", ##args)
#define log_notice(format, args...)     printf(format "\n", ##args)
#define log_warning(format, args...)    printf(format "\n", ##args)
#define log_error(format, args...)      printf(format "\n", ##args)
#define log_crit(format, args...)       printf(format "\n", ##args)
#define log_alert(format, args...)      printf(format "\n", ##args)
#define log_emerg(format, args...)      printf(format "\n", ##args)

#include "../pathops.c"
#include "../handler.c"
#include "../mt_log.c"
#include "../timer_set.c"
#include "../events_poll.c"
#include "../conn_mgmt.c"
#include "../cmdstr.c"

void test_spawn_transfer_process(void)
{
    printf("test spawn_transfer_process: ");

    struct migoption opt;
    spawn_transfer_process(&opt);

    printf("done\n");
}

static void test___split_addr(void)
{
    printf("---- test __split_addr ----\n");

    int rc;
    char ip1[20];
    uint16_t port1;

    char *valid1 = "192.168.66.30:7788";

    rc = __split_addr(valid1, ip1, &port1);
    assert(rc == 0);
    printf("format: %s, ip: %s, port: %u\n", valid1, ip1, port1);

    char *invalid1 = "333.333.333.333:7788";
    rc = __split_addr(invalid1, ip1, &port1);
    assert(rc == -1);

    char *invalid2 = "192.168.33.20:888888";
    rc = __split_addr(invalid2, ip1, &port1);
    assert(rc == -1);
}

static void test_split_addr(void)
{
    printf("---- test split_addr ----\n");

    int rc;

    char ip1[20];
    uint16_t port1;
    uint32_t id1;

    char *valid1 = "192.168.66.30:7788:0x80000003";
    rc = split_addr(valid1, ip1, &port1, &id1);
    assert(rc == 0);
    printf("format: %s, ip: %s, port: %u, id: 0x%x\n", valid1, ip1, port1, id1);

    char *invalid1 = "192.168.66.30:7777778888888:0x8000003";
    rc = split_addr(invalid1, ip1, &port1, &id1);
    assert(rc == -1);
}

static void test_create_ring(void)
{
    ring_t *ring;

    ring = create_ring(MAX_RING_DATA_LEN);
    assert(ring);

    printf("test: create_ring: success\n");
    destroy_ring(ring);
}

static void test_clear_ring(void)
{
    ring_t *ring1 = create_ring(MAX_RING_DATA_LEN);
    assert(ring1);
    ring_t *ring2 = clear_ring(ring1);
    assert(ring2 && ring2->read == 0 && ring2->write == 0);

    printf("test clear_ring: success\n");
    destroy_ring(ring1);
}

static void test_ring_ops(void)
{
    // CACHE_LINE_SIZE * 2 = 128
    uint32_t size = MIN_RING_SIZE;
    ring_t *ring = create_ring(size);
    assert(ring);

    uint32_t free1 = get_ring_free_size(ring);
    assert(free1 == size);

    char data1[] = "test_get_ring_free_size";
    int data1len = strlen(data1) + 1;
    int writelen1 = write_ring(ring, (uint8_t *)data1, data1len);
    assert(writelen1 == data1len);

    uint32_t free2 = get_ring_free_size(ring);
    uint32_t rlen2 = get_ring_data_size(ring);
    assert(rlen2 + free2 == size);

    char data2[] = "data2";
    int data2len = strlen(data2) + 1;
    int writelen2 = write_ring(ring, (uint8_t *)data2, data2len);
    assert(data2len == writelen2);

    uint32_t free3 = get_ring_free_size(ring);
    uint32_t rlen3 = get_ring_data_size(ring);
    assert(rlen3 + free3 == size);

    printf("test get_ring_free_size: success\n");
}

static void test_execute_command(void)
{
    int ret;

    ret = execute_command("stat execute_command.c");
    assert(ret == -1);

    ret = execute_command("df");
    assert(ret == 0);

    printf("test execute_command: success\n");
}

void test_open_path(void)
{
    char path1[] = "aaaaaa/bbbbbb/123456";
    int rc1 = open_path(path1);
    assert(rc1 >= 0);
    close(rc1);

    char path2[] = "bbb////bbb/123456";
    int rc2 = open_path(path2);
    assert(rc2 >= 0);
    close(rc2);

    char path3[] = "bbb/xxx/123456";
    int rc3 = open_path(path3);
    assert(rc3 >= 0);
    close(rc3);

    char path4[] = "bbb/xxx";
    int rc4 = open_path(path4);
    assert(rc4 == -1);

    char path5[] = "/tmp/aaa/123456";
    int rc5 = open_path(path5);
    assert(rc5 >= 0);
    close(rc5);

    char path6[] = "/tmp/aaa";
    int rc6 = open_path(path6);
    assert(rc6 == -1);

    char path7[] = "/tmp////aaa////bbb/123456";
    int rc7 = open_path(path7);
    assert(rc7 >= 0);
    close(rc7);
}

int main(void)
{
    test___split_addr();
    test_split_addr();
    test_create_ring();
    test_clear_ring();
    test_ring_ops();
    test_open_path();
    test_execute_command();
    test_spawn_transfer_process();
    return 0;
}
