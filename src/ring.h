
// ring.h

#ifndef RING_H
#define RING_H

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>

#define CACHE_LINE_SIZE 64
#define MAX_RING_SIZE   (64*1024*1024) // 64MB
#define MIN_RING_SIZE   (4*1024)       // 4KB

typedef struct ring_
{
    uint32_t size;  // 缓冲区大小
    uint32_t flags; // 是否有效的标志
    uint32_t read;  // 下一个可读取的位置
    uint32_t write; // 下一个可写入的位置
    uint32_t len;   // 缓冲区写入的长度
    uint8_t data[0];// 指向真正的数据
} ring_t;


static inline ring_t * create_ring(uint32_t suggest_size)
{
    uint32_t bias_size = suggest_size + CACHE_LINE_SIZE - 1;
    uint32_t real_size = (bias_size / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;
    if      (real_size > MAX_RING_SIZE) { real_size = MAX_RING_SIZE; }
    else if (real_size < MIN_RING_SIZE) { real_size = MIN_RING_SIZE; }
    else {} // real_size remains

    ring_t * ring = (ring_t *)calloc(1, sizeof(ring_t) + real_size);
    if (ring != NULL)
    {
        ring->size = real_size;
        ring->len = 0;
        ring->read = 0;
        ring->write = 0;
        return ring;
    }
    else
    {
        return NULL;
    }
}

static inline void destroy_ring(ring_t * ring)
{
    if (ring != NULL)
    {
        free(ring);
    }
}

static inline ring_t * clear_ring(ring_t * ring)
{
    if (ring != NULL)
    {
        ring->read = 0;
        ring->write = 0;
        ring->len = 0;
        return ring;
    }
    else
    {
        return NULL;
    }
}

static inline int is_empty_ring(ring_t * ring)
{
    if (ring != NULL && ring->len == 0)
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

static inline int is_full_ring(ring_t * ring)
{
    assert(ring != NULL);
    if (ring->size - ring->len < CACHE_LINE_SIZE)
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

static inline uint32_t get_ring_data_size(ring_t * ring)
{
    if (ring != NULL)
    {
        assert(ring->size >= ring->len);
        return ring->len;
    }
    else // ring == NULL
    {
        return 0;
    }
}

static uint32_t ring_size_transition(uint32_t size)
{
    if (size < CACHE_LINE_SIZE)
    {
        return 0;
    }
    else
    {
        return size;
    }
}

static inline uint32_t get_ring_free_size(ring_t * ring)
{
    if (ring != NULL)
    {
        assert(ring->size >= ring->len);
        uint32_t free_size = ring->size - ring->len;
        return ring_size_transition(free_size);
    }
    else // ring == NULL
    {
        return 0;
    }
}

// 将缓冲区内容拷贝到第 0 个字节处
static inline void adjust_ring(ring_t * ring)
{
    uint32_t data_size = get_ring_data_size(ring);
    if (data_size > 0)
    {
        if (ring->read > 0)
        {
            memmove(ring->data, &ring->data[ring->read], data_size);
            ring->read = 0;
            ring->write = data_size;
        }
        else // ring->read == 0
        {
            // 当前缓冲读下标是移动后想要的样子，不用处理
        }
    }
    else // data_size == 0
    {
        ring->read = 0;
        ring->write = 0;
        ring->len = 0;
    }
}

static inline int write_ring(ring_t * ring, uint8_t * data, uint32_t len)
{
    uint32_t free_size = get_ring_free_size(ring);
    if (free_size >= len)
    {
        uint32_t copy_len = ring->size - ring->write;
        if (copy_len > len)
        {
            memcpy(&ring->data[ring->write], data, len);
            ring->write = ring->write + len;
        }
        else if (copy_len < len)
        {
            memcpy(&ring->data[ring->write], data, copy_len);
            memcpy(&ring->data[0], &data[copy_len], len - copy_len);
            ring->write = len - copy_len;
        }
        else // copy_len == len
        {
            memcpy(&ring->data[ring->write], data, copy_len);
            ring->write = 0;
        }
        ring->len = ring->len + len;
        return len;
    }
    else
    {
        return 0;
    }
}

#endif
