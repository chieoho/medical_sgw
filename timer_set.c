 
// timer_set.c

#include <assert.h>

#include "timer_set.h"

typedef struct _list_head
{
    uint32_t head;
    uint32_t tail;
    uint32_t cnt;
} list_head_t;

typedef struct _timer_node
{
    user_timer_t user_timer;
    uint32_t flags;
    uint32_t next;
    uint64_t hold_jiffies;
    uint64_t exp_jiffies;
    list_head_t * p_list;
} timer_node_t;

struct timer_set
{
    uint32_t flags;
    uint32_t base_ticks;
    uint64_t jiffies;
    uint32_t curr_lists[8];
    list_head_t timer_lists[8][256];
    
    uint32_t free_start;
    uint32_t free_cnt;
    timer_node_t timer_nodes[MAX_TIMER_NODE_CNT];
};

uint64_t get_curr_jiffies(timer_set_t * p_timer_set)
{
    if (p_timer_set != NULL) {
        return p_timer_set->jiffies;
    } else {
        return 0;
    }
}


static inline uint32_t get_timer_index(
    timer_set_t *p_timer_set, timer_node_t *p_timer_node)
{
    return (uint32_t)(p_timer_node - p_timer_set->timer_nodes);
}

static inline timer_node_t *get_timer_node(
    timer_set_t *p_timer_set, uint32_t index)
{
    if (index >= MAX_TIMER_NODE_CNT) {
        return NULL;
    } else {
        return &(p_timer_set->timer_nodes[index]);
    }
}

user_timer_t *get_user_timer(timer_set_t *p_timer_set, int timer_id)
{
    if (timer_id <= 0 || timer_id >= MAX_TIMER_NODE_CNT) {
        return NULL;
    } else {
        return &(p_timer_set->timer_nodes[timer_id].user_timer);
    }
}

static inline user_timer_t *clear_user_timer(user_timer_t *p_user_timer)
{
    p_user_timer->timer_id = 0;
    p_user_timer->loop_cnt = 0;
    p_user_timer->hold_time = 0;
    p_user_timer->call_back = NULL;
    p_user_timer->pv_param1 = NULL;
    p_user_timer->pv_param2 = NULL;
    p_user_timer->pv_param3 = NULL;
    p_user_timer->pv_param4 = NULL;
    p_user_timer->pv_param5 = NULL;
    
    return p_user_timer;
}

static inline void clear_timer_node(timer_node_t *p_timer_node)
{
    clear_user_timer(&(p_timer_node->user_timer));
    p_timer_node->flags = 0;
    p_timer_node->next = MAX_TIMER_NODE_CNT;
    p_timer_node->hold_jiffies = 0;
    p_timer_node->exp_jiffies = 0;
    p_timer_node->p_list = NULL;
}

static inline void init_list_head(list_head_t *p_list)
{
    p_list->head = MAX_TIMER_NODE_CNT;
    p_list->tail = MAX_TIMER_NODE_CNT;
    p_list->cnt = 0;
}

static timer_set_t *init_timer_set(timer_set_t *ts, uint16_t base_ticks)
{
    ts->flags = 1;
    ts->base_ticks = base_ticks;
    ts->jiffies = 0;

    int i, j;
    for (i = 0; i < 8; i++) {
        for (j = 0; j < 256; j++) {
            init_list_head(&ts->timer_lists[i][j]);
        }
        ts->curr_lists[i] = 0;
    }
    
    ts->free_start = 1;
    ts->free_cnt = MAX_TIMER_NODE_CNT - 1;
    clear_timer_node(&ts->timer_nodes[0]);
    int k;
    for (k = 0; k < MAX_TIMER_NODE_CNT; k++) {
        clear_timer_node(&ts->timer_nodes[k]);
        ts->timer_nodes[k].next = k + 1;
    }
    
    return ts;
}

timer_set_t *create_timer_set(uint16_t base_ticks)
{
    timer_set_t * p_timer_set = NULL;
    
    if (base_ticks < 10 || base_ticks > 1000) {
        printf("%s --> %s --> L%d : base_ticks:%u \r\n",
               __FILE__, __FUNCTION__, __LINE__, base_ticks);
        return NULL;
    }
    
    p_timer_set = (timer_set_t *)malloc(sizeof(timer_set_t));
    if (p_timer_set == NULL) {
        printf("%s --> %s --> L%d : create timer set fail \r\n",
               __FILE__, __FUNCTION__, __LINE__);
        return NULL;
    }
    
    return init_timer_set(p_timer_set, base_ticks);
}

void destroy_timer_set(timer_set_t *p_timer_set)
{
    if (p_timer_set != NULL) {
        free(p_timer_set);
    }
}

static inline timer_node_t *alloc_timer_node(timer_set_t *ts)
{
    if (ts->free_cnt > 0) {
        int id = ts->free_start;
        if (id < MAX_TIMER_NODE_CNT) {
            timer_node_t *node = &ts->timer_nodes[id];
            ts->free_start = node->next;
            ts->free_cnt = ts->free_cnt - 1;
            clear_timer_node(node);
            node->flags = 1;
            return node;
        } else {
            printf("%s:%d:%s: next free timer node index %d is invalid!\n",
                   __FILE__, __LINE__, __FUNCTION__, id);
            return NULL;
        }
    } else {
        printf("%s:%d:%s: run out of timer node!\n",
               __FILE__, __LINE__, __FUNCTION__);
        return NULL;
    }
}

static inline int free_timer_node(
    timer_set_t *p_timer_set, timer_node_t *p_timer_node)
{
    uint32_t index = get_timer_index(p_timer_set, p_timer_node);

    if (index >= MAX_TIMER_NODE_CNT) {
        return -1;
    }

    if (p_timer_node->flags == 0) {
        return 0;
    }

    clear_timer_node(p_timer_node);
    
    p_timer_node->next = p_timer_set->free_start;
    p_timer_set->free_start = index;
    p_timer_set->free_cnt++;

    return 0;
}

static inline int get_timer_level(timer_set_t *ts, uint64_t exp_jiffies)
{
    uint64_t remain_jiffies;
    if (exp_jiffies > ts->jiffies) {
        remain_jiffies = exp_jiffies - ts->jiffies;
    } else {
        remain_jiffies = 0UL;
    }

    // 每一级的当前链都是下一次执行 run_timer_set() 时才有可能处理

    if      (remain_jiffies <=              0x100UL) {return 0;}
    else if (remain_jiffies <=            0x10000UL) {return 1;}
    else if (remain_jiffies <=          0x1000000UL) {return 2;}
    else if (remain_jiffies <=        0x100000000UL) {return 3;}
    else if (remain_jiffies <=      0x10000000000UL) {return 4;}
    else if (remain_jiffies <=    0x1000000000000UL) {return 5;}
    else if (remain_jiffies <=  0x100000000000000UL) {return 6;}
    else if (remain_jiffies <= 0xFFFFFFFFFFFFFFFFUL) {return 7;}
    else                                             {return 8;}
}

static inline int get_timer_list_index(
    timer_set_t *ts, int level, uint64_t exp_jiffies)
{
    // 每一级的当前链都是下一次执行 run_timer_set() 时才有可能处理

    uint64_t remain_jiffies;
    if (exp_jiffies > ts->jiffies) {
        remain_jiffies = exp_jiffies - ts->jiffies;
    } else {
        remain_jiffies = 0UL;
    }

    int offset = ((remain_jiffies >> (level * 8)) & 0x000001FF) - 1;
    if (offset < 0) {
        offset = 0;
    } else {
        // offset remains
    }

    return (ts->curr_lists[level] + offset) & 0x000000FF;
}

static inline list_head_t *get_timer_list_head(
    timer_set_t *ts, timer_node_t *node)
{
    // 每一级的当前链都是下一次执行 run_timer_set() 时才有可能处理

    int exp_jiffies = node->exp_jiffies;
    int level = get_timer_level(ts, exp_jiffies);
    if (0 <= level && level < 8) {
        int index = get_timer_list_index(ts, level, exp_jiffies);
        return &ts->timer_lists[level][index];
    } else {
        printf("%s:%d:%s: no level %d\n",
               __FILE__, __LINE__, __FUNCTION__, level);
        return NULL;
    }
}

static inline int add_timer_node_to_list(
    timer_set_t *ts, list_head_t *target, timer_node_t *node)
{
    uint32_t index = get_timer_index(ts, node);
    if (index < MAX_TIMER_NODE_CNT && target->cnt < MAX_TIMER_NODE_CNT) {
        node->p_list = target;
        target->cnt += 1;
        if (target->tail < MAX_TIMER_NODE_CNT) {
            ts->timer_nodes[target->tail].next = index;
            target->tail = index;
        } else /* target->tail >= MAX_TIMER_NODE_CNT */ {
            // 第一次往定时器列表中插入定时器
            target->head = index;
            target->tail = index;
        }
        return (int)(int32_t)index; // 0~100000 的无符号转换不会溢出
    } else {
        if (index >= MAX_TIMER_NODE_CNT) {
            printf("%s:%d:%s: invalid timer %d\n",
                   __FILE__, __LINE__, __FUNCTION__, index);
        } else /* curr->cnt >= MAX_TIMER_NODE_CNT */ {
            printf("%s:%d:%s: timer reach limits %d\n",
                   __FILE__, __LINE__, __FUNCTION__, MAX_TIMER_NODE_CNT);
        }
        return -1;
    }
}

static inline int insert_timer_node(timer_set_t *ts, timer_node_t *node)
{
    if (node->p_list == NULL) {
        // 定时器没有在工作链表中，找到目标链表，然后加入
        list_head_t *target = get_timer_list_head(ts, node);
        if (target != NULL) {
            return add_timer_node_to_list(ts, target, node);
        } else /* target == NULL */ {
            printf("%s:%d:%s: get timer list head failed\n",
                   __FILE__, __LINE__, __FUNCTION__);
            return -1;
        }
    } else /* node->p_list != NULL */ {
        // 已经在工作链表中
        return get_timer_index(ts, node);
    }
}

int create_one_timer(timer_set_t *set, user_timer_t *timer)
{
    assert(set);
    assert(timer);
    assert(timer->call_back);

    if (set->flags == 1) {
        timer_node_t *node = alloc_timer_node(set);
        if (node) {
            timer->timer_id = get_timer_index(set, node);
            node->user_timer = *timer;
            node->hold_jiffies = timer->hold_time / set->base_ticks;
            node->exp_jiffies = set->jiffies + node->hold_jiffies;
            int rc = insert_timer_node(set, node);
            if (rc < 0) {
                printf("insert_timer_node() fail, timer_id:%d\n",
                       node->user_timer.timer_id);
                free_timer_node(set, node);
                return -1;
            } else {
                return timer->timer_id;
            }
        } else {
            printf("alloc_timer_node fail\n");
            return -1;
        }
    } else {
        printf("timer set not init\n");
        return -1;
    }
}


static timer_node_t *delete_one_timer_from_list(
    timer_set_t *p_timer_set, timer_node_t *p_timer_node)
{
    uint32_t timer_index = get_timer_index(p_timer_set, p_timer_node);
    list_head_t * p_list = NULL;
    uint32_t prev_index = 0;
    
    if (timer_index >= MAX_TIMER_NODE_CNT || p_timer_node->p_list == NULL) {
        printf("timer_id:%u no in work list ", timer_index);
        return NULL;
    }
    
    p_list = p_timer_node->p_list;
    
    if (p_list->head == timer_index && p_list->tail == timer_index) {
        p_list->head = MAX_TIMER_NODE_CNT;
        p_list->tail = MAX_TIMER_NODE_CNT;
    } else if (p_list->head == timer_index) {
        p_list->head = p_timer_node->next;
    } else if (p_list->tail == timer_index) {
        prev_index = p_list->head;
        while (p_timer_set->timer_nodes[prev_index].next != timer_index) {
            prev_index = p_timer_set->timer_nodes[prev_index].next;
        }
        p_list->tail = prev_index;
        p_timer_set->timer_nodes[prev_index].next = MAX_TIMER_NODE_CNT;
    } else {
        prev_index = p_list->head;
        while (p_timer_set->timer_nodes[prev_index].next != timer_index) {
            prev_index = p_timer_set->timer_nodes[prev_index].next;
        }
        p_timer_set->timer_nodes[prev_index].next = p_timer_node->next;
    }
    
    p_list->cnt--;
    
    p_timer_node->next = MAX_TIMER_NODE_CNT;
    p_timer_node->p_list = NULL;
    
    return p_timer_node;
}

static inline timer_node_t *delete_first_timer_from_list(
    timer_set_t *ts, list_head_t *curr)
{
    if (curr->cnt > 0 && curr->head < MAX_TIMER_NODE_CNT) {
        timer_node_t *node = &ts->timer_nodes[curr->head];
        uint32_t index = get_timer_index(ts, node);
        if (index == curr->tail) {
            curr->head = MAX_TIMER_NODE_CNT;
            curr->tail = MAX_TIMER_NODE_CNT;
        } else {
            curr->head = node->next;
        }
        node->next = MAX_TIMER_NODE_CNT;
        node->p_list = NULL;
        curr->cnt -= 1;
        return node;
    } else /* curr->cnt == 0 || curr->head >= MAX_TIMER_NODE_CNT */ {
        printf("%s:%d:%s: no timer node in timer set %p current list %p\n",
               __FILE__, __LINE__, __FUNCTION__, ts, curr);
        return NULL;
    }
}

int destroy_one_timer(timer_set_t *p_timer_set, int timer_id)
{
    timer_node_t * p_timer_node = NULL;
    
    // printf("timer_id:%d ", timer_id);
    
    if (p_timer_set == NULL || timer_id < 0 || timer_id >= MAX_TIMER_NODE_CNT) {
        return -1;
    }
    
    p_timer_node = get_timer_node(p_timer_set, timer_id);
    if (p_timer_node->flags == 0) {
        printf("double free timer_id:%d ", timer_id);
        return 0;
    }
    
    delete_one_timer_from_list(p_timer_set, p_timer_node);
    return free_timer_node(p_timer_set, p_timer_node);
}

int reset_one_timer(timer_set_t *p_timer_set, int timer_id)
{
    timer_node_t * p_timer_node = NULL;
    
    // printf("reset timer_id:%d ", timer_id);
    
    if (p_timer_set == NULL || timer_id < 0 || timer_id >= MAX_TIMER_NODE_CNT) {
        return -1;
    }
    
    p_timer_node = get_timer_node(p_timer_set, timer_id);
    if (p_timer_node->flags == 0) {
        printf("timer_id:%d not in work list ", timer_id);
        return -1;
    }
    
    delete_one_timer_from_list(p_timer_set, p_timer_node);
    
    p_timer_node->exp_jiffies = p_timer_node->hold_jiffies + p_timer_set->jiffies;
    
    if (insert_timer_node(p_timer_set, p_timer_node) < 0) {
        printf("insert_timer_node() fail, timer_id:%d ", timer_id);
        free_timer_node(p_timer_set, p_timer_node);
        return -1;
    }
    
    return timer_id;
}

static inline int run_one_timer(timer_node_t *p_timer_node)
{
    return p_timer_node->user_timer.call_back(&(p_timer_node->user_timer));
}

static int move_timer_to_low_level(timer_set_t *ts, list_head_t *curr)
{
    uint32_t nr_deletes = 0;
    uint32_t nr_inserts = 0;
    uint32_t nr_nodes = curr->cnt;

    while (nr_deletes < nr_nodes) {
        timer_node_t *node = delete_first_timer_from_list(ts, curr);
        if (node != NULL) {
            nr_deletes = nr_deletes + 1;
            int ret = insert_timer_node(ts, node);
            if (ret < 0) {
                printf("%s:%d:%s: insert timer node %p into timer set %p failed, free it\n",
                       __FILE__, __LINE__, __FUNCTION__, node, ts);
                free_timer_node(ts, node);
            } else {
                // 插入成功，继续下一次循环处理
                nr_inserts = nr_inserts + 1;
            }
        } else {
            // 从列表中删除定时器失败，终止循环，等待下一次处理
            printf("%s:%d:%s: no timer node in timer set %p\n",
                   __FILE__, __LINE__, __FUNCTION__, ts);
            return -1;
        }
    }

    if (nr_deletes == nr_inserts && nr_inserts == nr_nodes) {
        return 0;
    } else {
        printf("%s:%d:%s: nr_nodes=%u, nr_deletes=%u, nr_inserts=%u\n",
               __FILE__, __LINE__, __FUNCTION__,
               nr_nodes, nr_deletes, nr_inserts);
        return -1;
    }
}

static inline void move_timer_from_higher_level(
    timer_set_t *ts, int curr_level)
{
    int higher_level = curr_level + 1;
    if (0 <= higher_level && higher_level < 8) {
        uint32_t higher_curr_index = ts->curr_lists[higher_level];
        uint32_t higher_next_index = (higher_curr_index + 1) & 0x00FF;
        ts->curr_lists[higher_level] = higher_next_index;

        list_head_t *higher_head = &ts->timer_lists[higher_level][higher_curr_index];
        int ret = move_timer_to_low_level(ts, higher_head); // 将高一级的分发到本级
        if (ret < 0) {
            printf("%s:%d:%s: move level %d timer list %d failed\n",
                   __FILE__, __LINE__, __FUNCTION__,
                   higher_level, higher_curr_index);
            // 虽然目前移动定时器有错误，但不影响整体继续运行
        } else {
            // 移动到更低级别成功，继续处理更高级别的定时器列表
            printf("move timer to lower level\n");
        }

        if (higher_next_index == 0) {
            // 更高一级的定时器列表处理完毕，继续处理更高级别的定时器列表
            move_timer_from_higher_level(ts, higher_level);
        } else {
            // 更高一级的定时器列表还没有处理完毕，不用再往更高级别处理
            printf("handling timer, no more high level\n");
        }
    } else {
        // 当前级别已是最高级别，没有更高级别，不做处理
        printf("already top level, do nothing\n");
    }
}

static void handle_one_timer_node(
    timer_set_t *ts, list_head_t *curr, timer_node_t *node)
{
    (void) curr;

    run_one_timer(node);

    // 运行完当前定时器节点后，当前定时器节点没有被释放，也没有被重置
    if (node->flags != 0 && node->p_list == NULL) {
        if (node->user_timer.loop_cnt > 1) {
            if (node->user_timer.loop_cnt < 0x80000000) {
                node->user_timer.loop_cnt -= 1;
            } else {
                // loop_cnt >= 0x80000000 是不会终止的定时器
            }
            node->exp_jiffies += node->hold_jiffies;
            int ret = insert_timer_node(ts, node);
            if (ret < 0) {
                printf("%s:%d:%s: insert timer %d failed\n",
                       __FILE__, __LINE__, __FUNCTION__,
                       node->user_timer.timer_id);
                goto done;
            } else {
                // 插入定时器成功，等待下一次处理
            }
        } else /* node->user_timer.loop_cnt <= 1 */ {
        done:
            free_timer_node(ts, node);
        }
    } else /* node->flags == 0 || node->p_list == NULL */ {
        // 当前定时器已被释放或者重置，不做处理
    }
}

static void __run_timer_set(timer_set_t *ts)
{
    ts->jiffies += 1;

    uint32_t curr_list_index = ts->curr_lists[0];
    list_head_t *curr = &ts->timer_lists[0][curr_list_index];
    uint32_t nr_nodes = curr->cnt;

    uint32_t next_list_index = (curr_list_index + 1) & 0x00FF;
    ts->curr_lists[0] = next_list_index;

    uint32_t i;
    for (i = 0; i < nr_nodes; i++) {
        timer_node_t *node = delete_first_timer_from_list(ts, curr);
        if (node != NULL) {
            printf("handle timer node %p\n", node);
            handle_one_timer_node(ts, curr, node);
        } else {
            printf("%s:%d:%s: no timer node in timer set %p current list %p\n",
                   __FILE__, __LINE__, __FUNCTION__, ts, curr);
            init_list_head(curr);
            break;
        }
    }

    if (next_list_index == 0) {
        // 当前级别的定时器列表处理完毕，将更高一级的定时器移动到当前级别，等待
        // 下一次处理
        printf("move higher level\n");
        move_timer_from_higher_level(ts, 0);
    } else {
        // 当前级别的定时器还没有处理完毕，等待下次处理
        printf("next_list_index: %d, do nothing\n", next_list_index);
    }
}

void run_timer_set(timer_set_t *p_timer_set)
{
    if (p_timer_set != NULL) {
        __run_timer_set(p_timer_set);
    } else {
        printf("%s:%d:%s: invalid timer_set",
               __FILE__, __LINE__, __FUNCTION__);
    }
}
