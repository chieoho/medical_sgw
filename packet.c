#include "public.h"
#include "mt_log.h"

void header_dump_packed(msg_t *h)
{
    log_debug("---- header dump packed ----");
    log_debug("length: %u, major: %d, minor: %d", be32toh(h->length), h->major, h->minor);
    log_debug("src_type: %d, dst_type: %d", h->src_type, h->dst_type);
    log_debug("src_id: %u, dst_id: %u", be32toh(h->src_id), be32toh(h->dst_id));
    log_debug("trans_id: %llu, sequence: %llu",
           (unsigned long long int)be64toh(h->trans_id),
           (unsigned long long int)be64toh(h->sequence));
    log_debug("command: 0x%x, ack_code: %u", be32toh(h->command), be32toh(h->ack_code));
    log_debug("total: %llu, offset: %llu, count: %u",
           (unsigned long long int)be64toh(h->total),
           (unsigned long long int)be64toh(h->offset),
           be32toh(h->count));
}

void header_dump_unpack(msg_t *h)
{
    log_debug("---- header dump unpack ----");
    log_debug("length: %u, major: %d, minor: %d", h->length, h->major, h->minor);
    log_debug("src_type: %d, dst_type: %d", h->src_type, h->dst_type);
    log_debug("src_id: %u, dst_id: %u", h->src_id, h->dst_id);
    log_debug("trans_id: %llu, sequence: %llu",
           (unsigned long long int)h->trans_id,
           (unsigned long long int)h->sequence);
    log_debug("command: 0x%x, ack_code: %u", h->command, h->ack_code);
    log_debug("total: %llu, offset: %llu, count: %u",
           (unsigned long long int)h->total,
           (unsigned long long int)h->offset,
           h->count);
}

void taskinfo_dump_unpack(task_info_t *t)
{
    uint32_t sgw_ip = t->sgw_ip;
    uint32_t proxy_ip = t->proxy_ip;

    log_debug("---- task_info dump unpack ----");
    log_debug("operation: %u", t->operation);
    log_debug("region_id: %u", t->region_id);
    log_debug("site_id: %u", t->site_id);
    log_debug("app_id: %u", t->app_id);
    log_debug("timestamp: %u", t->timestamp);
    log_debug("sgw_port: %u", t->sgw_port);
    log_debug("proxy_port: %u", t->proxy_port);
    log_debug("sgw_ip: %u.%u.%u.%u",
              sgw_ip & 0xFF, sgw_ip & 0xFF00,
              sgw_ip & 0xFF0000, sgw_ip & 0xFF000000);
    log_debug("proxy_ip: %u.%u.%u.%u",
              proxy_ip & 0xFF, proxy_ip & 0xFF00,
              proxy_ip & 0xFF0000, proxy_ip & 0xFF000000);
    log_debug("sgw_id: %u", t->sgw_id);
    log_debug("proxy_id: %u", t->proxy_id);
    log_debug("file_len: %llu", (unsigned long long int)t->file_len);
    log_debug("file_md5: %s", t->file_md5);
    log_debug("file_name: %s", t->file_name);
    log_debug("metadata_len: %u", t->metadata_len);
}

void taskinfo_dump_packed(task_info_t *t)
{
    uint32_t sgw_ip = be32toh(t->sgw_ip);
    uint32_t proxy_ip = be32toh(t->proxy_ip);

    log_debug("---- task_info dump packed ----");
    log_debug("operation: %u", be16toh(t->operation));
    log_debug("region_id: %u", be16toh(t->region_id));
    log_debug("site_id: %u", be32toh(t->site_id));
    log_debug("app_id: %u", be32toh(t->app_id));
    log_debug("timestamp: %u", be32toh(t->timestamp));
    log_debug("sgw_port: %u", be16toh(t->sgw_port));
    log_debug("proxy_port: %u", be16toh(t->proxy_port));
    log_debug("sgw_ip: %u.%u.%u.%u",
              sgw_ip & 0xFF, sgw_ip & 0xFF00,
              sgw_ip & 0xFF0000, sgw_ip & 0xFF000000);
    log_debug("proxy_ip: %u.%u.%u.%u",
              proxy_ip & 0xFF, proxy_ip & 0xFF00,
              proxy_ip & 0xFF0000, proxy_ip & 0xFF000000);
    log_debug("sgw_id: %u", be32toh(t->sgw_id));
    log_debug("proxy_id: %u", be32toh(t->proxy_id));
    log_debug("file_len: %llu", (unsigned long long int)be64toh(t->file_len));
    log_debug("file_md5: %s", t->file_md5);
    log_debug("file_name: %s", t->file_name);
    log_debug("metadata_len: %u", be32toh(t->metadata_len));
}

void pr_msg_packed(msg_t *m)
{
    task_info_t *t;

    int command = be32toh(m->command);
    switch (command) {
    case CMD_START_UPLOAD_RSP:
    case CMD_START_DOWNLOAD_RSP:
        log_debug("%s", command_string(command));
        header_dump_packed(m);
        t = (task_info_t *)m->data;
        taskinfo_dump_packed(t);
        break;
    case CMD_UPLOAD_FINISH_RSP:
    case CMD_DOWNLOAD_FINISH_RSP:
        log_debug("%s", command_string(command));
        header_dump_packed(m);
        break;
    default:
        break;
    }
}

void pr_msg_unpack(msg_t *m)
{
    task_info_t *t;

    switch (m->command) {
    case CMD_SEQ_DOWNLOAD_REQ:
    case CMD_START_UPLOAD_REQ:
    case CMD_START_DOWNLOAD_REQ:
    case CMD_GET_FILE_LIST_REQ:
        log_debug("%s", command_string(m->command));
        header_dump_unpack(m);
        t = (task_info_t *)m->data;
        taskinfo_dump_unpack(t);
        break;
    case CMD_UPLOAD_FINISH_REQ:
    case CMD_DOWNLOAD_FINISH_REQ:
        log_debug("%s", command_string(m->command));
        header_dump_unpack(m);
        break;
    default:
        break;
    }
}
