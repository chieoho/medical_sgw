#include "mt_log.h"
#include "public.h"

const char * command_string(uint32_t c)
{
    if (c == CMD_HEARTBEART_REQ)      return "CMD_HEARTBEART_REQ";
    if (c == CMD_HEARTBEART_RSP)      return "CMD_HEARTBEART_RSP";
    if (c == CMD_START_UPLOAD_REQ)    return "CMD_START_UPLOAD_REQ";
    if (c == CMD_START_UPLOAD_RSP)    return "CMD_START_UPLOAD_RSP";
    if (c == CMD_UPLOAD_DATA_REQ)     return "CMD_UPLOAD_DATA_REQ";
    if (c == CMD_UPLOAD_DATA_RSP)     return "CMD_UPLOAD_DATA_RSP";
    if (c == CMD_UPLOAD_FINISH_REQ)   return "CMD_UPLOAD_FINISH_REQ";
    if (c == CMD_UPLOAD_FINISH_RSP)   return "CMD_UPLOAD_FINISH_RSP";
    if (c == CMD_START_DOWNLOAD_REQ)  return "CMD_START_DOWNLOAD_REQ";
    if (c == CMD_START_DOWNLOAD_RSP)  return "CMD_START_DOWNLOAD_RSP";
    if (c == CMD_DOWNLOAD_DATA_REQ)   return "CMD_DOWNLOAD_DATA_REQ";
    if (c == CMD_DOWNLOAD_DATA_RSP)   return "CMD_DOWNLOAD_DATA_RSP";
    if (c == CMD_DOWNLOAD_FINISH_REQ) return "CMD_DOWNLOAD_FINISH_REQ";
    if (c == CMD_DOWNLOAD_FINISH_RSP) return "CMD_DOWNLOAD_FINISH_RSP";
    if (c == CMD_DELETE_REQ)          return "CMD_DELETE_REQ";
    if (c == CMD_DELETE_RSP)          return "CMD_DELETE_RSP";
    if (c == CMD_MIGRATION_START_REQ) return "CMD_MIGRATION_START_REQ";
    if (c == CMD_MIGRATION_START_RSP) return "CMD_MIGRATION_START_RSP";
    if (c == CMD_MIGRATION_STOP_REQ)  return "CMD_MIGRATION_STOP_REQ";
    if (c == CMD_MIGRATION_STOP_RSP)  return "CMD_MIGRATION_STOP_RSP";
    if (c == CMD_MIGRATION_FINISHED_REQ) return "CMD_MIGRATION_FINISHED_REQ";
    if (c == CMD_MIGRATION_FINISHED_RSP) return "CMD_MIGRATION_FINISHED_RSP";
    if (c == CMD_MIGRATION_CANCEL_REQ)   return "CMD_MIGRATION_CANCEL_REQ";
    if (c == CMD_MIGRATION_CANCEL_RSP)   return "CMD_MIGRATION_CANCEL_RSP";
    if (c == CMD_GET_FILE_LIST_REQ)      return "CMD_GET_FILE_LIST_REQ";
    if (c == CMD_GET_FILE_LIST_RSP)      return "CMD_GET_FILE_LIST_RSP";
    return "UNKNOWN";
}
