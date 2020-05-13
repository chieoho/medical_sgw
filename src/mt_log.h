
#ifndef MT_LOG_H
#define MT_LOG_H


#define	LOG_EMERG	0	/* system is unusable */
#define	LOG_ALERT	1	/* action must be taken immediately */
#define	LOG_CRIT	2	/* critical conditions */
#define	LOG_ERROR	3	/* error conditions */
#define	LOG_WARNING	4	/* warning conditions */
#define	LOG_NOTICE	5	/* normal but significant condition */
#define	LOG_INFO	6	/* informational */
#define	LOG_DEBUG	7	/* debug-level messages */


int init_log(char * app_name, int buffer_size);

int write_log(int log_level, const char *file_name, const char *func_name, const int line_num,
              const char *format, ...) __attribute__ ((__format__ (__printf__, 5, 6)));
                  
#define log_debug(format, args...)      write_log(LOG_DEBUG, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_info(format, args...)       write_log(LOG_INFO, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_notice(format, args...)     write_log(LOG_NOTICE, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_warning(format, args...)    write_log(LOG_WARNING, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_error(format, args...)      write_log(LOG_ERROR, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_crit(format, args...)       write_log(LOG_CRIT, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_alert(format, args...)      write_log(LOG_ALERT, __FILE__, __FUNCTION__, __LINE__, format, ##args)
#define log_emerg(format, args...)      write_log(LOG_EMERG, __FILE__, __FUNCTION__, __LINE__, format, ##args)


#endif

