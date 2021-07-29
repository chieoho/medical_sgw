#ifndef CONFIG_H
#define CONFIG_H

#define MD5
#ifdef MD5
#define CHECK_MD5 "on"
#else
#define CHECK_MD5 "off"
#endif

/* #undef TLS */
#ifdef TLS
#define USE_TLS "on"
#else
#define USE_TLS "off"
#endif

#ifndef MS_PER_TICK
#define MS_PER_TICK (1000) /* 单位是毫秒 */
#endif

#ifndef MNTDIRNAME
#define MNTDIRNAME "mountpoint"
#endif

#endif  /* CONFIG_H */
