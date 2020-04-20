
CC = gcc
# -fPIC: 地址无关
CFLAGS = -g -Wall -Wextra -Wno-unused-function -Wno-unused-parameter -fPIC

all: sgw test

OBJECTS=timer_set.o mt_log.o events_poll.o conn_mgmt.o main.o cmdstr.o pathops.o md5.o md5ops.o

main.o: handler.c main.c
	$(CC) -c $(CFLAGS) main.c -o $@

%.o:%.c
	$(CC) $(CFLAGS) -O2 -c $< -o $@

sgw:$(OBJECTS)
	$(CC) $(CFLAGS) -o sgw $(OBJECTS) -lpthread

.PHONY: test clean cppcheck splint

test:
	make -C test clean all

clean:
	rm -f sgw $(OBJECTS)

cppcheck:
	cppcheck -q --template='{file}:{line}:{severity}:{message}' --error-exitcode=1 --language=c --std=c99 --platform=unix64 *.c *.h

splint:
	splint +posixlib *.c *.h
