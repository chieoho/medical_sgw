CC := gcc
CFLAGS := -g -Wall -Wextra -Wno-unused-function -I..

TARGETS += test_sgw

.PHONY: all

all: $(TARGETS)

test_sgw: test_sgw.o ../md5ops.o ../md5.o
	$(CC) $(CFLAGS) $^ -pthread -o $@

.PHONY: clean

clean:
	rm -f $(TARGETS) *.o
