CC ?= gcc
CFLAGS ?= -std=gnu11 -O2 -g -Wall -Wextra -Werror -fPIC
CPPFLAGS ?= -Iinclude -Isrc
LDFLAGS ?= -pthread

SRCS := src/iomux_ring.c src/iomux_timer.c src/iomux_worker.c src/iomux_poller.c
OBJS := $(SRCS:.c=.o)

TESTS := test_basic test_timer test_bind test_errors test_scale

.PHONY: all clean check

all: libiomux.so libiomux.a

libiomux.so: $(OBJS)
	$(CC) -shared -o $@ $(OBJS) $(LDFLAGS)

libiomux.a: $(OBJS)
	ar rcs $@ $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

test_%: tests/test_%.c libiomux.a
	$(CC) $(CFLAGS) $(CPPFLAGS) -o $@ $< libiomux.a $(LDFLAGS)

check: $(TESTS)
	./test_basic
	./test_timer
	./test_bind
	./test_errors
	ulimit -n 65535 2>/dev/null; ./test_scale

clean:
	rm -f $(OBJS) libiomux.so libiomux.a $(TESTS)
