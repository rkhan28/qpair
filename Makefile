CC      ?= gcc
WARN    := -Wall -Wextra -Wshadow -Wconversion -Wno-sign-conversion -Wstrict-prototypes
CFLAGS  := -std=gnu11 -O2 -g -Iinclude $(WARN) -pthread
SRC     := src/device.c src/driver.c
HDR     := $(wildcard include/*.h)

all: build/test build/bench

build/test: $(SRC) tests/test_main.c $(HDR)
	@mkdir -p build
	$(CC) $(CFLAGS) $(SRC) tests/test_main.c -o $@

build/bench: $(SRC) tests/bench.c $(HDR)
	@mkdir -p build
	$(CC) $(CFLAGS) $(SRC) tests/bench.c -o $@

build/test-asan: $(SRC) tests/test_main.c $(HDR)
	@mkdir -p build
	$(CC) $(CFLAGS) -O1 -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined $(SRC) tests/test_main.c -o $@

build/test-tsan: $(SRC) tests/test_main.c $(HDR)
	@mkdir -p build
	$(CC) $(CFLAGS) -O1 -fsanitize=thread $(SRC) tests/test_main.c -o $@

test: build/test
	./build/test

asan: build/test-asan
	QP_TEST_SCALE=2 ./build/test-asan

tsan: build/test-tsan
	QP_TEST_SCALE=4 ./build/test-tsan

bench: build/bench
	./build/bench

check: test asan tsan

clean:
	rm -rf build

.PHONY: all test asan tsan bench check clean
