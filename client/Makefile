CC = gcc
CFLAGS = -Wall -Wextra -std=c23 -O3 -D_GNU_SOURCE
LDFLAGS = -lreadline

TARGET = kv-client
OBJS = kv_client.o main.o

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS)

kv_client.o: kv_client.c kv_client.h
	$(CC) $(CFLAGS) -c kv_client.c

main.o: main.c kv_client.h
	$(CC) $(CFLAGS) -c main.c

clean:
	rm -f $(TARGET) $(OBJS)