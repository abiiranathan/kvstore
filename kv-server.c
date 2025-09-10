#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "include/common.h"
#include "include/kvapi.h"

// Server configuration constants
#define DEFAULT_PORT       7379
#define DEFAULT_BACKLOG    512
#define MAX_EVENTS         1024
#define BUFFER_SIZE        (64 * 1024)  // 64KB buffer per client
#define MAX_CLIENTS        10000
#define WORKER_THREADS     4
#define MAX_VALUE_SIZE     (1024 * 1024)  // 1MB max value
#define CLIENT_TIMEOUT     300            // 5 minutes
#define KEEPALIVE_IDLE     60
#define KEEPALIVE_INTERVAL 10
#define KEEPALIVE_COUNT    3

// Protocol constants
#define PROTOCOL_VERSION        "1.0"
#define MAX_COMMAND_ARGS        32
#define RESP_OK                 "+OK\r\n"
#define RESP_ERROR_PREFIX       "-ERR "
#define RESP_NULL               "$-1\r\n"
#define RESP_INTEGER_PREFIX     ":"
#define RESP_BULK_STRING_PREFIX "$"
#define RESP_ARRAY_PREFIX       "*"

// Server state
typedef struct {
    int epoll_fd;
    int listen_fd;
    kvapi_handle_t* api;
    atomic_bool running;
    atomic_size_t active_connections;
    atomic_ullong total_requests;
    atomic_ullong total_errors;
    struct timespec start_time;
} server_state_t;

// Client connection state
typedef enum {
    CLIENT_STATE_READING_COMMAND,
    CLIENT_STATE_PROCESSING,
    CLIENT_STATE_WRITING_RESPONSE,
    CLIENT_STATE_CLOSING
} client_state_e;

typedef struct client {
    int fd;
    client_state_e state;
    struct sockaddr_in addr;

    // Buffers
    char read_buffer[BUFFER_SIZE];
    size_t read_pos;
    size_t read_len;

    char write_buffer[BUFFER_SIZE];
    size_t write_pos;
    size_t write_len;

    // Command parsing
    char** args;
    int argc;

    // Timing
    struct timespec last_activity;

    // Linked list for cleanup
    struct client* next;
} client_t;

// Global server state
static server_state_t g_server         = {0};
static client_t* g_clients_head        = NULL;
static pthread_mutex_t g_clients_mutex = PTHREAD_MUTEX_INITIALIZER;

// Configuration
typedef struct {
    const char* bind_addr;
    int port;
    int backlog;
    kvapi_config_t kv_config;
    bool daemonize;
    const char* log_file;
    size_t worker_threads;
} server_config_t;

static server_config_t g_config = {.bind_addr = "127.0.0.1",
                                   .port      = DEFAULT_PORT,
                                   .backlog   = DEFAULT_BACKLOG,
                                   .kv_config =
                                       {
                                           .capacity           = KVSTORE_DEFAULT_CAPACITY,
                                           .db_file            = "kvstore.db",
                                           .auto_save          = true,
                                           .auto_save_interval = 60,
                                       },
                                   .daemonize      = false,
                                   .log_file       = NULL,
                                   .worker_threads = WORKER_THREADS};

// Function declarations
static void signal_handler(int sig);
static int setup_server_socket(void);
static int setup_epoll(void);
static void setup_signals(void);
static int make_socket_non_blocking(int fd);
static int set_socket_options(int fd);
static client_t* create_client(int fd, struct sockaddr_in* addr);
static void destroy_client(client_t* client);
static void add_client(client_t* client);
static void remove_client(client_t* client);
static void cleanup_inactive_clients(void);
static int handle_new_connection(void);
static int handle_client_read(client_t* client);
static int handle_client_write(client_t* client);
static int process_client_command(client_t* client);
static void send_response(client_t* client, const char* response);
static void send_error(client_t* client, const char* error_msg);
static void send_ok(client_t* client);
static void send_null(client_t* client);
static void send_integer(client_t* client, int64_t value);
static void send_string(client_t* client, const char* str);
static void send_bulk_string(client_t* client, const char* str, size_t len);
static char** parse_command(const char* line, int* argc);
static void free_command_args(char** args, int argc);
static int cmd_ping(client_t* client, int argc, char** args);
static int cmd_info(client_t* client, int argc, char** args);
static int cmd_set(client_t* client, int argc, char** args);
static int cmd_get(client_t* client, int argc, char** args);
static int cmd_del(client_t* client, int argc, char** args);
static int cmd_exists(client_t* client, int argc, char** args);
static int cmd_keys(client_t* client, int argc, char** args);
static int cmd_clear(client_t* client, int argc, char** args);
static int cmd_stats(client_t* client, int argc, char** args);
static int cmd_save(client_t* client, int argc, char** args);
static int cmd_load(client_t* client, int argc, char** args);
static int cmd_quit(client_t* client, int argc, char** args);
static void print_usage(const char* prog_name);
static void print_version(void);
static void cleanup_server(void);
static void* worker_thread(void* arg);

// Command table
typedef struct {
    const char* name;
    int (*handler)(client_t* client, int argc, char** args);
    int min_args;
    int max_args;  // 0 means no upper limit
} server_command_t;

static const server_command_t server_commands[] = {
    {"PING", cmd_ping, 0, 1}, {"INFO", cmd_info, 0, 1},   {"SET", cmd_set, 2, 0},
    {"GET", cmd_get, 1, 1},   {"DEL", cmd_del, 1, 1},     {"EXISTS", cmd_exists, 1, 1},
    {"KEYS", cmd_keys, 0, 0}, {"CLEAR", cmd_clear, 0, 0}, {"STATS", cmd_stats, 0, 0},
    {"SAVE", cmd_save, 0, 1}, {"LOAD", cmd_load, 0, 1},   {"QUIT", cmd_quit, 0, 0},
    {NULL, NULL, 0, 0}};

// Signal handler for graceful shutdown
static void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        kv_log(LOG_INFO, "Received signal %d, shutting down server", sig);
        atomic_store(&g_server.running, false);
    }
}

// Setup server socket with production options
static int setup_server_socket(void) {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd == -1) {
        kv_log(LOG_ERROR, "Failed to create socket: %s", strerror(errno));
        return -1;
    }

    if (set_socket_options(listen_fd) != 0) {
        close(listen_fd);
        return -1;
    }

    if (make_socket_non_blocking(listen_fd) != 0) {
        close(listen_fd);
        return -1;
    }

    struct sockaddr_in server_addr = {0};
    server_addr.sin_family         = AF_INET;
    server_addr.sin_port           = htons((uint16_t)g_config.port);

    if (inet_pton(AF_INET, g_config.bind_addr, &server_addr.sin_addr) != 1) {
        kv_log(LOG_ERROR, "Invalid bind address: %s", g_config.bind_addr);
        close(listen_fd);
        return -1;
    }

    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        kv_log(LOG_ERROR, "Failed to bind to %s:%d: %s", g_config.bind_addr, g_config.port, strerror(errno));
        close(listen_fd);
        return -1;
    }

    if (listen(listen_fd, g_config.backlog) == -1) {
        kv_log(LOG_ERROR, "Failed to listen: %s", strerror(errno));
        close(listen_fd);
        return -1;
    }

    kv_log(LOG_INFO, "Server listening on %s:%d", g_config.bind_addr, g_config.port);
    return listen_fd;
}

// Set production-grade socket options
static int set_socket_options(int fd) {
    int opt = 1;

    // SO_REUSEADDR for quick restart
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        kv_log(LOG_ERROR, "setsockopt SO_REUSEADDR failed: %s", strerror(errno));
        return -1;
    }

    // SO_REUSEPORT for load balancing across processes
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) == -1) {
        kv_log(LOG_WARNING, "setsockopt SO_REUSEPORT failed: %s", strerror(errno));
        // Not critical, continue
    }

    // TCP_NODELAY to disable Nagle's algorithm for low latency
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) == -1) {
        kv_log(LOG_WARNING, "setsockopt TCP_NODELAY failed: %s", strerror(errno));
    }

    // SO_KEEPALIVE for connection health monitoring
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt)) == -1) {
        kv_log(LOG_WARNING, "setsockopt SO_KEEPALIVE failed: %s", strerror(errno));
    }

    // TCP keepalive parameters
    int keepidle  = KEEPALIVE_IDLE;
    int keepintvl = KEEPALIVE_INTERVAL;
    int keepcnt   = KEEPALIVE_COUNT;

    setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &keepidle, sizeof(keepidle));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &keepintvl, sizeof(keepintvl));
    setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &keepcnt, sizeof(keepcnt));

    // Set send/receive buffer sizes
    int buffer_size = BUFFER_SIZE;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size));

    return 0;
}

// Make socket non-blocking
static int make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        kv_log(LOG_ERROR, "fcntl F_GETFL failed: %s", strerror(errno));
        return -1;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        kv_log(LOG_ERROR, "fcntl F_SETFL failed: %s", strerror(errno));
        return -1;
    }

    return 0;
}

// Setup epoll instance
static int setup_epoll(void) {
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd == -1) {
        kv_log(LOG_ERROR, "epoll_create1 failed: %s", strerror(errno));
        return -1;
    }

    struct epoll_event ev = {0};
    ev.events             = EPOLLIN | EPOLLET;  // Edge-triggered for performance
    ev.data.fd            = g_server.listen_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_server.listen_fd, &ev) == -1) {
        kv_log(LOG_ERROR, "epoll_ctl ADD listen_fd failed: %s", strerror(errno));
        close(epoll_fd);
        return -1;
    }

    return epoll_fd;
}

// Setup signal handlers
static void setup_signals(void) {
    struct sigaction sa = {0};
    sa.sa_handler       = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // Ignore SIGPIPE
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
}

// Client management functions
static client_t* create_client(int fd, struct sockaddr_in* addr) {
    client_t* client = calloc(1, sizeof(client_t));
    if (!client) {
        return NULL;
    }

    client->fd    = fd;
    client->state = CLIENT_STATE_READING_COMMAND;
    client->addr  = *addr;
    clock_gettime(CLOCK_MONOTONIC, &client->last_activity);

    // Set socket options for client
    set_socket_options(fd);
    make_socket_non_blocking(fd);

    atomic_fetch_add(&g_server.active_connections, 1);
    return client;
}

static void destroy_client(client_t* client) {
    if (!client) return;

    if (client->fd >= 0) {
        epoll_ctl(g_server.epoll_fd, EPOLL_CTL_DEL, client->fd, NULL);
        close(client->fd);
    }

    free_command_args(client->args, client->argc);
    free(client);

    atomic_fetch_sub(&g_server.active_connections, 1);
}

static void add_client(client_t* client) {
    pthread_mutex_lock(&g_clients_mutex);
    client->next   = g_clients_head;
    g_clients_head = client;
    pthread_mutex_unlock(&g_clients_mutex);
}

static void remove_client(client_t* client) {
    pthread_mutex_lock(&g_clients_mutex);

    if (g_clients_head == client) {
        g_clients_head = client->next;
    } else {
        for (client_t* c = g_clients_head; c && c->next; c = c->next) {
            if (c->next == client) {
                c->next = client->next;
                break;
            }
        }
    }

    pthread_mutex_unlock(&g_clients_mutex);
}

// Handle new connection
static int handle_new_connection(void) {
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);

    while (true) {
        int client_fd = accept4(g_server.listen_fd, (struct sockaddr*)&client_addr, &addr_len,
                                SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // No more connections
            }
            kv_log(LOG_ERROR, "accept failed: %s", strerror(errno));
            return -1;
        }

        if (atomic_load(&g_server.active_connections) >= MAX_CLIENTS) {
            kv_log(LOG_WARNING, "Max clients reached, rejecting connection");
            close(client_fd);
            continue;
        }

        client_t* client = create_client(client_fd, &client_addr);
        if (!client) {
            kv_log(LOG_ERROR, "Failed to create client");
            close(client_fd);
            continue;
        }

        struct epoll_event ev = {0};
        ev.events             = EPOLLIN | EPOLLET;
        ev.data.ptr           = client;

        if (epoll_ctl(g_server.epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
            kv_log(LOG_ERROR, "epoll_ctl ADD client failed: %s", strerror(errno));
            destroy_client(client);
            continue;
        }

        add_client(client);

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        kv_log(LOG_DEBUG, "New connection from %s:%d (fd=%d)", client_ip, ntohs(client_addr.sin_port),
               client_fd);
    }

    return 0;
}

// Handle client read events
// Handle client read events
static int handle_client_read(client_t* client) {
    clock_gettime(CLOCK_MONOTONIC, &client->last_activity);

    while (true) {
        ssize_t bytes_read =
            read(client->fd, client->read_buffer + client->read_len, BUFFER_SIZE - client->read_len - 1);

        if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // No more data to read for now
            }
            kv_log(LOG_DEBUG, "Client read error (fd=%d): %s", client->fd, strerror(errno));
            return -1;  // Connection error
        }

        if (bytes_read == 0) {
            kv_log(LOG_DEBUG, "Client disconnected (fd=%d)", client->fd);
            return -1;  // Connection closed by peer
        }

        client->read_len += (size_t)bytes_read;

        // Ensure buffer is null-terminated for string functions
        client->read_buffer[client->read_len] = '\0';

        // Process all complete commands in the buffer
        char* current_pos = client->read_buffer;
        char* line_end;

        // Use strchr to find the next newline, which is the universal terminator
        while ((line_end = strchr(current_pos, '\n')) != NULL) {
            // Null-terminate the line. Check for a preceding '\r' and overwrite it too.
            if (line_end > current_pos && *(line_end - 1) == '\r') {
                *(line_end - 1) = '\0';
            } else {
                *line_end = '\0';
            }

            // Skip empty lines (e.g., user just presses enter)
            if (strlen(current_pos) > 0) {
                client->state = CLIENT_STATE_PROCESSING;
                client->args  = parse_command(current_pos, &client->argc);

                if (!client->args || client->argc == 0) {
                    send_error(client, "Invalid command format");
                } else {
                    if (process_client_command(client) == 0) {
                        atomic_fetch_add(&g_server.total_requests, 1);
                    } else {
                        atomic_fetch_add(&g_server.total_errors, 1);
                    }
                }

                // Clean up after processing the command
                free_command_args(client->args, client->argc);
                client->args  = NULL;
                client->argc  = 0;
                client->state = CLIENT_STATE_READING_COMMAND;
            }

            // Advance our position to the character after the '\n'
            current_pos = line_end + 1;
        }

        // Compact the buffer: move any partial command to the beginning
        size_t processed_len = (size_t)(current_pos - client->read_buffer);
        if (processed_len > 0) {
            client->read_len -= processed_len;
            if (client->read_len > 0) {
                memmove(client->read_buffer, current_pos, client->read_len);
            }
            client->read_buffer[client->read_len] = '\0';
        }

        // Check if the buffer is full with a partial command
        if (client->read_len >= BUFFER_SIZE - 1) {
            send_error(client, "Command too long");
            return -1;  // This is a fatal error for this client
        }
    }

    return 0;  // Success, continue waiting for more data
}

// Handle client write events
static int handle_client_write(client_t* client) {
    while (client->write_pos < client->write_len) {
        ssize_t bytes_written = write(client->fd, client->write_buffer + client->write_pos,
                                      client->write_len - client->write_pos);

        if (bytes_written == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // Would block
            }
            return -1;
        }

        client->write_pos += (size_t)bytes_written;
    }

    if (client->write_pos >= client->write_len) {
        client->write_pos = 0;
        client->write_len = 0;

        // Switch back to read-only events
        struct epoll_event ev = {0};
        ev.events             = EPOLLIN | EPOLLET;
        ev.data.ptr           = client;
        epoll_ctl(g_server.epoll_fd, EPOLL_CTL_MOD, client->fd, &ev);
    }

    return 0;
}

// Process client command
static int process_client_command(client_t* client) {
    if (!client->args || client->argc == 0) {
        send_error(client, "No command provided");
        return -1;
    }

    const char* cmd_name = client->args[0];

    // Find command handler
    for (const server_command_t* cmd = server_commands; cmd->name; cmd++) {
        if (strcasecmp(cmd->name, cmd_name) == 0) {
            if (client->argc < cmd->min_args + 1 || (cmd->max_args > 0 && client->argc > cmd->max_args + 1)) {
                send_error(client, "Wrong number of arguments");
                return -1;
            }
            return cmd->handler(client, client->argc, client->args);
        }
    }

    send_error(client, "Unknown command");
    return -1;
}

// Response sending functions
static void send_response(client_t* client, const char* response) {
    size_t len = strlen(response);
    if (len > BUFFER_SIZE - client->write_len - 1) {
        kv_log(LOG_ERROR, "Response too large");
        return;
    }

    memcpy(client->write_buffer + client->write_len, response, len);
    client->write_len += len;

    // Enable write events
    struct epoll_event ev = {0};
    ev.events             = EPOLLIN | EPOLLOUT | EPOLLET;
    ev.data.ptr           = client;
    epoll_ctl(g_server.epoll_fd, EPOLL_CTL_MOD, client->fd, &ev);
}

static void send_error(client_t* client, const char* error_msg) {
    char response[512];
    snprintf(response, sizeof(response), "-ERR %s\r\n", error_msg);
    send_response(client, response);
}

static void send_ok(client_t* client) {
    send_response(client, "+OK\r\n");
}

static void send_null(client_t* client) {
    send_response(client, "$-1\r\n");
}

static void send_integer(client_t* client, int64_t value) {
    char response[64];
    snprintf(response, sizeof(response), ":%ld\r\n", value);
    send_response(client, response);
}

static void send_string(client_t* client, const char* str) {
    char response[1024];
    snprintf(response, sizeof(response), "+%s\r\n", str);
    send_response(client, response);
}

static void send_bulk_string(client_t* client, const char* str, size_t len) {
    char header[64];
    snprintf(header, sizeof(header), "$%zu\r\n", len);
    send_response(client, header);

    // Send the string data
    if (len > 0 && str) {
        if (len <= BUFFER_SIZE - client->write_len - 3) {
            memcpy(client->write_buffer + client->write_len, str, len);
            client->write_len += len;
            memcpy(client->write_buffer + client->write_len, "\r\n", 2);
            client->write_len += 2;
        }
    } else {
        send_response(client, "\r\n");
    }
}

// Command parsing
static char** parse_command(const char* line, int* argc) {
    if (!line || !argc) return NULL;

    char** args = malloc(MAX_COMMAND_ARGS * sizeof(char*));
    if (!args) return NULL;

    *argc           = 0;
    char* line_copy = strdup(line);
    if (!line_copy) {
        free(args);
        return NULL;
    }

    char* token = strtok(line_copy, " \t");
    while (token && *argc < MAX_COMMAND_ARGS - 1) {
        args[*argc] = strdup(token);
        if (!args[*argc]) {
            free_command_args(args, *argc);
            free(line_copy);
            return NULL;
        }
        (*argc)++;
        token = strtok(NULL, " \t");
    }

    free(line_copy);
    return args;
}

static void free_command_args(char** args, int argc) {
    if (!args) return;

    for (int i = 0; i < argc; i++) {
        free(args[i]);
    }
    free(args);
}

// Command implementations
static int cmd_ping(client_t* client, int argc, char** args) {
    if (argc == 1) {
        send_string(client, "PONG");
    } else {
        send_bulk_string(client, args[1], strlen(args[1]));
    }
    return 0;
}

static int cmd_info(client_t* client, int argc, char** args) {
    (void)argc;
    (void)args;

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    double uptime = (double)(now.tv_sec - g_server.start_time.tv_sec) +
                    (double)(now.tv_nsec - g_server.start_time.tv_nsec) / 1e9;

    char info[2048];
    snprintf(info, sizeof(info),
             "# Server\r\n"
             "kv_version:%d.%d.%d\r\n"
             "protocol_version:%s\r\n"
             "uptime_in_seconds:%.0f\r\n"
             "connected_clients:%zu\r\n"
             "total_commands_processed:%llu\r\n"
             "total_errors:%llu\r\n"
             "\r\n"
             "# Keyspace\r\n"
             "keys:%zu\r\n",
             KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR, KVSTORE_VERSION_PATCH, PROTOCOL_VERSION, uptime,
             atomic_load(&g_server.active_connections), atomic_load(&g_server.total_requests),
             atomic_load(&g_server.total_errors), kvstore_size(kvapi_store(g_server.api)));

    send_bulk_string(client, info, strlen(info));
    return 0;
}

static int cmd_set(client_t* client, int argc, char** args) {
    if (!kv_validate_key(args[1])) {
        send_error(client, "Invalid key format");
        return -1;
    }

    kvstore_error_t error;

    // Handle simple case: SET key value
    if (argc == 3) {
        if (!kv_validate_value_len(strlen(args[2]))) {
            send_error(client, "Value too large");
            return -1;
        }
        error = kvapi_set_string(g_server.api, args[1], args[2]);
    } else {
        // Handle complex case: SET key value with spaces
        size_t value_len = 0;
        for (int i = 2; i < argc; i++) {
            value_len += strlen(args[i]);
            if (i < argc - 1) value_len++;  // for space
        }

        if (!kv_validate_value_len(value_len)) {
            send_error(client, "Value too large");
            return -1;
        }

        char* value = malloc(value_len + 1);
        if (!value) {
            send_error(client, "Memory allocation error");
            return -1;
        }

        value[0] = '\0';
        for (int i = 2; i < argc; i++) {
            strcat(value, args[i]);
            if (i < argc - 1) {
                strcat(value, " ");
            }
        }
        error = kvapi_set_string(g_server.api, args[1], value);
        free(value);
    }

    if (error != KVSTORE_OK) {
        send_error(client, kvstore_error_string(error));
        return -1;
    }

    send_ok(client);
    return 0;
}

static int cmd_get(client_t* client, int argc, char** args) {
    (void)argc;

    if (!kv_validate_key(args[1])) {
        send_error(client, "Invalid key format");
        return -1;
    }

    kvapi_result_t result;
    kvstore_error_t error = kvapi_get(g_server.api, args[1], &result);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        send_null(client);
        return 0;
    } else if (error != KVSTORE_OK) {
        send_error(client, kvstore_error_string(error));
        return -1;
    }

    switch (result.type) {
        case KVSTORE_TYPE_NULL:
            send_null(client);
            break;
        case KVSTORE_TYPE_STRING:
            send_bulk_string(client, result.value.str_val,
                             result.value.str_val ? strlen(result.value.str_val) : 0);
            break;
        case KVSTORE_TYPE_INT64: {
            char int_str[32];
            snprintf(int_str, sizeof(int_str), "%ld", result.value.int_val);
            send_bulk_string(client, int_str, strlen(int_str));
            break;
        }
        case KVSTORE_TYPE_DOUBLE: {
            char double_str[32];
            snprintf(double_str, sizeof(double_str), "%g", result.value.double_val);
            send_bulk_string(client, double_str, strlen(double_str));
            break;
        }
        case KVSTORE_TYPE_BOOL:
            send_bulk_string(client, result.value.bool_val ? "true" : "false", result.value.bool_val ? 4 : 5);
            break;
        case KVSTORE_TYPE_BINARY:
            send_bulk_string(client, (char*)result.value.binary_val.data, result.value.binary_val.len);
            break;
        default:
            send_null(client);
    }

    kvapi_free_result(&result);
    return 0;
}

static int cmd_del(client_t* client, int argc, char** args) {
    (void)argc;
    if (!kv_validate_key(args[1])) {
        send_error(client, "Invalid key format");
        return -1;
    }

    bool deleted;
    kvstore_error_t error = kvapi_delete(g_server.api, args[1], &deleted);

    if (error != KVSTORE_OK && error != KVSTORE_ERROR_KEY_NOT_FOUND) {
        send_error(client, kvstore_error_string(error));
        return -1;
    }

    send_integer(client, deleted ? 1 : 0);
    return 0;
}

static int cmd_exists(client_t* client, int argc, char** args) {
    (void)argc;
    if (!kv_validate_key(args[1])) {
        send_error(client, "Invalid key format");
        return -1;
    }

    bool exists = kvapi_exists(g_server.api, args[1]);
    send_integer(client, exists ? 1 : 0);
    return 0;
}

static int cmd_keys(client_t* client, int argc, char** args) {
    (void)argc;
    (void)args;

    kvstore_t* store = kvapi_store(g_server.api);
    size_t count     = kvstore_size(store);

    // Send array header
    char array_header[32];
    snprintf(array_header, sizeof(array_header), "*%zu\r\n", count);
    send_response(client, array_header);

    if (count == 0) {
        return 0;
    }

    // Send each key as bulk string
    kvstore_iterator_t iter = kvstore_iter_begin(store);
    while (kvstore_iter_valid(&iter)) {
        const kvstore_entry_t* entry = kvstore_iter_get(&iter);
        send_bulk_string(client, (char*)entry->key.data, entry->key.len);
        kvstore_iter_next(&iter);
    }

    return 0;
}

static int cmd_clear(client_t* client, int argc, char** args) {
    (void)argc;
    (void)args;

    kvstore_error_t error = kvapi_clear(g_server.api);
    if (error != KVSTORE_OK) {
        send_error(client, kvstore_error_string(error));
        return -1;
    }

    send_ok(client);
    return 0;
}

static int cmd_stats(client_t* client, int argc, char** args) {
    (void)argc;
    (void)args;

    // Get stats from kvapi and format as Redis-style response
    kvstore_t* store  = kvapi_store(g_server.api);
    size_t total_keys = kvstore_size(store);
    size_t capacity   = kvstore_capacity(store);

    char stats[1024];
    snprintf(stats, sizeof(stats),
             "keys:%zu\r\n"
             "capacity:%zu\r\n"
             "load_factor:%.2f\r\n"
             "memory_usage:estimated\r\n",
             total_keys, capacity, capacity > 0 ? (double)total_keys / (double)capacity : 0.0);

    send_bulk_string(client, stats, strlen(stats));
    return 0;
}

static int cmd_save(client_t* client, int argc, char** args) {
    const char* filename = (argc > 1) ? args[1] : g_config.kv_config.db_file;

    kvstore_error_t error = kvapi_save(g_server.api, filename);
    if (error != KVSTORE_OK) {
        send_error(client, kvstore_error_string(error));
        return -1;
    }

    send_ok(client);
    return 0;
}

static int cmd_load(client_t* client, int argc, char** args) {
    const char* filename = (argc > 1) ? args[1] : g_config.kv_config.db_file;

    kvstore_error_t error = kvapi_load(g_server.api, filename);
    if (error != KVSTORE_OK) {
        send_error(client, kvstore_error_string(error));
        return -1;
    }

    send_ok(client);
    return 0;
}

static int cmd_quit(client_t* client, int argc, char** args) {
    (void)argc;
    (void)args;

    send_ok(client);
    client->state = CLIENT_STATE_CLOSING;
    return 0;
}

// Cleanup inactive clients
static void cleanup_inactive_clients(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);

    pthread_mutex_lock(&g_clients_mutex);

    client_t* client = g_clients_head;
    client_t* prev   = NULL;

    while (client) {
        double idle_time = (double)(now.tv_sec - client->last_activity.tv_sec) +
                           (double)(now.tv_nsec - client->last_activity.tv_nsec) / 1e9;

        if (idle_time > CLIENT_TIMEOUT || client->state == CLIENT_STATE_CLOSING) {
            client_t* next = client->next;
            if (prev) {
                prev->next = next;
            } else {
                g_clients_head = next;
            }

            kv_log(LOG_DEBUG, "Cleaning up inactive client (fd=%d, idle=%.1fs)", client->fd, idle_time);
            destroy_client(client);

            client = next;
        } else {
            prev   = client;
            client = client->next;
        }
    }

    pthread_mutex_unlock(&g_clients_mutex);
}

// Worker thread for handling client cleanup and maintenance
static void* worker_thread(void* arg) {
    (void)arg;

    while (atomic_load(&g_server.running)) {
        cleanup_inactive_clients();
        sleep(10);  // Cleanup every 10 seconds
    }

    return NULL;
}

// Main server loop
static int run_server(void) {
    struct epoll_event events[MAX_EVENTS];

    // Start worker threads
    pthread_t* worker_threads = calloc(g_config.worker_threads, sizeof(pthread_t));
    if (!worker_threads) {
        perror("calloc");
        return -1;
    }

    for (size_t i = 0; i < g_config.worker_threads; i++) {
        if (pthread_create(&worker_threads[i], NULL, worker_thread, NULL) != 0) {
            kv_log(LOG_ERROR, "Failed to create worker thread %zu", i);
        }
    }

    kv_log(LOG_INFO, "Server started successfully");

    while (atomic_load(&g_server.running)) {
        int nfds = epoll_wait(g_server.epoll_fd, events, MAX_EVENTS, 1000);

        if (nfds == -1) {
            if (errno == EINTR) {
                continue;
            }
            kv_log(LOG_ERROR, "epoll_wait failed: %s", strerror(errno));
            break;
        }

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == g_server.listen_fd) {
                // New connection
                handle_new_connection();
            } else {
                // Client event
                client_t* client = (client_t*)events[i].data.ptr;

                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    // Client error or hangup
                    remove_client(client);
                    destroy_client(client);
                    continue;
                }

                if (events[i].events & EPOLLIN) {
                    if (handle_client_read(client) != 0) {
                        remove_client(client);
                        destroy_client(client);
                        continue;
                    }
                }

                if (events[i].events & EPOLLOUT) {
                    if (handle_client_write(client) != 0) {
                        remove_client(client);
                        destroy_client(client);
                        continue;
                    }
                }
            }
        }
    }

    // Wait for worker threads to finish
    for (size_t i = 0; i < g_config.worker_threads; i++) {
        pthread_join(worker_threads[i], NULL);
    }

    free(worker_threads);

    return 0;
}

// Cleanup and shutdown
static void cleanup_server(void) {
    kv_log(LOG_INFO, "Shutting down server");

    atomic_store(&g_server.running, false);

    // Close epoll fd
    if (g_server.epoll_fd >= 0) {
        close(g_server.epoll_fd);
    }

    // Close listen socket
    if (g_server.listen_fd >= 0) {
        close(g_server.listen_fd);
    }

    // Cleanup all clients
    pthread_mutex_lock(&g_clients_mutex);
    while (g_clients_head) {
        client_t* client = g_clients_head;
        g_clients_head   = client->next;
        destroy_client(client);
    }
    pthread_mutex_unlock(&g_clients_mutex);

    // Cleanup API
    if (g_server.api) {
        kv_cleanup(&g_config.kv_config, g_server.api);
    }

    kv_log(LOG_INFO, "Server shutdown complete");
}

// Usage and version functions
static void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("Options:\n");
    printf("  -p, --port <port>         Listen port (default: %d)\n", DEFAULT_PORT);
    printf("  -b, --bind <addr>         Bind address (default: %s)\n", g_config.bind_addr);
    printf("  -f, --db-file <file>      Database file (default: %s)\n", g_config.kv_config.db_file);
    printf("  -c, --capacity <n>        Initial capacity (default: %d)\n", KVSTORE_DEFAULT_CAPACITY);
    printf("  -w, --workers <n>         Worker threads (default: %d)\n", WORKER_THREADS);
    printf("  -d, --daemonize           Run as daemon\n");
    printf("  -l, --log-file <file>     Log file (default: stderr)\n");
    printf("  -h, --help                Show this help\n");
    printf("  -v, --version             Show version\n");
    printf("      --backlog <n>         Listen backlog (default: %d)\n", DEFAULT_BACKLOG);
    printf("      --no-auto-save        Disable auto-save\n");
}

static void print_version(void) {
    printf("KV Store Server v%d.%d.%d\n", KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR,
           KVSTORE_VERSION_PATCH);
    printf("Protocol version: %s\n", PROTOCOL_VERSION);
    printf("Built with: epoll, edge-triggered I/O\n");
}

// Daemonize process
static int daemonize(void) {
    pid_t pid = fork();
    if (pid < 0) {
        return -1;
    }

    if (pid > 0) {
        exit(0);  // Parent process exits
    }

    // Child process continues
    if (setsid() < 0) {
        return -1;
    }

    // Fork again to ensure we're not a session leader
    pid = fork();
    if (pid < 0) {
        return -1;
    }

    if (pid > 0) {
        exit(0);
    }

    // Change working directory to root
    chdir("/");

    // Close standard file descriptors
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);

    // Redirect to /dev/null
    open("/dev/null", O_RDONLY);  // stdin
    open("/dev/null", O_WRONLY);  // stdout
    open("/dev/null", O_WRONLY);  // stderr

    return 0;
}

// Set resource limits for production
static void set_resource_limits(void) {
    struct rlimit rlim;

    // Set max file descriptors
    if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
        rlim.rlim_cur = rlim.rlim_max;
        if (setrlimit(RLIMIT_NOFILE, &rlim) == 0) {
            kv_log(LOG_INFO, "Set max file descriptors to %lu", rlim.rlim_cur);
        }
    }

    // Set max memory (if needed)
    // getrlimit(RLIMIT_AS, &rlim);
    // rlim.rlim_cur = 2 * 1024 * 1024 * 1024; // 2GB
    // setrlimit(RLIMIT_AS, &rlim);
}

int main(int argc, char** argv) {
    // Parse command line options
    static struct option long_options[] = {
        {"port", required_argument, 0, 'p'},     {"bind", required_argument, 0, 'b'},
        {"db-file", required_argument, 0, 'f'},  {"capacity", required_argument, 0, 'c'},
        {"workers", required_argument, 0, 'w'},  {"daemonize", no_argument, 0, 'd'},
        {"log-file", required_argument, 0, 'l'}, {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'v'},        {"backlog", required_argument, 0, 1001},
        {"no-auto-save", no_argument, 0, 1002},  {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "p:b:f:c:w:dl:hv", long_options, NULL)) != -1) {
        switch (opt) {
            case 'p':
                g_config.port = atoi(optarg);
                if (g_config.port <= 0 || g_config.port > 65535) {
                    fprintf(stderr, "Invalid port: %s\n", optarg);
                    return 1;
                }
                break;
            case 'b':
                g_config.bind_addr = optarg;
                break;
            case 'f':
                g_config.kv_config.db_file = optarg;
                break;
            case 'c':
                g_config.kv_config.capacity = (size_t)atoi(optarg);
                if (g_config.kv_config.capacity == 0) {
                    fprintf(stderr, "Invalid capacity: %s\n", optarg);
                    return 1;
                }
                break;
            case 'w':
                g_config.worker_threads = (size_t)atoi(optarg);
                if (g_config.worker_threads <= 0 || g_config.worker_threads > 64) {
                    fprintf(stderr, "Invalid worker count: %s\n", optarg);
                    return 1;
                }
                break;
            case 'd':
                g_config.daemonize = true;
                break;
            case 'l':
                g_config.log_file = optarg;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 'v':
                print_version();
                return 0;
            case 1001:
                g_config.backlog = atoi(optarg);
                break;
            case 1002:
                g_config.kv_config.auto_save = false;
                break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    // Initialize server state
    atomic_store(&g_server.running, true);
    atomic_store(&g_server.active_connections, 0);
    atomic_store(&g_server.total_requests, 0);
    atomic_store(&g_server.total_errors, 0);
    clock_gettime(CLOCK_MONOTONIC, &g_server.start_time);

    // Daemonize if requested
    if (g_config.daemonize) {
        if (daemonize() != 0) {
            fprintf(stderr, "Failed to daemonize\n");
            return 1;
        }
    }

    // Set up logging
    if (g_config.log_file) {
        // Redirect stderr to log file
        freopen(g_config.log_file, "a", stderr);
    }

    kv_log(LOG_INFO, "Starting KV Store Server v%d.%d.%d", KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR,
           KVSTORE_VERSION_PATCH);

    // Set resource limits
    set_resource_limits();

    // Set up signal handlers
    setup_signals();

    // Set up exit handler
    atexit(cleanup_server);

    // Initialize KV API
    g_server.api = kvapi_init(&g_config.kv_config);
    if (!g_server.api) {
        kv_log(LOG_ERROR, "Failed to initialize KV API");
        return 1;
    }

    // Create server socket
    g_server.listen_fd = setup_server_socket();
    if (g_server.listen_fd < 0) {
        return 1;
    }

    // Set up epoll
    g_server.epoll_fd = setup_epoll();
    if (g_server.epoll_fd < 0) {
        return 1;
    }

    // Run the server
    int result = run_server();

    kv_log(LOG_INFO, "Server exiting");
    return result;
}
