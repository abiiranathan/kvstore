#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "kv_client.h"

#define READ_BUFFER_SIZE    (16 * 1024)
#define ERROR_BUFFER_SIZE   256
#define CONNECT_TIMEOUT_SEC 5

// The internal client handle structure
struct kv_client_t {
    int fd;
    char error_str[ERROR_BUFFER_SIZE];
    char read_buffer[READ_BUFFER_SIZE];
    size_t read_pos;
    size_t read_len;
};

// Internal forward declarations
static kv_response_t* parse_response(kv_client_t* client);

// Set a formatted error message on the client handle
static void set_error(kv_client_t* client, const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(client->error_str, ERROR_BUFFER_SIZE, fmt, ap);
    va_end(ap);
}

kv_client_t* kv_connect(const char* host, int port) {
    kv_client_t* client = calloc(1, sizeof(kv_client_t));
    if (!client) return NULL;

    struct hostent* he = gethostbyname(host);
    if (!he) {
        set_error(client, "gethostbyname failed: %s", hstrerror(h_errno));
        free(client);
        return NULL;
    }

    client->fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client->fd == -1) {
        set_error(client, "socket failed: %s", strerror(errno));
        free(client);
        return NULL;
    }

    struct sockaddr_in server_addr = {0};
    server_addr.sin_family         = AF_INET;
    server_addr.sin_port           = htons((uint16_t)port);
    memcpy(&server_addr.sin_addr, he->h_addr_list[0], (size_t)he->h_length);

    // Set connection timeout
    struct timeval tv;
    tv.tv_sec  = CONNECT_TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(client->fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(client->fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Disable Nagle's algorithm for low latency
    int opt = 1;
    setsockopt(client->fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    if (connect(client->fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
        set_error(client, "connect to %s:%d failed: %s", host, port, strerror(errno));
        close(client->fd);
        free(client);
        return NULL;
    }

    return client;
}

void kv_disconnect(kv_client_t* client) {
    if (!client) return;
    if (client->fd >= 0) {
        close(client->fd);
    }
    free(client);
}

const char* kv_get_last_error(const kv_client_t* client) {
    return client ? client->error_str : "Invalid client handle.";
}

// Fills the internal buffer by reading from the socket
static int fill_buffer(kv_client_t* client) {
    if (client->read_pos > 0) {
        if (client->read_len > client->read_pos) {
            memmove(client->read_buffer, client->read_buffer + client->read_pos,
                    client->read_len - client->read_pos);
        }
        client->read_len -= client->read_pos;
        client->read_pos = 0;
    }

    if (client->read_len >= READ_BUFFER_SIZE) {
        set_error(client, "Read buffer is full, cannot read more data");
        return -1;
    }

    ssize_t bytes_read =
        read(client->fd, client->read_buffer + client->read_len, READ_BUFFER_SIZE - client->read_len);

    if (bytes_read > 0) {
        client->read_len += (size_t)bytes_read;
        return 0;
    } else if (bytes_read == 0) {
        set_error(client, "Server closed the connection");
        return -1;
    } else {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            set_error(client, "Read timeout");
        } else {
            set_error(client, "Read failed: %s", strerror(errno));
        }
        return -1;
    }
}

// Reads a single line (\r\n terminated) from the buffer
static int read_line(kv_client_t* client, char* buf, size_t size) {
    while (1) {
        char* newline =
            memmem(client->read_buffer + client->read_pos, client->read_len - client->read_pos, "\r\n", 2);
        if (newline) {
            size_t line_len = (size_t)(newline - (client->read_buffer + client->read_pos));
            if (line_len >= size) {
                set_error(client, "Response line too long for buffer");
                return -1;
            }
            memcpy(buf, client->read_buffer + client->read_pos, line_len);
            buf[line_len] = '\0';
            client->read_pos += line_len + 2;  // +2 for \r\n
            return 0;
        }

        // Line not found, need more data
        if (fill_buffer(client) != 0) {
            return -1;
        }
    }
}

// Reads a raw block of data of a specific length
static int read_raw(kv_client_t* client, char* buf, size_t len) {
    while (client->read_len - client->read_pos < len) {
        if (fill_buffer(client) != 0) {
            return -1;
        }
    }
    memcpy(buf, client->read_buffer + client->read_pos, len);
    client->read_pos += len;
    return 0;
}

// Allocates and parses a single response from the server's stream
static kv_response_t* parse_response(kv_client_t* client) {
    char line[128];
    if (read_line(client, line, sizeof(line)) != 0) {
        return NULL;
    }

    kv_response_t* resp = calloc(1, sizeof(kv_response_t));
    if (!resp) {
        set_error(client, "Memory allocation failed for response");
        return NULL;
    }

    switch (line[0]) {
        case '+':  // Simple String
            resp->type      = KV_RESPONSE_STRING;
            resp->value.str = strdup(line + 1);
            break;

        case '-':  // Error
            resp->type      = KV_RESPONSE_ERROR;
            resp->value.str = strdup(line + 1);
            break;

        case ':':  // Integer
            resp->type          = KV_RESPONSE_INTEGER;
            resp->value.integer = atoll(line + 1);
            break;

        case '$': {  // Bulk String
            long long len = atoll(line + 1);
            if (len == -1) {
                resp->type = KV_RESPONSE_NIL;
            } else if (len >= 0) {
                resp->type      = KV_RESPONSE_STRING;
                resp->value.str = malloc((size_t)len + 1);
                if (!resp->value.str) {
                    set_error(client, "Memory allocation failed for bulk string");
                    free(resp);
                    return NULL;
                }
                if (read_raw(client, resp->value.str, (size_t)len + 2) != 0) {
                    free(resp->value.str);
                    free(resp);
                    return NULL;
                }
                resp->value.str[len] = '\0';  // Null terminate
            } else {
                goto protocol_error;
            }
            break;
        }

        case '*': {  // Array
            long long count = atoll(line + 1);
            if (count == -1) {
                resp->type = KV_RESPONSE_NIL;
            } else if (count >= 0) {
                resp->type              = KV_RESPONSE_ARRAY;
                resp->value.array.count = (size_t)count;
                if (count > 0) {
                    resp->value.array.elements = calloc((size_t)count, sizeof(kv_response_t*));
                    if (!resp->value.array.elements) {
                        set_error(client, "Memory allocation failed for array elements");
                        free(resp);
                        return NULL;
                    }
                    for (size_t i = 0; i < (size_t)count; i++) {
                        resp->value.array.elements[i] = parse_response(client);
                        if (!resp->value.array.elements[i]) {
                            // Clean up partially created array
                            for (size_t j = 0; j < i; j++) {
                                kv_free_response(resp->value.array.elements[j]);
                            }
                            free(resp->value.array.elements);
                            free(resp);
                            return NULL;
                        }
                    }
                }
            } else {
                goto protocol_error;
            }
            break;
        }

        default:
        protocol_error:
            set_error(client, "Protocol error: Unexpected response start '%c'", line[0]);
            free(resp);
            return NULL;
    }
    return resp;
}

kv_response_t* kv_command(kv_client_t* client, const char* cmd) {
    if (!client || client->fd < 0) {
        if (client) set_error(client, "Client is not connected");
        return NULL;
    }

    // Format the command with CRLF
    size_t cmd_len = strlen(cmd);
    char* full_cmd = malloc(cmd_len + 3);  // +2 for \r\n, +1 for \0
    if (!full_cmd) {
        set_error(client, "Failed to allocate memory for command buffer");
        return NULL;
    }
    memcpy(full_cmd, cmd, cmd_len);
    full_cmd[cmd_len]     = '\r';
    full_cmd[cmd_len + 1] = '\n';
    full_cmd[cmd_len + 2] = '\0';

    // Send the command
    size_t total_sent = 0;
    while (total_sent < cmd_len + 2) {
        ssize_t sent = write(client->fd, full_cmd + total_sent, (cmd_len + 2) - total_sent);
        if (sent == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                set_error(client, "Write timeout");
            } else {
                set_error(client, "Write failed: %s", strerror(errno));
            }
            free(full_cmd);
            return NULL;
        }
        total_sent += (size_t)sent;
    }
    free(full_cmd);

    // Parse the response
    return parse_response(client);
}

void kv_free_response(kv_response_t* response) {
    if (!response) return;

    switch (response->type) {
        case KV_RESPONSE_STRING:
        case KV_RESPONSE_ERROR:
            free(response->value.str);
            break;
        case KV_RESPONSE_ARRAY:
            if (response->value.array.elements) {
                for (size_t i = 0; i < response->value.array.count; i++) {
                    kv_free_response(response->value.array.elements[i]);
                }
                free(response->value.array.elements);
            }
            break;
        case KV_RESPONSE_NIL:
        case KV_RESPONSE_INTEGER:
            // No dynamic memory to free for these types
            break;
    }
    free(response);
}

void kv_print_response(const kv_response_t* response) {
    if (!response) {
        printf("(null response)\n");
        return;
    }

    switch (response->type) {
        case KV_RESPONSE_NIL:
            printf("(nil)\n");
            break;
        case KV_RESPONSE_INTEGER:
            printf("(integer) %ld\n", response->value.integer);
            break;
        case KV_RESPONSE_STRING:
            printf("\"%s\"\n", response->value.str);
            break;
        case KV_RESPONSE_ERROR:
            printf("(error) %s\n", response->value.str);
            break;
        case KV_RESPONSE_ARRAY:
            if (response->value.array.count == 0) {
                printf("(empty array)\n");
            } else {
                for (size_t i = 0; i < response->value.array.count; i++) {
                    printf("%zu) ", i + 1);
                    kv_print_response(response->value.array.elements[i]);
                }
            }
            break;
    }
}
