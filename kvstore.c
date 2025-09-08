#include "kvstore.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

// Internal store structure
struct kvstore {
    kvstore_pair_t* pairs;
    size_t count;
    size_t capacity;
};

// Network byte order conversion functions
static uint32_t htonl_portable(uint32_t hostlong) {
    static const int endian_test = 1;
    if (*(char*)&endian_test == 1) {  // Little endian
        return ((hostlong & 0xFF) << 24) | ((hostlong & 0xFF00) << 8) | ((hostlong & 0xFF0000) >> 8) |
               ((hostlong & 0xFF000000) >> 24);
    }
    return hostlong;  // Already big endian
}

static uint32_t ntohl_portable(uint32_t netlong) {
    return htonl_portable(netlong);  // Same operation
}

// Helper I/O functions
static ssize_t read_full(int fd, void* buf, size_t size) {
    char* ptr        = (char*)buf;
    size_t remaining = size;

    while (remaining > 0) {
        ssize_t bytes_read = read(fd, ptr, remaining);
        if (bytes_read <= 0) {
            return bytes_read;  // Error or EOF
        }
        ptr += bytes_read;
        remaining -= bytes_read;
    }
    return size;
}

static ssize_t write_full(int fd, const void* buf, size_t size) {
    const char* ptr  = (const char*)buf;
    size_t remaining = size;

    while (remaining > 0) {
        ssize_t bytes_written = write(fd, ptr, remaining);
        if (bytes_written <= 0) {
            return bytes_written;  // Error
        }
        ptr += bytes_written;
        remaining -= bytes_written;
    }
    return size;
}

// String utilities implementation
kvstore_string_t kvstore_string_create(const char* data, uint32_t len) {
    kvstore_string_t s = {0};
    if (!data && len > 0) return s;

    if (len > KVSTORE_MAX_STRING_SIZE) return s;

    if (len == 0) {
        s.data = malloc(1);
        if (s.data) s.data[0] = '\0';
        return s;
    }

    s.data = malloc(len + 1);
    if (!s.data) return s;

    memcpy(s.data, data, len);
    s.data[len] = '\0';  // Null terminate for convenience
    s.len       = len;
    return s;
}

kvstore_string_t kvstore_string_from_cstr(const char* cstr) {
    if (!cstr) return (kvstore_string_t){0};
    return kvstore_string_create(cstr, strlen(cstr));
}

void kvstore_string_destroy(kvstore_string_t* str) {
    if (str && str->data) {
        free(str->data);
        str->data = NULL;
        str->len  = 0;
    }
}

bool kvstore_string_equals(const kvstore_string_t* a, const kvstore_string_t* b) {
    if (!a || !b) return false;
    if (a->len != b->len) return false;
    if (a->len == 0) return true;
    return memcmp(a->data, b->data, a->len) == 0;
}

kvstore_string_t kvstore_string_copy(const kvstore_string_t* src) {
    if (!src) return (kvstore_string_t){0};
    return kvstore_string_create(src->data, src->len);
}

// Core API implementation
kvstore_t* kvstore_create(size_t capacity) {
    if (capacity == 0) capacity = KVSTORE_DEFAULT_CAPACITY;

    kvstore_t* store = malloc(sizeof(kvstore_t));
    if (!store) return NULL;

    store->pairs = calloc(capacity, sizeof(kvstore_pair_t));
    if (!store->pairs) {
        free(store);
        return NULL;
    }

    store->count    = 0;
    store->capacity = capacity;
    return store;
}

void kvstore_destroy(kvstore_t* store) {
    if (!store) return;

    kvstore_clear(store);
    free(store->pairs);
    free(store);
}

kvstore_error_t kvstore_clear(kvstore_t* store) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;

    for (size_t i = 0; i < store->count; i++) {
        kvstore_string_destroy(&store->pairs[i].key);
        kvstore_string_destroy(&store->pairs[i].value);
    }

    store->count = 0;
    return KVSTORE_OK;
}

size_t kvstore_size(const kvstore_t* store) {
    return store ? store->count : 0;
}

size_t kvstore_capacity(const kvstore_t* store) {
    return store ? store->capacity : 0;
}

static ssize_t find_key(const kvstore_t* store, const char* key_data, uint32_t key_len) {
    if (!store || !key_data || key_len == 0) return -1;

    for (size_t i = 0; i < store->count; i++) {
        if (store->pairs[i].key.len == key_len && memcmp(store->pairs[i].key.data, key_data, key_len) == 0) {
            return i;
        }
    }
    return -1;
}

kvstore_error_t kvstore_put(kvstore_t* store, const char* key_data, uint32_t key_len, const char* value_data,
                            uint32_t value_len) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;

    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;
    if (key_len > KVSTORE_MAX_STRING_SIZE || value_len > KVSTORE_MAX_STRING_SIZE) {
        return KVSTORE_ERROR_STRING_TOO_LARGE;
    }

    ssize_t index = find_key(store, key_data, key_len);

    if (index >= 0) {
        // Update existing key
        kvstore_string_destroy(&store->pairs[index].value);
        store->pairs[index].value = kvstore_string_create(value_data, value_len);
        return store->pairs[index].value.data || value_len == 0 ? KVSTORE_OK : KVSTORE_ERROR_MEMORY;
    }

    // Add new key
    if (store->count >= store->capacity) {
        return KVSTORE_ERROR_CAPACITY_FULL;
    }

    kvstore_string_t key = kvstore_string_create(key_data, key_len);
    if (!key.data) return KVSTORE_ERROR_MEMORY;

    kvstore_string_t value = kvstore_string_create(value_data, value_len);
    if (!value.data && value_len > 0) {
        kvstore_string_destroy(&key);
        return KVSTORE_ERROR_MEMORY;
    }

    store->pairs[store->count].key   = key;
    store->pairs[store->count].value = value;
    store->count++;

    return KVSTORE_OK;
}

kvstore_error_t kvstore_get(const kvstore_t* store, const char* key_data, uint32_t key_len,
                            const kvstore_string_t** value) {
    if (!store || !value) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;

    ssize_t index = find_key(store, key_data, key_len);
    if (index < 0) return KVSTORE_ERROR_KEY_NOT_FOUND;

    *value = &store->pairs[index].value;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_delete(kvstore_t* store, const char* key_data, uint32_t key_len) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;

    ssize_t index = find_key(store, key_data, key_len);
    if (index < 0) return KVSTORE_ERROR_KEY_NOT_FOUND;

    // Free the strings
    kvstore_string_destroy(&store->pairs[index].key);
    kvstore_string_destroy(&store->pairs[index].value);

    // Move last element to fill the gap
    if (index < (ssize_t)(store->count - 1)) {
        store->pairs[index] = store->pairs[store->count - 1];
    }

    // Clear the last slot
    memset(&store->pairs[store->count - 1], 0, sizeof(kvstore_pair_t));
    store->count--;

    return KVSTORE_OK;
}

bool kvstore_exists(const kvstore_t* store, const char* key_data, uint32_t key_len) {
    return find_key(store, key_data, key_len) >= 0;
}

// Convenience functions
kvstore_error_t kvstore_put_str(kvstore_t* store, const char* key, const char* value) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    return kvstore_put(store, key, strlen(key), value, value ? strlen(value) : 0);
}

kvstore_error_t kvstore_get_str(const kvstore_t* store, const char* key, const kvstore_string_t** value) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    return kvstore_get(store, key, strlen(key), value);
}

kvstore_error_t kvstore_delete_str(kvstore_t* store, const char* key) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    return kvstore_delete(store, key, strlen(key));
}

bool kvstore_exists_str(const kvstore_t* store, const char* key) {
    if (!key) return false;
    return kvstore_exists(store, key, strlen(key));
}

// Persistence implementation
static bool write_string_to_fd(int fd, const kvstore_string_t* str) {
    if (!str) return false;

    uint32_t net_len = htonl_portable(str->len);

    if (write_full(fd, &net_len, sizeof(net_len)) != sizeof(net_len)) {
        return false;
    }

    if (str->len > 0 && write_full(fd, str->data, str->len) != (ssize_t)str->len) {
        return false;
    }
    return true;
}

// Fixed read_string_from_fd function
static kvstore_string_t read_string_from_fd(int fd) {
    kvstore_string_t str = {0};
    uint32_t net_len;

    // Read length
    if (read_full(fd, &net_len, sizeof(net_len)) != sizeof(net_len)) {
        return str;
    }

    uint32_t len = ntohl_portable(net_len);

    // Handle empty string
    if (len == 0) {
        str.data = malloc(1);
        if (str.data) str.data[0] = '\0';
        str.len = 0;
        return str;
    }

    // Allocate and read data in one go
    str.data = malloc(len + 1);
    if (!str.data) {
        return str;
    }

    // Read the actual string data
    if (read_full(fd, str.data, len) != (ssize_t)len) {
        free(str.data);
        str.data = NULL;
        str.len  = 0;
        return str;
    }

    str.data[len] = '\0';  // Null terminate
    str.len       = len;
    return str;
}

kvstore_error_t kvstore_save(const kvstore_t* store, const char* filename) {
    if (!store || !filename) return KVSTORE_ERROR_NULL_POINTER;

    int fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) return KVSTORE_ERROR_IO;

    // Write magic header
    uint32_t magic = htonl_portable(KVSTORE_MAGIC_HEADER);
    if (write_full(fd, &magic, sizeof(magic)) != sizeof(magic)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    // Write count
    uint32_t net_count = htonl_portable(store->count);
    if (write_full(fd, &net_count, sizeof(net_count)) != sizeof(net_count)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    // Write all pairs
    for (size_t i = 0; i < store->count; i++) {
        if (!write_string_to_fd(fd, &store->pairs[i].key) ||
            !write_string_to_fd(fd, &store->pairs[i].value)) {
            close(fd);
            return KVSTORE_ERROR_IO;
        }
    }

    if (close(fd) == -1) return KVSTORE_ERROR_IO;
    return KVSTORE_OK;
}
// Fixed kvstore_load function
kvstore_error_t kvstore_load(kvstore_t* store, const char* filename) {
    if (!store || !filename) return KVSTORE_ERROR_NULL_POINTER;

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        return errno == ENOENT ? KVSTORE_OK : KVSTORE_ERROR_IO;
    }

    kvstore_clear(store);

    // Read and verify magic header
    uint32_t magic;
    if (read_full(fd, &magic, sizeof(magic)) != sizeof(magic)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    if (ntohl_portable(magic) != KVSTORE_MAGIC_HEADER) {
        close(fd);
        return KVSTORE_ERROR_INVALID_FORMAT;
    }

    // Read count
    uint32_t net_count;
    if (read_full(fd, &net_count, sizeof(net_count)) != sizeof(net_count)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    uint32_t count = ntohl_portable(net_count);
    if (count > store->capacity) {
        close(fd);
        return KVSTORE_ERROR_CAPACITY_FULL;
    }

    // Read pairs
    for (uint32_t i = 0; i < count; i++) {
        kvstore_string_t key = read_string_from_fd(fd);
        if (!key.data && key.len > 0) {  // Allocation failed for non-empty string
            close(fd);
            return KVSTORE_ERROR_MEMORY;
        }

        kvstore_string_t value = read_string_from_fd(fd);
        if (!value.data && value.len > 0) {  // Allocation failed for non-empty string
            kvstore_string_destroy(&key);
            close(fd);
            return KVSTORE_ERROR_MEMORY;
        }

        // Check if we have space
        if (store->count >= store->capacity) {
            kvstore_string_destroy(&key);
            kvstore_string_destroy(&value);
            close(fd);
            return KVSTORE_ERROR_CAPACITY_FULL;
        }

        store->pairs[store->count].key   = key;
        store->pairs[store->count].value = value;
        store->count++;
    }

    close(fd);
    return KVSTORE_OK;
}

// Iterator implementation
kvstore_iterator_t kvstore_iter_begin(const kvstore_t* store) {
    kvstore_iterator_t iter = {.store = store, .index = 0};
    return iter;
}

bool kvstore_iter_valid(const kvstore_iterator_t* iter) {
    return iter && iter->store && iter->index < iter->store->count;
}

void kvstore_iter_next(kvstore_iterator_t* iter) {
    if (iter) iter->index++;
}

const kvstore_pair_t* kvstore_iter_get(const kvstore_iterator_t* iter) {
    if (!kvstore_iter_valid(iter)) return NULL;
    return &iter->store->pairs[iter->index];
}

// Utility functions
const char* kvstore_error_string(kvstore_error_t error) {
    switch (error) {
        case KVSTORE_OK:
            return "Success";
        case KVSTORE_ERROR_NULL_POINTER:
            return "Null pointer";
        case KVSTORE_ERROR_INVALID_KEY:
            return "Invalid key";
        case KVSTORE_ERROR_CAPACITY_FULL:
            return "Capacity full";
        case KVSTORE_ERROR_KEY_NOT_FOUND:
            return "Key not found";
        case KVSTORE_ERROR_MEMORY:
            return "Memory allocation failed";
        case KVSTORE_ERROR_IO:
            return "I/O error";
        case KVSTORE_ERROR_INVALID_FORMAT:
            return "Invalid file format";
        case KVSTORE_ERROR_STRING_TOO_LARGE:
            return "String too large";
        default:
            return "Unknown error";
    }
}

void kvstore_print_stats(const kvstore_t* store) {
    if (!store) {
        printf("Store: NULL\n");
        return;
    }

    printf("Store Statistics:\n");
    printf("  Count: %zu\n", store->count);
    printf("  Capacity: %zu\n", store->capacity);
    printf("  Load factor: %.2f%%\n", store->capacity > 0 ? (store->count * 100.0) / store->capacity : 0.0);
}

void kvstore_print_all(const kvstore_t* store) {
    if (!store) {
        printf("Store: NULL\n");
        return;
    }

    printf("Key-Value Store Contents (%zu pairs):\n", store->count);
    for (size_t i = 0; i < store->count; i++) {
        printf("[%zu] '%.*s' -> '%.*s'\n", i, (int)store->pairs[i].key.len, store->pairs[i].key.data,
               (int)store->pairs[i].value.len, store->pairs[i].value.data);
    }
}
