#include "kvstore.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

// Internal store structure with hash table
struct kvstore {
    kvstore_entry_t** buckets;  // Array of bucket heads
    size_t bucket_count;        // Number of buckets
    size_t entry_count;         // Total number of entries
    double max_load_factor;     // Resize threshold
};

// Hash function using FNV-1a algorithm
static uint32_t hash_key(const char* data, uint32_t len) {
    static const uint32_t FNV_PRIME        = 0x01000193;
    static const uint32_t FNV_OFFSET_BASIS = 0x811c9dc5;

    uint32_t hash = FNV_OFFSET_BASIS;
    for (uint32_t i = 0; i < len; i++) {
        hash ^= (unsigned char)data[i];
        hash *= FNV_PRIME;
    }
    return hash;
}

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

// Hash table entry management
static kvstore_entry_t* create_entry(const char* key_data, uint32_t key_len, const char* value_data,
                                     uint32_t value_len, uint32_t hash) {
    kvstore_entry_t* entry = malloc(sizeof(kvstore_entry_t));
    if (!entry) return NULL;

    entry->key = kvstore_string_create(key_data, key_len);
    if (!entry->key.data) {
        free(entry);
        return NULL;
    }

    entry->value = kvstore_string_create(value_data, value_len);
    if (!entry->value.data && value_len > 0) {
        kvstore_string_destroy(&entry->key);
        free(entry);
        return NULL;
    }

    entry->hash = hash;
    entry->next = NULL;
    return entry;
}

static void destroy_entry(kvstore_entry_t* entry) {
    if (!entry) return;
    kvstore_string_destroy(&entry->key);
    kvstore_string_destroy(&entry->value);
    free(entry);
}

// Find the next power of 2 greater than or equal to n
static size_t next_power_of_2(size_t n) {
    if (n <= KVSTORE_MIN_CAPACITY) return KVSTORE_MIN_CAPACITY;

    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if (sizeof(size_t) > 4) {
        n |= n >> 32;
    }
    return n + 1;
}

// Resize hash table
static kvstore_error_t resize_hash_table(kvstore_t* store, size_t new_bucket_count) {
    kvstore_entry_t** new_buckets = calloc(new_bucket_count, sizeof(kvstore_entry_t*));
    if (!new_buckets) return KVSTORE_ERROR_MEMORY;

    // Rehash all entries
    for (size_t i = 0; i < store->bucket_count; i++) {
        kvstore_entry_t* entry = store->buckets[i];
        while (entry) {
            kvstore_entry_t* next = entry->next;
            size_t new_index      = entry->hash % new_bucket_count;

            entry->next            = new_buckets[new_index];
            new_buckets[new_index] = entry;

            entry = next;
        }
    }

    free(store->buckets);
    store->buckets      = new_buckets;
    store->bucket_count = new_bucket_count;
    return KVSTORE_OK;
}

// Core API implementation
kvstore_t* kvstore_create(size_t capacity) {
    if (capacity == 0) capacity = KVSTORE_DEFAULT_CAPACITY;

    size_t bucket_count = next_power_of_2(capacity);

    kvstore_t* store = malloc(sizeof(kvstore_t));
    if (!store) return NULL;

    store->buckets = calloc(bucket_count, sizeof(kvstore_entry_t*));
    if (!store->buckets) {
        free(store);
        return NULL;
    }

    store->bucket_count    = bucket_count;
    store->entry_count     = 0;
    store->max_load_factor = KVSTORE_DEFAULT_LOAD_FACTOR;
    return store;
}

void kvstore_destroy(kvstore_t* store) {
    if (!store) return;

    kvstore_clear(store);
    free(store->buckets);
    free(store);
}

kvstore_error_t kvstore_clear(kvstore_t* store) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;

    for (size_t i = 0; i < store->bucket_count; i++) {
        kvstore_entry_t* entry = store->buckets[i];
        while (entry) {
            kvstore_entry_t* next = entry->next;
            destroy_entry(entry);
            entry = next;
        }
        store->buckets[i] = NULL;
    }

    store->entry_count = 0;
    return KVSTORE_OK;
}

size_t kvstore_size(const kvstore_t* store) {
    return store ? store->entry_count : 0;
}

size_t kvstore_capacity(const kvstore_t* store) {
    return store ? store->bucket_count : 0;
}

double kvstore_load_factor(const kvstore_t* store) {
    if (!store || store->bucket_count == 0) return 0.0;
    return (double)store->entry_count / (double)store->bucket_count;
}

static kvstore_entry_t* find_entry(const kvstore_t* store, const char* key_data, uint32_t key_len,
                                   uint32_t hash) {
    if (!store || !key_data || key_len == 0) return NULL;

    size_t index           = hash % store->bucket_count;
    kvstore_entry_t* entry = store->buckets[index];

    while (entry) {
        if (entry->hash == hash && entry->key.len == key_len &&
            memcmp(entry->key.data, key_data, key_len) == 0) {
            return entry;
        }
        entry = entry->next;
    }
    return NULL;
}

kvstore_error_t kvstore_put(kvstore_t* store, const char* key_data, uint32_t key_len, const char* value_data,
                            uint32_t value_len) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;
    if (key_len > KVSTORE_MAX_STRING_SIZE || value_len > KVSTORE_MAX_STRING_SIZE) {
        return KVSTORE_ERROR_STRING_TOO_LARGE;
    }

    uint32_t hash             = hash_key(key_data, key_len);
    kvstore_entry_t* existing = find_entry(store, key_data, key_len, hash);

    if (existing) {
        // Update existing entry
        kvstore_string_destroy(&existing->value);
        existing->value = kvstore_string_create(value_data, value_len);
        return existing->value.data || value_len == 0 ? KVSTORE_OK : KVSTORE_ERROR_MEMORY;
    }

    // Check if we need to resize
    if (kvstore_load_factor(store) >= store->max_load_factor) {
        kvstore_error_t resize_result = resize_hash_table(store, store->bucket_count * 2);
        if (resize_result != KVSTORE_OK) return resize_result;
    }

    // Create new entry
    kvstore_entry_t* new_entry = create_entry(key_data, key_len, value_data, value_len, hash);
    if (!new_entry) return KVSTORE_ERROR_MEMORY;

    // Insert at head of bucket
    size_t index          = hash % store->bucket_count;
    new_entry->next       = store->buckets[index];
    store->buckets[index] = new_entry;
    store->entry_count++;

    return KVSTORE_OK;
}

kvstore_error_t kvstore_get(const kvstore_t* store, const char* key_data, uint32_t key_len,
                            const kvstore_string_t** value) {
    if (!store || !value) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;

    uint32_t hash          = hash_key(key_data, key_len);
    kvstore_entry_t* entry = find_entry(store, key_data, key_len, hash);

    if (!entry) return KVSTORE_ERROR_KEY_NOT_FOUND;

    *value = &entry->value;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_delete(kvstore_t* store, const char* key_data, uint32_t key_len) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;

    uint32_t hash = hash_key(key_data, key_len);
    size_t index  = hash % store->bucket_count;

    kvstore_entry_t* entry = store->buckets[index];
    kvstore_entry_t* prev  = NULL;

    while (entry) {
        if (entry->hash == hash && entry->key.len == key_len &&
            memcmp(entry->key.data, key_data, key_len) == 0) {

            // Remove from chain
            if (prev) {
                prev->next = entry->next;
            } else {
                store->buckets[index] = entry->next;
            }

            destroy_entry(entry);
            store->entry_count--;
            return KVSTORE_OK;
        }
        prev  = entry;
        entry = entry->next;
    }

    return KVSTORE_ERROR_KEY_NOT_FOUND;
}

bool kvstore_exists(const kvstore_t* store, const char* key_data, uint32_t key_len) {
    if (!store || !key_data || key_len == 0) return false;

    uint32_t hash = hash_key(key_data, key_len);
    return find_entry(store, key_data, key_len, hash) != NULL;
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

static kvstore_string_t read_string_from_fd(int fd) {
    kvstore_string_t str = {0};
    uint32_t net_len;

    if (read_full(fd, &net_len, sizeof(net_len)) != sizeof(net_len)) {
        return str;
    }

    uint32_t len = ntohl_portable(net_len);

    if (len == 0) {
        str.data = malloc(1);
        if (str.data) str.data[0] = '\0';
        str.len = 0;
        return str;
    }

    str.data = malloc(len + 1);
    if (!str.data) return str;

    if (read_full(fd, str.data, len) != (ssize_t)len) {
        free(str.data);
        str.data = NULL;
        str.len  = 0;
        return str;
    }

    str.data[len] = '\0';
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
    uint32_t net_count = htonl_portable(store->entry_count);
    if (write_full(fd, &net_count, sizeof(net_count)) != sizeof(net_count)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    // Write all entries
    for (size_t i = 0; i < store->bucket_count; i++) {
        kvstore_entry_t* entry = store->buckets[i];
        while (entry) {
            if (!write_string_to_fd(fd, &entry->key) || !write_string_to_fd(fd, &entry->value)) {
                close(fd);
                return KVSTORE_ERROR_IO;
            }
            entry = entry->next;
        }
    }

    if (close(fd) == -1) return KVSTORE_ERROR_IO;
    return KVSTORE_OK;
}

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

    // Read entries
    for (uint32_t i = 0; i < count; i++) {
        kvstore_string_t key = read_string_from_fd(fd);
        if (!key.data && key.len > 0) {
            close(fd);
            return KVSTORE_ERROR_MEMORY;
        }

        kvstore_string_t value = read_string_from_fd(fd);
        if (!value.data && value.len > 0) {
            kvstore_string_destroy(&key);
            close(fd);
            return KVSTORE_ERROR_MEMORY;
        }

        // Insert into hash table
        kvstore_error_t result = kvstore_put(store, key.data, key.len, value.data, value.len);

        kvstore_string_destroy(&key);
        kvstore_string_destroy(&value);

        if (result != KVSTORE_OK) {
            close(fd);
            return result;
        }
    }

    close(fd);
    return KVSTORE_OK;
}

// Iterator implementation
kvstore_iterator_t kvstore_iter_begin(const kvstore_t* store) {
    kvstore_iterator_t iter = {.store = store, .bucket_index = 0, .current_entry = NULL};

    if (!store) return iter;

    // Find first non-empty bucket
    for (size_t i = 0; i < store->bucket_count; i++) {
        if (store->buckets[i]) {
            iter.bucket_index  = i;
            iter.current_entry = store->buckets[i];
            break;
        }
    }

    return iter;
}

bool kvstore_iter_valid(const kvstore_iterator_t* iter) {
    return iter && iter->store && iter->current_entry;
}

void kvstore_iter_next(kvstore_iterator_t* iter) {
    if (!kvstore_iter_valid(iter)) return;

    // Try next entry in current bucket
    if (iter->current_entry->next) {
        iter->current_entry = iter->current_entry->next;
        return;
    }

    // Find next non-empty bucket
    for (size_t i = iter->bucket_index + 1; i < iter->store->bucket_count; i++) {
        if (iter->store->buckets[i]) {
            iter->bucket_index  = i;
            iter->current_entry = iter->store->buckets[i];
            return;
        }
    }

    // End of iteration
    iter->current_entry = NULL;
}

const kvstore_entry_t* kvstore_iter_get(const kvstore_iterator_t* iter) {
    return kvstore_iter_valid(iter) ? iter->current_entry : NULL;
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

    printf("Hash Table Statistics:\n");
    printf("  Entries: %zu\n", store->entry_count);
    printf("  Buckets: %zu\n", store->bucket_count);
    printf("  Load factor: %.3f\n", kvstore_load_factor(store));
    printf("  Max load factor: %.3f\n", store->max_load_factor);

    // Chain length statistics
    size_t empty_buckets      = 0;
    size_t max_chain_length   = 0;
    size_t total_chain_length = 0;

    for (size_t i = 0; i < store->bucket_count; i++) {
        size_t chain_length    = 0;
        kvstore_entry_t* entry = store->buckets[i];

        if (!entry) {
            empty_buckets++;
        } else {
            while (entry) {
                chain_length++;
                entry = entry->next;
            }
            if (chain_length > max_chain_length) {
                max_chain_length = chain_length;
            }
            total_chain_length += chain_length;
        }
    }

    printf("  Empty buckets: %zu (%.1f%%)\n", empty_buckets, (empty_buckets * 100.0) / store->bucket_count);
    printf("  Max chain length: %zu\n", max_chain_length);
    if (store->bucket_count > empty_buckets) {
        printf("  Avg chain length: %.2f\n",
               (double)total_chain_length / (store->bucket_count - empty_buckets));
    }
}

void kvstore_print_all(const kvstore_t* store) {
    if (!store) {
        printf("Store: NULL\n");
        return;
    }

    printf("Key-Value Store Contents (%zu entries):\n", store->entry_count);

    kvstore_iterator_t iter = kvstore_iter_begin(store);
    size_t index            = 0;

    while (kvstore_iter_valid(&iter)) {
        const kvstore_entry_t* entry = kvstore_iter_get(&iter);
        printf("[%zu] '%.*s' -> '%.*s' (hash: 0x%08x)\n", index, (int)entry->key.len, entry->key.data,
               (int)entry->value.len, entry->value.data, entry->hash);
        kvstore_iter_next(&iter);
        index++;
    }
}
