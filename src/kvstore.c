#include "../include/kvstore.h"
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define MAGIC_NUMBER 0x4B56DB02

// Arena/Memory Pool Configuration
#define ARENA_BLOCK_SIZE (size_t)(64 * 1024)  // 64KB blocks
#define ARENA_ALIGNMENT  (size_t)8            // 8-byte alignment for all allocations

// Arena block structure
typedef struct arena_block {
    struct arena_block* next;
    size_t size;
    size_t used;
    char data[];
} arena_block_t;

// Arena allocator structure
typedef struct arena {
    arena_block_t* blocks;
    arena_block_t* current;
    size_t total_allocated;
    size_t total_used;
} arena_t;

// Internal store structure with hash table and arena allocator
struct kvstore {
    kvstore_entry_t** buckets;
    size_t bucket_count;
    size_t entry_count;
    double max_load_factor;
    arena_t arena;
};

// Arena allocator functions
static size_t align_size(size_t size) {
    return (size + ARENA_ALIGNMENT - 1) & ~(ARENA_ALIGNMENT - 1);
}

static arena_block_t* arena_create_block(size_t min_size) {
    size_t block_size = ARENA_BLOCK_SIZE;
    if (min_size > block_size - sizeof(arena_block_t)) {
        block_size = min_size + sizeof(arena_block_t);
    }

    arena_block_t* block = malloc(block_size);
    if (!block) return NULL;

    block->next = NULL;
    block->size = block_size - sizeof(arena_block_t);
    block->used = 0;
    return block;
}

static void arena_init(arena_t* arena) {
    memset(arena, 0, sizeof(arena_t));
}

static void* arena_alloc(arena_t* arena, size_t size) {
    if (size == 0) return NULL;

    size = align_size(size);

    // Try to allocate from current block
    if (arena->current && arena->current->used + size <= arena->current->size) {
        void* ptr = arena->current->data + arena->current->used;
        arena->current->used += size;
        arena->total_used += size;
        return ptr;
    }

    // Need a new block
    arena_block_t* new_block = arena_create_block(size);
    if (!new_block) return NULL;

    // Link the new block
    if (arena->current) {
        new_block->next = arena->blocks;
        arena->blocks   = arena->current;
    }
    arena->current = new_block;
    arena->total_allocated += new_block->size + sizeof(arena_block_t);

    // Allocate from the new block
    void* ptr       = new_block->data;
    new_block->used = size;
    arena->total_used += size;
    return ptr;
}

static void arena_destroy(arena_t* arena) {
    // Free current block
    if (arena->current) {
        free(arena->current);
        arena->current = NULL;
    }

    // Free all other blocks
    arena_block_t* block = arena->blocks;
    while (block) {
        arena_block_t* next = block->next;
        free(block);
        block = next;
    }

    memset(arena, 0, sizeof(arena_t));
}

static void arena_clear(arena_t* arena) {
    // Reset all blocks to unused state
    if (arena->current) {
        arena->current->used = 0;
    }

    arena_block_t* block = arena->blocks;
    while (block) {
        block->used = 0;
        block       = block->next;
    }

    // Move all blocks back to the main list and use the first one as current
    if (arena->current) {
        arena->current->next = arena->blocks;
        arena->blocks        = arena->current;
        arena->current       = arena->blocks;
        if (arena->current) {
            arena->blocks        = arena->current->next;
            arena->current->next = NULL;
        }
    }

    arena->total_used = 0;
}

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
    if (*(char*)&endian_test == 1) {
        return ((hostlong & 0xFF) << 24) | ((hostlong & 0xFF00) << 8) | ((hostlong & 0xFF0000) >> 8) |
               ((hostlong & 0xFF000000) >> 24);
    }
    return hostlong;
}

static uint32_t ntohl_portable(uint32_t netlong) {
    return htonl_portable(netlong);
}

static uint64_t htonll_portable(uint64_t hostlonglong) {
    static const int endian_test = 1;
    if (*(char*)&endian_test == 1) {                            // Little-endian
        uint32_t low  = (uint32_t)(hostlonglong & 0xFFFFFFFF);  // lower 32 bits, already fits into uint32_t
        uint32_t high = (uint32_t)(hostlonglong >> 32);         // upper 32 bits, may implicitly truncate
        return ((uint64_t)htonl_portable(high) << 32) | htonl_portable(low);
    }
    return hostlonglong;
}

static uint64_t ntohll_portable(uint64_t netlonglong) {
    return htonll_portable(netlonglong);
}

// Helper I/O functions
static ssize_t read_full(int fd, void* buf, size_t size) {
    char* ptr        = (char*)buf;
    size_t remaining = size;

    while (remaining > 0) {
        ssize_t bytes_read = read(fd, ptr, remaining);
        if (bytes_read <= 0) return bytes_read;
        ptr += bytes_read;
        remaining -= (size_t)bytes_read;
    }
    return (ssize_t)size;
}

static ssize_t write_full(int fd, const void* buf, size_t size) {
    const char* ptr  = (const char*)buf;
    size_t remaining = size;

    while (remaining > 0) {
        ssize_t bytes_written = write(fd, ptr, remaining);
        if (bytes_written <= 0) return bytes_written;
        ptr += bytes_written;
        remaining -= (size_t)bytes_written;
    }
    return (ssize_t)size;
}

// String utilities using arena allocator
kvstore_string_t kvstore_string_create(const char* data, uint32_t len) {
    kvstore_string_t s = {0};
    if (!data && len > 0) {
        return s;
    }
    if (len > KVSTORE_MAX_STRING_SIZE) {
        return s;
    }

    s.data = malloc(len + 1);
    if (!s.data) {
        return s;
    }

    if (len > 0) {
        memcpy(s.data, data, len);
    }

    s.data[len] = '\0';
    s.len       = len;
    return s;
}

// Arena-based string creation for internal use
static kvstore_string_t kvstore_string_create_arena(arena_t* arena, const char* data, uint32_t len) {
    kvstore_string_t s = {0};
    if (!arena || (!data && len > 0)) {
        return s;
    }
    if (len > KVSTORE_MAX_STRING_SIZE) {
        return s;
    }

    s.data = arena_alloc(arena, len + 1);
    if (!s.data) {
        return s;
    }

    if (len > 0) {
        memcpy(s.data, data, len);
    }

    s.data[len] = '\0';
    s.len       = len;
    return s;
}

kvstore_string_t kvstore_string_from_cstr(const char* cstr) {
    if (!cstr) return (kvstore_string_t){0};
    return kvstore_string_create(cstr, (uint32_t)strlen(cstr));
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

// Value creation and destruction
kvstore_value_t kvstore_value_create_null(void) {
    return (kvstore_value_t){.type = KVSTORE_TYPE_NULL, .data = {{0}}};
}

kvstore_value_t kvstore_value_create_string(const char* data, uint32_t len) {
    kvstore_value_t val = {.type = KVSTORE_TYPE_STRING, .data = {{0}}};
    val.data.str_val    = kvstore_string_create(data, len);
    if (!val.data.str_val.data && len > 0) {
        val.type = KVSTORE_TYPE_NULL;
    }
    return val;
}

kvstore_value_t kvstore_value_create_int64(int64_t value) {
    return (kvstore_value_t){.type = KVSTORE_TYPE_INT64, .data = {.int_val = value}};
}

kvstore_value_t kvstore_value_create_double(double value) {
    return (kvstore_value_t){.type = KVSTORE_TYPE_DOUBLE, .data = {.double_val = value}};
}

kvstore_value_t kvstore_value_create_bool(bool value) {
    return (kvstore_value_t){.type = KVSTORE_TYPE_BOOL, .data = {.bool_val = value}};
}

kvstore_value_t kvstore_value_create_binary(const void* data, uint32_t len) {
    kvstore_value_t val = {.type = KVSTORE_TYPE_BINARY, .data = {{0}}};
    val.data.binary_val = kvstore_string_create((const char*)data, len);
    if (!val.data.binary_val.data && len > 0) {
        val.type = KVSTORE_TYPE_NULL;
    }
    return val;
}

// Arena-based value creation for internal use
static kvstore_value_t kvstore_value_create_string_arena(arena_t* arena, const char* data, uint32_t len) {
    kvstore_value_t val = {.type = KVSTORE_TYPE_STRING, .data = {{0}}};
    val.data.str_val    = kvstore_string_create_arena(arena, data, len);
    if (!val.data.str_val.data && len > 0) {
        val.type = KVSTORE_TYPE_NULL;
    }
    return val;
}

static kvstore_value_t kvstore_value_create_binary_arena(arena_t* arena, const void* data, uint32_t len) {
    kvstore_value_t val = {.type = KVSTORE_TYPE_BINARY, .data = {{0}}};
    val.data.binary_val = kvstore_string_create_arena(arena, (const char*)data, len);
    if (!val.data.binary_val.data && len > 0) {
        val.type = KVSTORE_TYPE_NULL;
    }
    return val;
}

void kvstore_value_destroy(kvstore_value_t* value) {
    if (!value) return;

    switch (value->type) {
        case KVSTORE_TYPE_STRING:
            kvstore_string_destroy(&value->data.str_val);
            break;
        case KVSTORE_TYPE_BINARY:
            kvstore_string_destroy(&value->data.binary_val);
            break;
        default:
            break;
    }

    value->type = KVSTORE_TYPE_NULL;
    memset(&value->data, 0, sizeof(value->data));
}

static kvstore_value_t kvstore_value_copy_arena(arena_t* arena, const kvstore_value_t* src) {
    if (!src) return kvstore_value_create_null();

    switch (src->type) {
        case KVSTORE_TYPE_NULL:
            return kvstore_value_create_null();
        case KVSTORE_TYPE_STRING:
            return kvstore_value_create_string_arena(arena, src->data.str_val.data, src->data.str_val.len);
        case KVSTORE_TYPE_INT64:
            return kvstore_value_create_int64(src->data.int_val);
        case KVSTORE_TYPE_DOUBLE:
            return kvstore_value_create_double(src->data.double_val);
        case KVSTORE_TYPE_BOOL:
            return kvstore_value_create_bool(src->data.bool_val);
        case KVSTORE_TYPE_BINARY:
            return kvstore_value_create_binary_arena(arena, src->data.binary_val.data,
                                                     src->data.binary_val.len);
        default:
            return kvstore_value_create_null();
    }
}

kvstore_value_t kvstore_value_copy(const kvstore_value_t* src) {
    if (!src) return kvstore_value_create_null();

    switch (src->type) {
        case KVSTORE_TYPE_NULL:
            return kvstore_value_create_null();
        case KVSTORE_TYPE_STRING:
            return kvstore_value_create_string(src->data.str_val.data, src->data.str_val.len);
        case KVSTORE_TYPE_INT64:
            return kvstore_value_create_int64(src->data.int_val);
        case KVSTORE_TYPE_DOUBLE:
            return kvstore_value_create_double(src->data.double_val);
        case KVSTORE_TYPE_BOOL:
            return kvstore_value_create_bool(src->data.bool_val);
        case KVSTORE_TYPE_BINARY:
            return kvstore_value_create_binary(src->data.binary_val.data, src->data.binary_val.len);
        default:
            return kvstore_value_create_null();
    }
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

// Hash table entry management using arena allocator
static kvstore_entry_t* create_entry(arena_t* arena, const char* key_data, uint32_t key_len,
                                     const kvstore_value_t* value, uint32_t hash) {
    kvstore_entry_t* entry = arena_alloc(arena, sizeof(kvstore_entry_t));
    if (!entry) return NULL;

    entry->key = kvstore_string_create_arena(arena, key_data, key_len);
    if (!entry->key.data && key_len > 0) {
        return NULL;
    }

    entry->value = kvstore_value_copy_arena(arena, value);
    entry->hash  = hash;
    entry->next  = NULL;
    return entry;
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

// Core API implementation
kvstore_t* kvstore_create(size_t capacity) {
    if (capacity == 0) capacity = KVSTORE_DEFAULT_CAPACITY;
    if (capacity > SIZE_MAX / 2) return NULL;

    size_t bucket_count = next_power_of_2(capacity);

    kvstore_t* store = malloc(sizeof(kvstore_t));
    if (!store) return NULL;

    store->buckets = calloc(bucket_count, sizeof(kvstore_entry_t*));
    if (!store->buckets) {
        free(store);
        return NULL;
    }

    arena_init(&store->arena);
    store->bucket_count    = bucket_count;
    store->entry_count     = 0;
    store->max_load_factor = KVSTORE_DEFAULT_LOAD_FACTOR;

    return store;
}

void kvstore_destroy(kvstore_t* store) {
    if (!store) return;

    arena_destroy(&store->arena);
    free(store->buckets);
    free(store);
}

kvstore_error_t kvstore_clear(kvstore_t* store) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;

    // Clear the arena (this effectively frees all entries)
    arena_clear(&store->arena);

    // Clear bucket pointers
    memset(store->buckets, 0, store->bucket_count * sizeof(kvstore_entry_t*));
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

kvstore_error_t kvstore_put_value(kvstore_t* store, const char* key_data, uint32_t key_len,
                                  const kvstore_value_t* value) {
    if (!store || !value) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    uint32_t hash             = hash_key(key_data, key_len);
    kvstore_entry_t* existing = find_entry(store, key_data, key_len, hash);

    if (existing) {
        // For updates, we need to replace the value but can't free the old one
        // since it's in the arena. We'll just overwrite it.
        existing->value = kvstore_value_copy_arena(&store->arena, value);
        return KVSTORE_OK;
    }

    if (kvstore_load_factor(store) >= store->max_load_factor) {
        kvstore_error_t resize_result = resize_hash_table(store, store->bucket_count * 2);
        if (resize_result != KVSTORE_OK) return resize_result;
    }

    kvstore_entry_t* new_entry = create_entry(&store->arena, key_data, key_len, value, hash);
    if (!new_entry) return KVSTORE_ERROR_MEMORY;

    size_t index          = hash % store->bucket_count;
    new_entry->next       = store->buckets[index];
    store->buckets[index] = new_entry;
    store->entry_count++;

    return KVSTORE_OK;
}

kvstore_error_t kvstore_get_value(const kvstore_t* store, const char* key_data, uint32_t key_len,
                                  const kvstore_value_t** value) {
    if (!store || !value) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    *value                 = NULL;
    uint32_t hash          = hash_key(key_data, key_len);
    kvstore_entry_t* entry = find_entry(store, key_data, key_len, hash);
    if (!entry) return KVSTORE_ERROR_KEY_NOT_FOUND;

    *value = &entry->value;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_delete_key(kvstore_t* store, const char* key_data, uint32_t key_len) {
    if (!store) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    uint32_t hash              = hash_key(key_data, key_len);
    size_t index               = hash % store->bucket_count;
    kvstore_entry_t** head_ptr = &store->buckets[index];
    kvstore_entry_t* entry     = store->buckets[index];
    kvstore_entry_t* prev      = NULL;

    while (entry) {
        if (entry->hash == hash && entry->key.len == key_len &&
            memcmp(entry->key.data, key_data, key_len) == 0) {
            if (prev) {
                prev->next = entry->next;
            } else {
                *head_ptr = entry->next;
            }
            // Note: We don't free the entry since it's in the arena
            // The memory will be reclaimed when the arena is cleared/destroyed
            store->entry_count--;
            return KVSTORE_OK;
        }
        prev  = entry;
        entry = entry->next;
    }
    return KVSTORE_ERROR_KEY_NOT_FOUND;
}

bool kvstore_exists_key(const kvstore_t* store, const char* key_data, uint32_t key_len) {
    if (!store || !key_data || key_len == 0 || key_len > KVSTORE_MAX_STRING_SIZE) return false;

    uint32_t hash = hash_key(key_data, key_len);
    return find_entry(store, key_data, key_len, hash) != NULL;
}

kvstore_error_t kvstore_get_type(const kvstore_t* store, const char* key_data, uint32_t key_len,
                                 kvstore_type_t* type) {
    if (!store || !type) return KVSTORE_ERROR_NULL_POINTER;
    if (!key_data || key_len == 0) return KVSTORE_ERROR_INVALID_KEY;
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    *type                  = KVSTORE_TYPE_NULL;
    uint32_t hash          = hash_key(key_data, key_len);
    kvstore_entry_t* entry = find_entry(store, key_data, key_len, hash);
    if (!entry) return KVSTORE_ERROR_KEY_NOT_FOUND;

    *type = entry->value.type;
    return KVSTORE_OK;
}

// Type-specific put operations
kvstore_error_t kvstore_put_null(kvstore_t* store, const char* key) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;
    kvstore_value_t val = kvstore_value_create_null();
    return kvstore_put_value(store, key, (uint32_t)key_len, &val);
}

kvstore_error_t kvstore_put_string(kvstore_t* store, const char* key, const char* value) {
    if (!key || !value) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len   = strlen(key);
    size_t value_len = strlen(value);
    if (key_len > KVSTORE_MAX_STRING_SIZE || value_len > KVSTORE_MAX_STRING_SIZE) {
        return KVSTORE_ERROR_STRING_TOO_LARGE;
    }
    kvstore_value_t val    = kvstore_value_create_string(value, (uint32_t)value_len);
    kvstore_error_t result = kvstore_put_value(store, key, (uint32_t)key_len, &val);
    kvstore_value_destroy(&val);
    return result;
}

kvstore_error_t kvstore_put_string_len(kvstore_t* store, const char* key, const char* value, uint32_t len) {
    if (!store || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE || len > KVSTORE_MAX_STRING_SIZE) {
        return KVSTORE_ERROR_STRING_TOO_LARGE;
    }
    kvstore_value_t val    = kvstore_value_create_string(value, len);
    kvstore_error_t result = kvstore_put_value(store, key, (uint32_t)key_len, &val);
    kvstore_value_destroy(&val);
    return result;
}

kvstore_error_t kvstore_put_int64(kvstore_t* store, const char* key, int64_t value) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;
    kvstore_value_t val = kvstore_value_create_int64(value);
    return kvstore_put_value(store, key, (uint32_t)key_len, &val);
}

kvstore_error_t kvstore_put_double(kvstore_t* store, const char* key, double value) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;
    kvstore_value_t val = kvstore_value_create_double(value);
    return kvstore_put_value(store, key, (uint32_t)key_len, &val);
}

kvstore_error_t kvstore_put_bool(kvstore_t* store, const char* key, bool value) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;
    kvstore_value_t val = kvstore_value_create_bool(value);
    return kvstore_put_value(store, key, (uint32_t)key_len, &val);
}

kvstore_error_t kvstore_put_binary(kvstore_t* store, const char* key, const void* data, uint32_t len) {
    if (!key || !data) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE || len > KVSTORE_MAX_STRING_SIZE) {
        return KVSTORE_ERROR_STRING_TOO_LARGE;
    }
    kvstore_value_t val    = kvstore_value_create_binary(data, len);
    kvstore_error_t result = kvstore_put_value(store, key, (uint32_t)key_len, &val);
    kvstore_value_destroy(&val);
    return result;
}

// Type-specific get operations
kvstore_error_t kvstore_get_string(const kvstore_t* store, const char* key, const kvstore_string_t** value) {
    if (!store || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    const kvstore_value_t* val;
    kvstore_error_t err = kvstore_get_value(store, key, (uint32_t)key_len, &val);
    if (err != KVSTORE_OK) return err;
    if (val->type != KVSTORE_TYPE_STRING) return KVSTORE_ERROR_TYPE_MISMATCH;

    *value = &val->data.str_val;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_get_int64(const kvstore_t* store, const char* key, int64_t* value) {
    if (!store || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    const kvstore_value_t* val;
    kvstore_error_t err = kvstore_get_value(store, key, (uint32_t)key_len, &val);
    if (err != KVSTORE_OK) return err;
    if (val->type != KVSTORE_TYPE_INT64) return KVSTORE_ERROR_TYPE_MISMATCH;

    *value = val->data.int_val;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_get_double(const kvstore_t* store, const char* key, double* value) {
    if (!store || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    const kvstore_value_t* val;
    kvstore_error_t err = kvstore_get_value(store, key, (uint32_t)key_len, &val);
    if (err != KVSTORE_OK) return err;
    if (val->type != KVSTORE_TYPE_DOUBLE) return KVSTORE_ERROR_TYPE_MISMATCH;

    *value = val->data.double_val;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_get_bool(const kvstore_t* store, const char* key, bool* value) {
    if (!store || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    const kvstore_value_t* val;
    kvstore_error_t err = kvstore_get_value(store, key, (uint32_t)key_len, &val);
    if (err != KVSTORE_OK) return err;
    if (val->type != KVSTORE_TYPE_BOOL) return KVSTORE_ERROR_TYPE_MISMATCH;

    *value = val->data.bool_val;
    return KVSTORE_OK;
}

kvstore_error_t kvstore_get_binary(const kvstore_t* store, const char* key, const kvstore_string_t** data) {
    if (!store || !key || !data) return KVSTORE_ERROR_NULL_POINTER;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;

    const kvstore_value_t* val;
    kvstore_error_t err = kvstore_get_value(store, key, (uint32_t)key_len, &val);
    if (err != KVSTORE_OK) return err;
    if (val->type != KVSTORE_TYPE_BINARY) return KVSTORE_ERROR_TYPE_MISMATCH;

    *data = &val->data.binary_val;
    return KVSTORE_OK;
}

// Legacy string-only operations
kvstore_error_t kvstore_put_str(kvstore_t* store, const char* key, const char* value) {
    return kvstore_put_string(store, key, value);
}

kvstore_error_t kvstore_get_str(const kvstore_t* store, const char* key, const kvstore_string_t** value) {
    return kvstore_get_string(store, key, value);
}

kvstore_error_t kvstore_delete_str(kvstore_t* store, const char* key) {
    if (!key) return KVSTORE_ERROR_INVALID_KEY;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return KVSTORE_ERROR_STRING_TOO_LARGE;
    return kvstore_delete_key(store, key, (uint32_t)key_len);
}

bool kvstore_exists_str(const kvstore_t* store, const char* key) {
    if (!key) return false;
    size_t key_len = strlen(key);
    if (key_len > KVSTORE_MAX_STRING_SIZE) return false;
    return kvstore_exists_key(store, key, (uint32_t)key_len);
}

// Persistence API
static kvstore_error_t write_value(int fd, const kvstore_value_t* value) {
    uint8_t type_byte = (uint8_t)value->type;
    if (write_full(fd, &type_byte, sizeof(uint8_t)) != sizeof(uint8_t)) return KVSTORE_ERROR_IO;

    switch (value->type) {
        case KVSTORE_TYPE_NULL:
            return KVSTORE_OK;
        case KVSTORE_TYPE_STRING:
        case KVSTORE_TYPE_BINARY: {
            const kvstore_string_t* s =
                value->type == KVSTORE_TYPE_STRING ? &value->data.str_val : &value->data.binary_val;
            uint32_t len_n = htonl_portable(s->len);
            if (write_full(fd, &len_n, sizeof(uint32_t)) != sizeof(uint32_t)) return KVSTORE_ERROR_IO;
            if (s->len > 0 && write_full(fd, s->data, s->len) != (ssize_t)s->len) return KVSTORE_ERROR_IO;
            return KVSTORE_OK;
        }
        case KVSTORE_TYPE_INT64: {
            uint64_t int_n = htonll_portable((uint64_t)value->data.int_val);
            if (write_full(fd, &int_n, sizeof(uint64_t)) != sizeof(uint64_t)) return KVSTORE_ERROR_IO;
            return KVSTORE_OK;
        }
        case KVSTORE_TYPE_DOUBLE: {
            if (write_full(fd, &value->data.double_val, sizeof(double)) != sizeof(double))
                return KVSTORE_ERROR_IO;
            return KVSTORE_OK;
        }
        case KVSTORE_TYPE_BOOL: {
            uint8_t bool_byte = (uint8_t)value->data.bool_val;
            if (write_full(fd, &bool_byte, sizeof(uint8_t)) != sizeof(uint8_t)) return KVSTORE_ERROR_IO;
            return KVSTORE_OK;
        }
        default:
            return KVSTORE_ERROR_INVALID_TYPE;
    }
}

static kvstore_error_t read_value(int fd, kvstore_value_t* value) {
    uint8_t type_byte;
    if (read_full(fd, &type_byte, sizeof(uint8_t)) != sizeof(uint8_t)) {
        return KVSTORE_ERROR_IO;
    }

    kvstore_type_t type = (kvstore_type_t)type_byte;

    // Validate type
    if (type < KVSTORE_TYPE_NULL || type > KVSTORE_TYPE_BINARY) {
        return KVSTORE_ERROR_INVALID_FORMAT;
    }

    value->type = type;

    switch (type) {
        case KVSTORE_TYPE_NULL:
            return KVSTORE_OK;

        case KVSTORE_TYPE_STRING:
        case KVSTORE_TYPE_BINARY: {
            uint32_t len_n;
            if (read_full(fd, &len_n, sizeof(uint32_t)) != sizeof(uint32_t)) {
                return KVSTORE_ERROR_IO;
            }
            uint32_t len = ntohl_portable(len_n);

            // Validate length
            if (len > KVSTORE_MAX_STRING_SIZE) {
                return KVSTORE_ERROR_STRING_TOO_LARGE;
            }

            // Handle zero-length strings
            if (len == 0) {
                kvstore_string_t empty = kvstore_string_create("", 0);
                if (type == KVSTORE_TYPE_STRING) {
                    value->data.str_val = empty;
                } else {
                    value->data.binary_val = empty;
                }
                return KVSTORE_OK;
            }

            // Allocate memory for the string first
            char* buffer = malloc(len + 1);
            if (!buffer) {
                return KVSTORE_ERROR_MEMORY;
            }

            // Read the data directly into the allocated buffer
            if (read_full(fd, buffer, len) != (ssize_t)len) {
                free(buffer);
                return KVSTORE_ERROR_IO;
            }
            buffer[len] = '\0';  // Ensure null termination

            // Now create the kvstore_string_t with the populated buffer
            kvstore_string_t s;
            s.data = buffer;
            s.len  = len;

            if (type == KVSTORE_TYPE_STRING) {
                value->data.str_val = s;
            } else {
                value->data.binary_val = s;
            }
            return KVSTORE_OK;
        }

        case KVSTORE_TYPE_INT64: {
            uint64_t int_n;
            if (read_full(fd, &int_n, sizeof(uint64_t)) != sizeof(uint64_t)) {
                return KVSTORE_ERROR_IO;
            }
            value->data.int_val = (int64_t)ntohll_portable(int_n);
            return KVSTORE_OK;
        }

        case KVSTORE_TYPE_DOUBLE: {
            if (read_full(fd, &value->data.double_val, sizeof(double)) != sizeof(double)) {
                return KVSTORE_ERROR_IO;
            }
            return KVSTORE_OK;
        }

        case KVSTORE_TYPE_BOOL: {
            uint8_t bool_byte;
            if (read_full(fd, &bool_byte, sizeof(uint8_t)) != sizeof(uint8_t)) {
                return KVSTORE_ERROR_IO;
            }
            value->data.bool_val = (bool_byte != 0);
            return KVSTORE_OK;
        }

        default:
            return KVSTORE_ERROR_INVALID_TYPE;
    }
}

kvstore_error_t kvstore_load(kvstore_t* store, const char* filename) {
    if (!store || !filename) return KVSTORE_ERROR_NULL_POINTER;

    int fd = open(filename, O_RDONLY);
    if (fd < 0) return KVSTORE_ERROR_IO;

    // Read magic header first to verify file format
    uint32_t magic;
    if (read_full(fd, &magic, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    magic = ntohl_portable(magic);
    if (magic != MAGIC_NUMBER) {
        close(fd);
        return KVSTORE_ERROR_INVALID_FORMAT;
    }

    // Read version info (optional but good practice)
    uint8_t version_major, version_minor, version_patch;
    if (read_full(fd, &version_major, sizeof(uint8_t)) != sizeof(uint8_t) ||
        read_full(fd, &version_minor, sizeof(uint8_t)) != sizeof(uint8_t) ||
        read_full(fd, &version_patch, sizeof(uint8_t)) != sizeof(uint8_t)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    // Read entry count
    uint32_t count_n;
    if (read_full(fd, &count_n, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }
    uint32_t count = ntohl_portable(count_n);

    kvstore_clear(store);

    for (uint32_t i = 0; i < count; i++) {
        // Read key length
        uint32_t key_len_n;
        if (read_full(fd, &key_len_n, sizeof(uint32_t)) != sizeof(uint32_t)) {
            close(fd);
            return KVSTORE_ERROR_IO;
        }
        uint32_t key_len = ntohl_portable(key_len_n);
        if (key_len == 0 || key_len > KVSTORE_MAX_STRING_SIZE) {
            close(fd);
            return KVSTORE_ERROR_STRING_TOO_LARGE;
        }

        // Read key data
        char* key_data = malloc(key_len + 1);
        if (!key_data) {
            close(fd);
            return KVSTORE_ERROR_MEMORY;
        }

        if (read_full(fd, key_data, key_len) != (ssize_t)key_len) {
            free(key_data);
            close(fd);
            return KVSTORE_ERROR_IO;
        }
        key_data[key_len] = '\0';

        // Read value
        kvstore_value_t value;
        kvstore_error_t err = read_value(fd, &value);
        if (err != KVSTORE_OK) {
            free(key_data);
            close(fd);
            return err;
        }

        // Store entry
        err = kvstore_put_value(store, key_data, key_len, &value);
        kvstore_value_destroy(&value);
        free(key_data);

        if (err != KVSTORE_OK) {
            close(fd);
            return err;
        }
    }

    close(fd);
    return KVSTORE_OK;
}

kvstore_error_t kvstore_save(const kvstore_t* store, const char* filename) {
    if (!store || !filename) return KVSTORE_ERROR_NULL_POINTER;

    int fd = open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return KVSTORE_ERROR_IO;

    // Write magic header
    uint32_t magic_n = htonl_portable(MAGIC_NUMBER);
    if (write_full(fd, &magic_n, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    // Write version info
    uint8_t version_major = KVSTORE_VERSION_MAJOR;
    uint8_t version_minor = KVSTORE_VERSION_MINOR;
    uint8_t version_patch = KVSTORE_VERSION_PATCH;
    if (write_full(fd, &version_major, sizeof(uint8_t)) != sizeof(uint8_t) ||
        write_full(fd, &version_minor, sizeof(uint8_t)) != sizeof(uint8_t) ||
        write_full(fd, &version_patch, sizeof(uint8_t)) != sizeof(uint8_t)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    // Write entry count
    uint32_t count_n = htonl_portable((uint32_t)kvstore_size(store));
    if (write_full(fd, &count_n, sizeof(uint32_t)) != sizeof(uint32_t)) {
        close(fd);
        return KVSTORE_ERROR_IO;
    }

    kvstore_error_t err     = KVSTORE_OK;
    kvstore_iterator_t iter = kvstore_iter_begin(store);
    while (kvstore_iter_valid(&iter)) {
        const kvstore_entry_t* entry = kvstore_iter_get(&iter);
        uint32_t key_len_n           = htonl_portable(entry->key.len);

        // Write key length and key data
        if (write_full(fd, &key_len_n, sizeof(uint32_t)) != sizeof(uint32_t) ||
            write_full(fd, entry->key.data, entry->key.len) != (ssize_t)entry->key.len) {
            err = KVSTORE_ERROR_IO;
            break;
        }

        // Write value
        if (write_value(fd, &entry->value) != KVSTORE_OK) {
            err = KVSTORE_ERROR_IO;
            break;
        }

        kvstore_iter_next(&iter);
    }

    close(fd);
    return err;
}

// Iteration API
kvstore_iterator_t kvstore_iter_begin(const kvstore_t* store) {
    kvstore_iterator_t iter = {0};
    if (!store) return iter;
    iter.store         = store;
    iter.bucket_index  = 0;
    iter.current_entry = store->buckets[0];

    // Find first non-empty bucket
    while (iter.bucket_index < store->bucket_count && !iter.current_entry) {
        iter.bucket_index++;
        if (iter.bucket_index < store->bucket_count) {
            iter.current_entry = store->buckets[iter.bucket_index];
        }
    }
    return iter;
}

bool kvstore_iter_valid(const kvstore_iterator_t* iter) {
    return iter && iter->store && iter->bucket_index < iter->store->bucket_count && iter->current_entry;
}

void kvstore_iter_next(kvstore_iterator_t* iter) {
    if (!iter || !iter->store || !iter->current_entry) return;

    // Move to next entry in current bucket
    if (iter->current_entry->next) {
        iter->current_entry = iter->current_entry->next;
        return;
    }

    // Move to next non-empty bucket
    iter->bucket_index++;
    iter->current_entry = NULL;
    while (iter->bucket_index < iter->store->bucket_count) {
        iter->current_entry = iter->store->buckets[iter->bucket_index];
        if (iter->current_entry) break;
        iter->bucket_index++;
    }
}

const kvstore_entry_t* kvstore_iter_get(const kvstore_iterator_t* iter) {
    return iter && iter->current_entry ? iter->current_entry : NULL;
}

// Arena-specific utility functions
static size_t kvstore_arena_total_allocated(const kvstore_t* store) {
    return store ? store->arena.total_allocated : 0;
}
static size_t kvstore_arena_total_used(const kvstore_t* store) {
    return store ? store->arena.total_used : 0;
}

static double kvstore_arena_utilization(const kvstore_t* store) {
    if (!store || store->arena.total_allocated == 0) return 0.0;
    return (double)store->arena.total_used / (double)store->arena.total_allocated;
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
            return "Invalid format";
        case KVSTORE_ERROR_STRING_TOO_LARGE:
            return "String too large";
        case KVSTORE_ERROR_TYPE_MISMATCH:
            return "Type mismatch";
        case KVSTORE_ERROR_INVALID_TYPE:
            return "Invalid type";
        default:
            return "Unknown error";
    }
}

const char* kvstore_type_string(kvstore_type_t type) {
    switch (type) {
        case KVSTORE_TYPE_NULL:
            return "null";
        case KVSTORE_TYPE_STRING:
            return "string";
        case KVSTORE_TYPE_INT64:
            return "int64";
        case KVSTORE_TYPE_DOUBLE:
            return "double";
        case KVSTORE_TYPE_BOOL:
            return "bool";
        case KVSTORE_TYPE_BINARY:
            return "binary";
        default:
            return "unknown";
    }
}

void kvstore_print_stats(const kvstore_t* store) {
    if (!store) return;
    printf("KVStore Stats:\n");
    printf("  Size: %zu\n", kvstore_size(store));
    printf("  Capacity: %zu\n", kvstore_capacity(store));
    printf("  Load Factor: %.2f\n", kvstore_load_factor(store));
    printf("  Arena Total Allocated: %zu bytes\n", kvstore_arena_total_allocated(store));
    printf("  Arena Total Used: %zu bytes\n", kvstore_arena_total_used(store));
    printf("  Arena Utilization: %.2f%%\n", kvstore_arena_utilization(store) * 100.0);
}

void kvstore_print_all(const kvstore_t* store) {
    if (!store) return;
    printf("{\n");
    kvstore_iterator_t iter = kvstore_iter_begin(store);
    bool first              = true;

    while (kvstore_iter_valid(&iter)) {
        const kvstore_entry_t* entry = kvstore_iter_get(&iter);
        if (!first) printf(",\n");

        printf("  \"%.*s\": ", (int)entry->key.len, entry->key.data);

        switch (entry->value.type) {
            case KVSTORE_TYPE_NULL:
                printf("null");
                break;
            case KVSTORE_TYPE_STRING:
                printf("\"%s\"", entry->value.data.str_val.data);
                break;
            case KVSTORE_TYPE_INT64:
                printf("%ld", entry->value.data.int_val);
                break;
            case KVSTORE_TYPE_DOUBLE:
                printf("%g", entry->value.data.double_val);
                break;
            case KVSTORE_TYPE_BOOL:
                printf("%s", entry->value.data.bool_val ? "true" : "false");
                break;
            case KVSTORE_TYPE_BINARY:
                printf("<binary %u bytes>", entry->value.data.binary_val.len);
                break;
            default:
                printf("unknown");
        }

        kvstore_iter_next(&iter);
        first = false;
    }
    printf("\n}\n");
}
