#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// Version and constants
#define KVSTORE_VERSION_MAJOR 1
#define KVSTORE_VERSION_MINOR 0
#define KVSTORE_VERSION_PATCH 0

#define KVSTORE_DEFAULT_CAPACITY 1024
#define KVSTORE_MAX_STRING_SIZE  (1024 * 1024)  // 1MB limit
#define KVSTORE_MAGIC_HEADER     0x4B56DB00

// Error codes
typedef enum {
    KVSTORE_OK = 0,
    KVSTORE_ERROR_NULL_POINTER,
    KVSTORE_ERROR_INVALID_KEY,
    KVSTORE_ERROR_CAPACITY_FULL,
    KVSTORE_ERROR_KEY_NOT_FOUND,
    KVSTORE_ERROR_MEMORY,
    KVSTORE_ERROR_IO,
    KVSTORE_ERROR_INVALID_FORMAT,
    KVSTORE_ERROR_STRING_TOO_LARGE
} kvstore_error_t;

// String abstraction for binary-safe key-value storage
typedef struct {
    uint32_t len;
    char* data;
} kvstore_string_t;

// Key-value pair
typedef struct {
    kvstore_string_t key;
    kvstore_string_t value;
} kvstore_pair_t;

// Forward declaration
typedef struct kvstore kvstore_t;

// Core API - Store management
kvstore_t* kvstore_create(size_t capacity);
void kvstore_destroy(kvstore_t* store);
kvstore_error_t kvstore_clear(kvstore_t* store);
size_t kvstore_size(const kvstore_t* store);
size_t kvstore_capacity(const kvstore_t* store);

// Core API - Key-Value operations
kvstore_error_t kvstore_put(kvstore_t* store, const char* key_data, uint32_t key_len, const char* value_data,
                            uint32_t value_len);
kvstore_error_t kvstore_get(const kvstore_t* store, const char* key_data, uint32_t key_len,
                            const kvstore_string_t** value);
kvstore_error_t kvstore_delete(kvstore_t* store, const char* key_data, uint32_t key_len);
bool kvstore_exists(const kvstore_t* store, const char* key_data, uint32_t key_len);

// Convenience functions for null-terminated strings
kvstore_error_t kvstore_put_str(kvstore_t* store, const char* key, const char* value);
kvstore_error_t kvstore_get_str(const kvstore_t* store, const char* key, const kvstore_string_t** value);
kvstore_error_t kvstore_delete_str(kvstore_t* store, const char* key);
bool kvstore_exists_str(const kvstore_t* store, const char* key);

// Persistence API
kvstore_error_t kvstore_load(kvstore_t* store, const char* filename);
kvstore_error_t kvstore_save(const kvstore_t* store, const char* filename);

// Iteration API
typedef struct {
    const kvstore_t* store;
    size_t index;
} kvstore_iterator_t;

kvstore_iterator_t kvstore_iter_begin(const kvstore_t* store);
bool kvstore_iter_valid(const kvstore_iterator_t* iter);
void kvstore_iter_next(kvstore_iterator_t* iter);
const kvstore_pair_t* kvstore_iter_get(const kvstore_iterator_t* iter);

// Utility functions
const char* kvstore_error_string(kvstore_error_t error);
void kvstore_print_stats(const kvstore_t* store);
void kvstore_print_all(const kvstore_t* store);

// String utilities
kvstore_string_t kvstore_string_create(const char* data, uint32_t len);
kvstore_string_t kvstore_string_from_cstr(const char* cstr);
void kvstore_string_destroy(kvstore_string_t* str);
bool kvstore_string_equals(const kvstore_string_t* a, const kvstore_string_t* b);
kvstore_string_t kvstore_string_copy(const kvstore_string_t* src);

#ifdef __cplusplus
}
#endif

#endif  // KVSTORE_H
