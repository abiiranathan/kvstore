#ifndef KVSTORE_H
#define KVSTORE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Version and constants
#define KVSTORE_VERSION_MAJOR 3
#define KVSTORE_VERSION_MINOR 0
#define KVSTORE_VERSION_PATCH 0

#define KVSTORE_DEFAULT_CAPACITY    1024
#define KVSTORE_MAX_STRING_SIZE     (1024 * 1024)  // 1MB limit
#define KVSTORE_DEFAULT_LOAD_FACTOR 0.75
#define KVSTORE_MIN_CAPACITY        16

// Data types supported by the KV store
typedef enum {
    KVSTORE_TYPE_NULL = 0,  // Null/empty value
    KVSTORE_TYPE_STRING,    // String data
    KVSTORE_TYPE_INT64,     // 64-bit signed integer
    KVSTORE_TYPE_DOUBLE,    // Double precision float
    KVSTORE_TYPE_BOOL,      // Boolean
    KVSTORE_TYPE_BINARY,    // Binary blob
} kvstore_type_t;

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
    KVSTORE_ERROR_STRING_TOO_LARGE,
    KVSTORE_ERROR_TYPE_MISMATCH,
    KVSTORE_ERROR_INVALID_TYPE,
} kvstore_error_t;

// Forward declarations
typedef struct kvstore kvstore_t;
typedef struct kvstore_value kvstore_value_t;

// String abstraction for binary-safe storage
typedef struct {
    uint32_t len;
    char* data;
} kvstore_string_t;

// Value union with type information
struct kvstore_value {
    kvstore_type_t type;
    union {
        kvstore_string_t str_val;
        int64_t int_val;
        double double_val;
        bool bool_val;
        kvstore_string_t binary_val;
    } data;
};

// Hash table entry with typed values
typedef struct kvstore_entry {
    kvstore_string_t key;
    kvstore_value_t value;
    struct kvstore_entry* next;
    uint32_t hash;
} kvstore_entry_t;

// Arena allocator
typedef struct arena_block {
    struct arena_block* next;
    size_t used;
    size_t size;
    char data[];
} arena_block_t;

typedef struct {
    arena_block_t* first;
    arena_block_t* current;
    size_t block_size;
} arena_t;

// Core API - Store management
kvstore_t* kvstore_create(size_t capacity);
void kvstore_destroy(kvstore_t* store);
kvstore_error_t kvstore_clear(kvstore_t* store);
size_t kvstore_size(const kvstore_t* store);
size_t kvstore_capacity(const kvstore_t* store);
double kvstore_load_factor(const kvstore_t* store);

// Core API - Typed operations
kvstore_error_t kvstore_put_value(kvstore_t* store, const char* key_data, uint32_t key_len,
                                  const kvstore_value_t* value);
kvstore_error_t kvstore_get_value(const kvstore_t* store, const char* key_data, uint32_t key_len,
                                  const kvstore_value_t** value);
kvstore_error_t kvstore_delete_key(kvstore_t* store, const char* key_data, uint32_t key_len);
bool kvstore_exists_key(const kvstore_t* store, const char* key_data, uint32_t key_len);
kvstore_error_t kvstore_get_type(const kvstore_t* store, const char* key_data, uint32_t key_len,
                                 kvstore_type_t* type);

// Type-specific put operations
kvstore_error_t kvstore_put_null(kvstore_t* store, const char* key);
kvstore_error_t kvstore_put_string(kvstore_t* store, const char* key, const char* value);
kvstore_error_t kvstore_put_string_len(kvstore_t* store, const char* key, const char* value, uint32_t len);
kvstore_error_t kvstore_put_int64(kvstore_t* store, const char* key, int64_t value);
kvstore_error_t kvstore_put_double(kvstore_t* store, const char* key, double value);
kvstore_error_t kvstore_put_bool(kvstore_t* store, const char* key, bool value);
kvstore_error_t kvstore_put_binary(kvstore_t* store, const char* key, const void* data, uint32_t len);

// Type-specific get operations
kvstore_error_t kvstore_get_string(const kvstore_t* store, const char* key, const kvstore_string_t** value);
kvstore_error_t kvstore_get_int64(const kvstore_t* store, const char* key, int64_t* value);
kvstore_error_t kvstore_get_double(const kvstore_t* store, const char* key, double* value);
kvstore_error_t kvstore_get_bool(const kvstore_t* store, const char* key, bool* value);
kvstore_error_t kvstore_get_binary(const kvstore_t* store, const char* key, const kvstore_string_t** data);

// Legacy string-only operations
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
    size_t bucket_index;
    const kvstore_entry_t* current_entry;
} kvstore_iterator_t;

kvstore_iterator_t kvstore_iter_begin(const kvstore_t* store);
bool kvstore_iter_valid(const kvstore_iterator_t* iter);
void kvstore_iter_next(kvstore_iterator_t* iter);
const kvstore_entry_t* kvstore_iter_get(const kvstore_iterator_t* iter);

// Value creation and destruction
kvstore_value_t kvstore_value_create_null(void);
kvstore_value_t kvstore_value_create_string(const char* data, uint32_t len);
kvstore_value_t kvstore_value_create_int64(int64_t value);
kvstore_value_t kvstore_value_create_double(double value);
kvstore_value_t kvstore_value_create_bool(bool value);
kvstore_value_t kvstore_value_create_binary(const void* data, uint32_t len);
void kvstore_value_destroy(kvstore_value_t* value);
kvstore_value_t kvstore_value_copy(const kvstore_value_t* src);

// Utility functions
const char* kvstore_error_string(kvstore_error_t error);
const char* kvstore_type_string(kvstore_type_t type);
void kvstore_print_stats(const kvstore_t* store);
void kvstore_print_all(const kvstore_t* store);

// String utilities
kvstore_string_t kvstore_string_create(const char* data, uint32_t len);
kvstore_string_t kvstore_string_from_cstr(const char* cstr);
void kvstore_string_destroy(kvstore_string_t* str);
bool kvstore_string_equals(const kvstore_string_t* a, const kvstore_string_t* b);
kvstore_string_t kvstore_string_copy(const kvstore_string_t* src);

// Arena allocator functions
arena_t* arena_create(size_t block_size);
void arena_destroy(arena_t* arena);
void* arena_alloc(arena_t* arena, size_t size);
void arena_reset(arena_t* arena);

#ifdef __cplusplus
}
#endif

#endif  // KVSTORE_H
