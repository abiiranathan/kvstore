#ifndef KVAPI_H
#define KVAPI_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "kvstore.h"

// Configuration structure (mirrors CLI config)
typedef struct {
    size_t capacity;
    const char* db_file;
    bool auto_save;
    int auto_save_interval;  // seconds, for future auto-save thread
} kvapi_config_t;

// Default config
extern kvapi_config_t kvapi_default_config;

// KVAPI handle (opaque, wraps kvstore_t and config)
typedef struct kvapi_handle kvapi_handle_t;

// Result structures for get operations
typedef struct {
    kvstore_type_t type;
    union {
        char* str_val;  // Null-terminated, caller must free if non-NULL
        int64_t int_val;
        double double_val;
        bool bool_val;
        struct {
            uint8_t* data;  // Binary data, caller must free if non-NULL
            uint32_t len;
        } binary_val;
    } value;
    kvstore_error_t error;  // Always set; KVSTORE_OK on success
} kvapi_result_t;

// Initialize KVAPI with config (loads DB if exists)
kvapi_handle_t* kvapi_init(const kvapi_config_t* config);

// Destroy KVAPI handle (auto-saves if enabled)
void kvapi_destroy(kvapi_handle_t* handle);

// Set string value
kvstore_error_t kvapi_set_string(kvapi_handle_t* handle, const char* key, const char* value);

// Set int64 value
kvstore_error_t kvapi_set_int64(kvapi_handle_t* handle, const char* key, int64_t value);

// Set double value
kvstore_error_t kvapi_set_double(kvapi_handle_t* handle, const char* key, double value);

// Set bool value
kvstore_error_t kvapi_set_bool(kvapi_handle_t* handle, const char* key, bool value);

// Set null value
kvstore_error_t kvapi_set_null(kvapi_handle_t* handle, const char* key);

// Get value (auto-detect type, populate result)
kvstore_error_t kvapi_get(kvapi_handle_t* handle, const char* key, kvapi_result_t* result);

// Returns the handle to the store.
kvstore_t* kvapi_store(kvapi_handle_t* handle);

// Get specific type (use kvapi_get for auto-detect)
kvstore_error_t kvapi_get_string(kvapi_handle_t* handle, const char* key, char** value);  // Caller frees
kvstore_error_t kvapi_get_int64(kvapi_handle_t* handle, const char* key, int64_t* value);
kvstore_error_t kvapi_get_double(kvapi_handle_t* handle, const char* key, double* value);
kvstore_error_t kvapi_get_bool(kvapi_handle_t* handle, const char* key, bool* value);

// Get type of key
kvstore_error_t kvapi_get_type(kvapi_handle_t* handle, const char* key, kvstore_type_t* type);

// Delete key
kvstore_error_t kvapi_delete(kvapi_handle_t* handle, const char* key, bool* deleted);  // true if existed

// Check if key exists
bool kvapi_exists(kvapi_handle_t* handle, const char* key);

// Clear all keys
kvstore_error_t kvapi_clear(kvapi_handle_t* handle);

// Save to file
kvstore_error_t kvapi_save(kvapi_handle_t* handle, const char* filename);

// Load from file
kvstore_error_t kvapi_load(kvapi_handle_t* handle, const char* filename);

// Backup to file (timestamped if NULL)
kvstore_error_t kvapi_backup(kvapi_handle_t* handle, const char* filename);

// Get stats (printf to FILE*)
void kvapi_stats(kvapi_handle_t* handle, FILE* out);

// Get size (number of entries)
size_t kvapi_size(kvapi_handle_t* handle);

// Free result (for str/binary)
void kvapi_free_result(kvapi_result_t* result);

#endif /* KVAPI_H */
