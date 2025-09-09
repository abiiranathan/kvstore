#include "../include/kvapi.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>  // For backup timestamp

// Internal handle structure
struct kvapi_handle {
    kvstore_t* store;
    kvapi_config_t config;
    pthread_mutex_t mutex;
    bool initialized;
};

// Default config
kvapi_config_t kvapi_default_config = {
    .capacity           = KVSTORE_DEFAULT_CAPACITY,
    .db_file            = "kvstore.db",
    .auto_save          = true,
    .auto_save_interval = 60,
};

// Static helper for mutex (reuse CLI's if possible, but wrap for API)
#define LOCK_API(h)   pthread_mutex_lock(&(h)->mutex)
#define UNLOCK_API(h) pthread_mutex_unlock(&(h)->mutex)

kvapi_handle_t* kvapi_init(const kvapi_config_t* user_config) {
    if (!user_config) user_config = &kvapi_default_config;

    kvapi_handle_t* handle = malloc(sizeof(kvapi_handle_t));
    if (!handle) return NULL;

    handle->config = *user_config;
    handle->store  = kvstore_create(handle->config.capacity);
    if (!handle->store) {
        free(handle);
        return NULL;
    }

    pthread_mutex_init(&handle->mutex, NULL);
    handle->initialized = true;

    // Load DB
    LOCK_API(handle);
    kvstore_error_t err = kvstore_load(handle->store, handle->config.db_file);
    UNLOCK_API(handle);

    if (err != KVSTORE_OK && err != KVSTORE_ERROR_IO) {
        // Log or handle error as needed
        kvapi_destroy(handle);
        return NULL;
    }

    return handle;
}

void kvapi_destroy(kvapi_handle_t* handle) {
    if (!handle || !handle->initialized) return;

    // Auto-save if enabled
    if (handle->config.auto_save) {
        LOCK_API(handle);
        kvstore_save(handle->store, handle->config.db_file);
        UNLOCK_API(handle);
    }

    kvstore_destroy(handle->store);
    pthread_mutex_destroy(&handle->mutex);
    handle->initialized = false;
    free(handle);
}

// Set string
kvstore_error_t kvapi_set_string(kvapi_handle_t* handle, const char* key, const char* value) {
    if (!handle || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_put_string(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}

// Set int64
kvstore_error_t kvapi_set_int64(kvapi_handle_t* handle, const char* key, int64_t value) {
    if (!handle || !key) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_put_int64(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}

// Set double
kvstore_error_t kvapi_set_double(kvapi_handle_t* handle, const char* key, double value) {
    if (!handle || !key) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_put_double(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}

// Set bool
kvstore_error_t kvapi_set_bool(kvapi_handle_t* handle, const char* key, bool value) {
    if (!handle || !key) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_put_bool(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}

// Set null
kvstore_error_t kvapi_set_null(kvapi_handle_t* handle, const char* key) {
    if (!handle || !key) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_put_null(handle->store, key);
    UNLOCK_API(handle);
    return err;
}

// Get (auto-detect)
kvstore_error_t kvapi_get(kvapi_handle_t* handle, const char* key, kvapi_result_t* result) {
    if (!handle || !key || !result) return KVSTORE_ERROR_NULL_POINTER;
    memset(result, 0, sizeof(kvapi_result_t));

    LOCK_API(handle);
    const kvstore_value_t* val;
    kvstore_error_t err = kvstore_get_value(handle->store, key, (uint32_t)strlen(key), &val);
    UNLOCK_API(handle);

    if (err != KVSTORE_OK) {
        result->error = err;
        return err;
    }

    result->type  = val->type;
    result->error = KVSTORE_OK;
    switch (val->type) {
        case KVSTORE_TYPE_NULL:
            return KVSTORE_OK;
        case KVSTORE_TYPE_STRING:
            result->value.str_val = strdup(val->data.str_val.data);
            return KVSTORE_OK;
        case KVSTORE_TYPE_INT64:
            result->value.int_val = val->data.int_val;
            return KVSTORE_OK;
        case KVSTORE_TYPE_DOUBLE:
            result->value.double_val = val->data.double_val;
            return KVSTORE_OK;
        case KVSTORE_TYPE_BOOL:
            result->value.bool_val = val->data.bool_val;
            return KVSTORE_OK;
        case KVSTORE_TYPE_BINARY:
            result->value.binary_val.data = malloc(val->data.binary_val.len);
            if (result->value.binary_val.data) {
                memcpy(result->value.binary_val.data, val->data.binary_val.data, val->data.binary_val.len);
                result->value.binary_val.len = val->data.binary_val.len;
            } else {
                err = KVSTORE_ERROR_MEMORY;
            }
            return err;
        default:
            return KVSTORE_ERROR_INVALID_TYPE;
    }
}

// Returns the handle to the store.
kvstore_t* kvapi_store(kvapi_handle_t* handle) {
    if (!handle) return NULL;
    return handle->store;
}

// Get type
kvstore_error_t kvapi_get_type(kvapi_handle_t* handle, const char* key, kvstore_type_t* type) {
    if (!handle || !key || !type) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_get_type(handle->store, key, (uint32_t)strlen(key), type);
    UNLOCK_API(handle);
    return err;
}

// Delete
kvstore_error_t kvapi_delete(kvapi_handle_t* handle, const char* key, bool* deleted) {
    if (!handle || !key) return KVSTORE_ERROR_NULL_POINTER;
    if (deleted) *deleted = false;

    LOCK_API(handle);
    kvstore_error_t err = kvstore_delete_key(handle->store, key, (uint32_t)strlen(key));
    if (err == KVSTORE_OK && deleted)
        *deleted = true;
    else if (err == KVSTORE_ERROR_KEY_NOT_FOUND && deleted)
        *deleted = false;
    UNLOCK_API(handle);
    return err;
}

// Exists
bool kvapi_exists(kvapi_handle_t* handle, const char* key) {
    if (!handle || !key) return false;
    LOCK_API(handle);
    bool exists = kvstore_exists_key(handle->store, key, (uint32_t)strlen(key));
    UNLOCK_API(handle);
    return exists;
}

// Clear
kvstore_error_t kvapi_clear(kvapi_handle_t* handle) {
    if (!handle) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_clear(handle->store);
    UNLOCK_API(handle);
    return err;
}

// Save
kvstore_error_t kvapi_save(kvapi_handle_t* handle, const char* filename) {
    if (!handle) return KVSTORE_ERROR_NULL_POINTER;
    const char* fname = filename ? filename : handle->config.db_file;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_save(handle->store, fname);
    UNLOCK_API(handle);
    return err;
}

// Load
kvstore_error_t kvapi_load(kvapi_handle_t* handle, const char* filename) {
    if (!handle) return KVSTORE_ERROR_NULL_POINTER;
    const char* fname = filename ? filename : handle->config.db_file;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_load(handle->store, fname);
    UNLOCK_API(handle);
    return err;
}

// Backup (timestamp if NULL)
kvstore_error_t kvapi_backup(kvapi_handle_t* handle, const char* filename) {
    if (!handle) return KVSTORE_ERROR_NULL_POINTER;
    char backup_name[256];
    if (!filename) {
        time_t now    = time(NULL);
        struct tm* tm = localtime(&now);
        snprintf(backup_name, sizeof(backup_name), "%s.backup.%04d%02d%02d-%02d%02d%02d",
                 handle->config.db_file, tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour,
                 tm->tm_min, tm->tm_sec);
        filename = backup_name;
    }
    LOCK_API(handle);
    kvstore_error_t err = kvstore_save(handle->store, filename);
    UNLOCK_API(handle);
    return err;
}

// Stats
void kvapi_stats(kvapi_handle_t* handle, FILE* out) {
    if (!handle || !out) return;
    LOCK_API(handle);
    kvstore_print_stats(handle->store);
    UNLOCK_API(handle);
}

// Size
size_t kvapi_size(kvapi_handle_t* handle) {
    if (!handle) return 0;
    LOCK_API(handle);
    size_t sz = kvstore_size(handle->store);
    UNLOCK_API(handle);
    return sz;
}

// Free result
void kvapi_free_result(kvapi_result_t* result) {
    if (!result) return;
    if (result->type == KVSTORE_TYPE_STRING && result->value.str_val) {
        free(result->value.str_val);
    } else if (result->type == KVSTORE_TYPE_BINARY && result->value.binary_val.data) {
        free(result->value.binary_val.data);
    }
    memset(result, 0, sizeof(kvapi_result_t));
}

// Type-specific gets (implement similarly to kvapi_get, extracting from kvstore_get_*)
kvstore_error_t kvapi_get_string(kvapi_handle_t* handle, const char* key, char** value) {
    if (!handle || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    *value = NULL;
    const kvstore_string_t* str;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_get_string(handle->store, key, &str);
    if (err == KVSTORE_OK) {
        *value = strdup(str->data);
    }
    UNLOCK_API(handle);
    return err;
}

// ... (similar for int64, double, bool using kvstore_get_int64, etc.)
kvstore_error_t kvapi_get_int64(kvapi_handle_t* handle, const char* key, int64_t* value) {
    if (!handle || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_get_int64(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}

kvstore_error_t kvapi_get_double(kvapi_handle_t* handle, const char* key, double* value) {
    if (!handle || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_get_double(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}

kvstore_error_t kvapi_get_bool(kvapi_handle_t* handle, const char* key, bool* value) {
    if (!handle || !key || !value) return KVSTORE_ERROR_NULL_POINTER;
    LOCK_API(handle);
    kvstore_error_t err = kvstore_get_bool(handle->store, key, value);
    UNLOCK_API(handle);
    return err;
}
