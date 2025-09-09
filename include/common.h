#ifndef COMMON_H
#define COMMON_H

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>

#include "kvapi.h"

#define MAX_COMMAND_LEN         4096
#define MAX_CONFIG_LINE         256
#define KVSTORE_MAX_STRING_SIZE (1024 * 1024)  // 1MB, adjust as needed

// Logging levels
typedef enum { LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERROR } log_level_t;

// Utility function declarations
void kv_log(log_level_t level, const char* format, ...);
void kv_print_error(const char* cmd, kvstore_error_t error, const char* details);
bool kv_validate_key(const char* key);
bool kv_validate_value_len(size_t value_len);
char** kv_split_args(char* line, int* argc);
int kv_load_config(const char* config_file, kvapi_config_t* config);
void kv_cleanup(kvapi_config_t* config, kvapi_handle_t* api);
bool kv_parse_bool(const char* str, bool* result);

// For server-specific logging alias (if needed)
#define LOG_MESSAGE(level, fmt, ...) common_logger(level, fmt, ##__VA_ARGS__)

#endif  // COMMON_H
