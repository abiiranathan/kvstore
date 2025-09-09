#include "../include/common.h"
#include <stdarg.h>

__attribute__((format(printf, 2, 3))) void kv_log(log_level_t level, const char* format, ...) {
    va_list args;
    va_start(args, format);

    const char* level_str;
    FILE* output;

    switch (level) {
        case LOG_DEBUG:
            level_str = "DEBUG";
            output    = stdout;
            break;
        case LOG_INFO:
            level_str = "INFO";
            output    = stdout;
            break;
        case LOG_WARNING:
            level_str = "WARN";
            output    = stderr;
            break;
        case LOG_ERROR:
            level_str = "ERROR";
            output    = stderr;
            break;
        default:
            level_str = "INFO";
            output    = stdout;
            break;
    }

    time_t now   = time(NULL);
    struct tm* t = localtime(&now);

    fprintf(output, "[%04d-%02d-%02d %02d:%02d:%02d] [%s] ", t->tm_year + 1900, t->tm_mon + 1, t->tm_mday,
            t->tm_hour, t->tm_min, t->tm_sec, level_str);

    vfprintf(output, format, args);
    fprintf(output, "\n");

    va_end(args);

    if (output == stderr) {
        fflush(stderr);
    }
}

void kv_print_error(const char* cmd, kvstore_error_t error, const char* details) {
    if (details) {
        kv_log(LOG_ERROR, "Command %s failed: %s (%s)", cmd, kvstore_error_string(error), details);
    } else {
        kv_log(LOG_ERROR, "Command %s failed: %s", cmd, kvstore_error_string(error));
    }
}

bool kv_validate_key(const char* key) {
    if (!key || strlen(key) == 0) {
        kv_log(LOG_DEBUG, "Key validation failed: null or empty key");
        return false;
    }

    if (strlen(key) > KVSTORE_MAX_STRING_SIZE) {
        kv_log(LOG_DEBUG, "Key validation failed: key too long (%zu > %u)", strlen(key),
               KVSTORE_MAX_STRING_SIZE);
        return false;
    }

    return true;
}

bool kv_validate_value_len(size_t value_len) {
    if (value_len > KVSTORE_MAX_STRING_SIZE) {
        kv_log(LOG_DEBUG, "Value validation failed: value too long (%zu > %u)", value_len,
               KVSTORE_MAX_STRING_SIZE);
        return false;
    }
    return true;
}

char** kv_split_args(char* line, int* argc) {
    static char* args[64];
    *argc = 0;

    if (!line || strlen(line) == 0) {
        return args;
    }

    char* ptr       = line;
    bool in_quote   = false;
    bool in_token   = false;
    char quote_char = '\0';

    while (*ptr && *argc < 63) {
        // Skip leading whitespace
        while (*ptr == ' ' || *ptr == '\t') {
            ptr++;
        }

        if (!*ptr) break;

        // Handle quoted strings
        if (*ptr == '"' || *ptr == '\'') {
            in_quote   = true;
            quote_char = *ptr;
            ptr++;  // Skip the opening quote
            args[(*argc)++] = ptr;
            in_token        = true;
        } else {
            args[(*argc)++] = ptr;
            in_token        = true;
        }

        // Find the end of this argument
        while (*ptr && (in_quote || (*ptr != ' ' && *ptr != '\t'))) {
            if (in_quote) {
                if (*ptr == quote_char) {
                    // Found closing quote
                    *ptr = '\0';  // Terminate the token
                    ptr++;
                    in_quote = false;
                    break;
                }
                if (*ptr == '\\' && *(ptr + 1) == quote_char) {
                    // Escaped quote, remove the backslash
                    memmove(ptr, ptr + 1, strlen(ptr));
                }
            }
            ptr++;
        }

        if (in_token) {
            // Terminate the current token
            if (*ptr) {
                *ptr = '\0';
                ptr++;
            }
            in_token = false;
        }

        // Handle unclosed quotes
        if (in_quote) {
            in_quote = false;
        }
    }

    args[*argc] = NULL;
    return args;
}

int kv_load_config(const char* config_file, kvapi_config_t* config) {
    FILE* file = fopen(config_file, "r");
    if (!file) {
        kv_log(LOG_DEBUG, "Config file %s not found, using defaults", config_file);
        return 0;
    }

    char line[MAX_CONFIG_LINE] = {0};
    int line_num               = 0;

    while (fgets(line, sizeof(line), file)) {
        line_num++;

        // Remove trailing newline and comments
        char* comment = strchr(line, '#');
        if (comment) *comment = '\0';

        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }

        // Skip empty lines
        if (strlen(line) == 0) continue;

        // Parse config lines (key=value format)
        char* eq_pos = strchr(line, '=');
        if (!eq_pos) continue;

        *eq_pos     = '\0';
        char* key   = line;
        char* value = eq_pos + 1;

        // Trim value
        while (*value == ' ' || *value == '\t')
            value++;
        char* end = value + strlen(value) - 1;
        while (end > value && (*end == ' ' || *end == '\t'))
            end--;
        *(end + 1) = '\0';

        if (strcmp(key, "capacity") == 0) {
            config->capacity = (size_t)atoi(value);
        } else if (strcmp(key, "db_file") == 0) {
            kv_log(LOG_WARNING, "db_file cannot be changed at runtime. Restart with -f option.\n");
        } else if (strcmp(key, "auto_save") == 0) {
            config->auto_save = (strcmp(value, "true") == 0 || strcmp(value, "1") == 0);
        } else if (strcmp(key, "auto_save_interval") == 0) {
            config->auto_save_interval = atoi(value);
        } else {
            kv_log(LOG_WARNING, "Unknown config option on line %d: %s", line_num, line);
        }
    }

    fclose(file);
    kv_log(LOG_INFO, "Configuration loaded from %s", config_file);
    return 0;
}

void kv_cleanup(kvapi_config_t* g_config, kvapi_handle_t* g_api) {
    kv_log(LOG_INFO, "Cleaning up resources");

    // Auto-save if enabled
    if (g_config->auto_save && g_api) {
        size_t api_size = kvapi_size(g_api);
        if (api_size > 0) {
            kv_log(LOG_INFO, "Auto-saving %zu key-value pairs", api_size);
            kvstore_error_t error = kvapi_save(g_api, g_config->db_file);
            if (error != KVSTORE_OK) {
                kv_log(LOG_ERROR, "Failed to auto-save: %s", kvstore_error_string(error));
            } else {
                kv_log(LOG_INFO, "Auto-save completed successfully");
            }
        }
    }

    // Destroy API
    if (g_api) {
        kv_log(LOG_DEBUG, "Destroying KVAPI handle");
        kvapi_destroy(g_api);
        g_api = NULL;
    }

    kv_log(LOG_INFO, "Cleanup completed");
}

bool kv_parse_bool(const char* str, bool* result) {
    if (strcmp(str, "true") == 0 || strcmp(str, "1") == 0) {
        *result = true;
        return true;
    } else if (strcmp(str, "false") == 0 || strcmp(str, "0") == 0) {
        *result = false;
        return true;
    }
    return false;
}
