#include <stdio.h>  // Before readline include for FILE* symbol.

#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <signal.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>

#include "include/common.h"
#include "include/kvapi.h"

#define DEFAULT_DB_FILE   "kvstore.db"
#define MAX_COMMAND_LEN   4096
#define MAX_CONFIG_LINE   256
#define HISTORY_FILE_SIZE 1000

// Global state
static kvapi_handle_t* g_api = NULL;  // The KV API handle.

// CLI configuration.
static kvapi_config_t g_config = {
    .capacity           = KVSTORE_DEFAULT_CAPACITY,
    .db_file            = DEFAULT_DB_FILE,
    .auto_save          = true,
    .auto_save_interval = 60,
};

static bool g_running                         = true;
static volatile sig_atomic_t g_immediate_exit = 0;

// Command structure
typedef struct {
    const char* name;
    const char* usage;
    const char* description;
    int (*handler)(int argc, char** argv);
} command_t;

// Forward declarations
static int cmd_help(int argc, char** argv);
static int cmd_set(int argc, char** argv);
static int cmd_get(int argc, char** argv);
static int cmd_del(int argc, char** argv);
static int cmd_exists(int argc, char** argv);
static int cmd_keys(int argc, char** argv);
static int cmd_clear(int argc, char** argv);
static int cmd_stats(int argc, char** argv);
static int cmd_save(int argc, char** argv);
static int cmd_load(int argc, char** argv);
static int cmd_backup(int argc, char** argv);
static int cmd_config(int argc, char** argv);
static int cmd_quit(int argc, char** argv);
static int cmd_type(int argc, char** argv);
static int cmd_set_int(int argc, char** argv);
static int cmd_get_int(int argc, char** argv);
static int cmd_set_double(int argc, char** argv);
static int cmd_get_double(int argc, char** argv);
static int cmd_set_bool(int argc, char** argv);
static int cmd_get_bool(int argc, char** argv);
static int cmd_set_null(int argc, char** argv);

// Utility function declarations;
static void setup_signals(void);
static void setup_readline(void);
static void save_history(void);
static int load_config(const char* config_file);
static void cleanup(void);

// Command table
static const command_t commands[] = {
    {"help", "help [command]", "Show help for commands", cmd_help},
    {"set", "set <key> <value>", "Set key to string value", cmd_set},
    {"set-int", "set-int <key> <int_value>", "Set key to int64 value", cmd_set_int},
    {"set-double", "set-double <key> <double_value>", "Set key to double value", cmd_set_double},
    {"set-bool", "set-bool <key> <true|false|1|0>", "Set key to bool value", cmd_set_bool},
    {"set-null", "set-null <key>", "Set key to null value", cmd_set_null},
    {"get", "get <key>", "Get value for key (auto-detect type)", cmd_get},
    {"get-int", "get-int <key>", "Get int64 value for key", cmd_get_int},
    {"get-double", "get-double <key>", "Get double value for key", cmd_get_double},
    {"get-bool", "get-bool <key>", "Get bool value for key", cmd_get_bool},
    {"type", "type <key>", "Get type of key", cmd_type},
    {"del", "del <key>", "Delete key", cmd_del},
    {"exists", "exists <key>", "Check if key exists", cmd_exists},
    {"keys", "keys", "List all keys", cmd_keys},
    {"clear", "clear", "Clear all keys", cmd_clear},
    {"stats", "stats", "Show store statistics", cmd_stats},
    {"save", "save [filename]", "Save store to file", cmd_save},
    {"load", "load [filename]", "Load store from file", cmd_load},
    {"backup", "backup [filename]", "Create backup", cmd_backup},
    {"config", "config [key] [value]", "View or set configuration", cmd_config},
    {"quit", "quit", "Exit the program", cmd_quit},
    {"exit", "exit", "Exit the program", cmd_quit},
    {NULL, NULL, NULL, NULL},
};

// Signal handler for graceful shutdown
static void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        // First signal - try graceful shutdown
        if (!g_immediate_exit) {
            kv_log(LOG_INFO, "Received signal %d, initiating graceful shutdown", sig);
            g_running        = false;
            g_immediate_exit = 1;

            // If we're in readline, we need to force exit
            rl_done = 1;  // Tell readline to return immediately
        } else {
            // Second signal - force immediate exit
            kv_log(LOG_INFO, "Received second signal %d, forcing immediate exit", sig);
            _exit(1);
        }
    }
}

// Command implementations
static int cmd_help(int argc, char** argv) {
    if (argc == 1) {
        printf("Available commands:\n");
        for (const command_t* cmd = commands; cmd->name; cmd++) {
            printf("  %-30s %s\n", cmd->usage, cmd->description);
        }
        printf("\nUse 'help <command>' for specific command help.\n");
        return 0;
    }

    const char* cmd_name = argv[1];
    for (const command_t* cmd = commands; cmd->name; cmd++) {
        if (strcmp(cmd->name, cmd_name) == 0) {
            printf("Usage: %s\n", cmd->usage);
            printf("Description: %s\n", cmd->description);
            return 0;
        }
    }

    printf("Unknown command: %s\n", cmd_name);
    return 1;
}

// Enhanced set command that handles values with spaces properly
static int cmd_set(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: set <key> <value>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("set", KVSTORE_ERROR_INVALID_KEY, "Invalid key format or length");
        return 1;
    }

    // Reconstruct the value from remaining arguments
    size_t value_len = 0;
    for (int i = 2; i < argc; i++) {
        value_len += strlen(argv[i]);
        if (i < argc - 1) value_len++;  // Space between args
    }

    if (!kv_validate_value_len(value_len)) {
        kv_print_error("set", KVSTORE_ERROR_STRING_TOO_LARGE, "Value too long");
        return 1;
    }

    // If there's only one value argument, use it directly
    if (argc == 3) {
        kvstore_error_t error = kvapi_set_string(g_api, argv[1], argv[2]);
        if (error != KVSTORE_OK) {
            kv_print_error("set", error, NULL);
            return 1;
        }
    } else {
        // Multiple arguments, concatenate them
        char* value = malloc(value_len + 1);
        if (!value) {
            kv_print_error("set", KVSTORE_ERROR_MEMORY, "Value allocation failed");
            return 1;
        }

        value[0] = '\0';
        for (int i = 2; i < argc; i++) {
            if (i > 2) {
                strcat(value, " ");
            }
            strcat(value, argv[i]);
        }

        kvstore_error_t error = kvapi_set_string(g_api, argv[1], value);
        free(value);

        if (error != KVSTORE_OK) {
            kv_print_error("set", error, NULL);
            return 1;
        }
    }

    printf("OK\n");
    return 0;
}

static int cmd_set_int(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: set-int <key> <int_value>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("set-int", KVSTORE_ERROR_INVALID_KEY, "Invalid key format or length");
        return 1;
    }

    char* endptr;
    errno         = 0;
    int64_t value = strtoll(argv[2], &endptr, 10);
    if (errno != 0 || *endptr != '\0') {
        kv_print_error("set-int", KVSTORE_ERROR_INVALID_TYPE, "Invalid integer value");
        return 1;
    }

    kvstore_error_t error = kvapi_set_int64(g_api, argv[1], value);

    if (error != KVSTORE_OK) {
        kv_print_error("set-int", error, NULL);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_set_double(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: set-double <key> <double_value>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("set-double", KVSTORE_ERROR_INVALID_KEY, "Invalid key format or length");
        return 1;
    }

    char* endptr;
    errno        = 0;
    double value = strtod(argv[2], &endptr);
    if (errno != 0 || *endptr != '\0') {
        kv_print_error("set-double", KVSTORE_ERROR_INVALID_TYPE, "Invalid double value");
        return 1;
    }

    kvstore_error_t error = kvapi_set_double(g_api, argv[1], value);

    if (error != KVSTORE_OK) {
        kv_print_error("set-double", error, NULL);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_set_bool(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: set-bool <key> <true|false|1|0>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("set-bool", KVSTORE_ERROR_INVALID_KEY, "Invalid key format or length");
        return 1;
    }

    bool value;
    if (!kv_parse_bool(argv[2], &value)) {
        kv_print_error("set-bool", KVSTORE_ERROR_INVALID_TYPE, "Invalid boolean value (use true/false/1/0)");
        return 1;
    }

    kvstore_error_t error = kvapi_set_bool(g_api, argv[1], value);

    if (error != KVSTORE_OK) {
        kv_print_error("set-bool", error, NULL);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_set_null(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: set-null <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("set-null", KVSTORE_ERROR_INVALID_KEY, "Invalid key format or length");
        return 1;
    }

    kvstore_error_t error = kvapi_set_null(g_api, argv[1]);

    if (error != KVSTORE_OK) {
        kv_print_error("set-null", error, NULL);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_type(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: type <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("type", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    kvstore_type_t type;

    kvstore_error_t error = kvapi_get_type(g_api, argv[1], &type);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(unknown)\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        kv_print_error("type", error, NULL);
        return 1;
    }

    printf("%s\n", kvstore_type_string(type));
    return 0;
}

static int cmd_get(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: get <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("get", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    kvapi_result_t result;

    kvstore_error_t error = kvapi_get(g_api, argv[1], &result);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(nil)\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        kv_print_error("get", error, NULL);
        return 1;
    }

    switch (result.type) {
        case KVSTORE_TYPE_NULL:
            printf("null\n");
            break;
        case KVSTORE_TYPE_STRING:
            printf("\"%s\"\n", result.value.str_val ? result.value.str_val : "");
            break;
        case KVSTORE_TYPE_INT64:
            printf("(integer) %ld\n", result.value.int_val);
            break;
        case KVSTORE_TYPE_DOUBLE:
            printf("(double) %g\n", result.value.double_val);
            break;
        case KVSTORE_TYPE_BOOL:
            printf("(boolean) %s\n", result.value.bool_val ? "true" : "false");
            break;
        case KVSTORE_TYPE_BINARY:
            printf("(binary) %u bytes\n", result.value.binary_val.len);
            break;
        default:
            printf("(unknown)\n");
    }

    kvapi_free_result(&result);
    return 0;
}

static int cmd_get_int(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: get-int <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("get-int", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    int64_t value;

    kvstore_error_t error = kvapi_get_int64(g_api, argv[1], &value);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(nil)\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        kv_print_error("get-int", error, NULL);
        return 1;
    }

    printf("(integer) %ld\n", value);
    return 0;
}

static int cmd_get_double(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: get-double <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("get-double", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    double value;

    kvstore_error_t error = kvapi_get_double(g_api, argv[1], &value);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(nil)\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        kv_print_error("get-double", error, NULL);
        return 1;
    }

    printf("(double) %g\n", value);
    return 0;
}

static int cmd_get_bool(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: get-bool <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("get-bool", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    bool value;

    kvstore_error_t error = kvapi_get_bool(g_api, argv[1], &value);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(nil)\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        kv_print_error("get-bool", error, NULL);
        return 1;
    }

    printf("(boolean) %s\n", value ? "true" : "false");
    return 0;
}

static int cmd_del(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: del <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("del", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    bool deleted;

    kvstore_error_t error = kvapi_delete(g_api, argv[1], &deleted);

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(integer) 0\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        kv_print_error("del", error, NULL);
        return 1;
    }

    printf("(integer) %d\n", deleted ? 1 : 0);
    return 0;
}

static int cmd_exists(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: exists <key>\n");
        return 1;
    }

    if (!kv_validate_key(argv[1])) {
        kv_print_error("exists", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    bool exists = kvapi_exists(g_api, argv[1]);

    printf("(integer) %d\n", exists ? 1 : 0);
    return 0;
}

static int cmd_keys(int argc, char** argv) {
    (void)argc;
    (void)argv;

    kvstore_t* store = kvapi_store(g_api);
    size_t count     = kvstore_size(store);
    if (count == 0) {
        printf("(empty list or set)\n");
        return 0;
    }
    printf("%zu keys found:\n", count);

    kvstore_iterator_t iter = kvstore_iter_begin(store);
    size_t i                = 1;

    while (kvstore_iter_valid(&iter)) {
        const kvstore_entry_t* entry = kvstore_iter_get(&iter);
        printf("  %zu) \"%.*s\" (%s)\n", i++, (int)entry->key.len, entry->key.data,
               kvstore_type_string(entry->value.type));
        kvstore_iter_next(&iter);
    }
    return 0;
}

static int cmd_clear(int argc, char** argv) {
    (void)argc;
    (void)argv;

    kvstore_error_t error = kvapi_clear(g_api);

    if (error != KVSTORE_OK) {
        kv_print_error("clear", error, NULL);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_stats(int argc, char** argv) {
    (void)argc;
    (void)argv;

    kvapi_stats(g_api, stdout);
    return 0;
}

static int cmd_save(int argc, char** argv) {
    const char* filename = (argc > 1) ? argv[1] : g_config.db_file;

    kvstore_error_t error = kvapi_save(g_api, filename);

    if (error != KVSTORE_OK) {
        kv_print_error("save", error, filename);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_load(int argc, char** argv) {
    const char* filename = (argc > 1) ? argv[1] : g_config.db_file;

    kvstore_error_t error = kvapi_load(g_api, filename);

    if (error != KVSTORE_OK) {
        kv_print_error("load", error, filename);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_backup(int argc, char** argv) {
    const char* backup_file = (argc > 1) ? argv[1] : NULL;

    kvstore_error_t error = kvapi_backup(g_api, backup_file);

    if (error != KVSTORE_OK) {
        kv_print_error("backup", error, backup_file ? backup_file : "default");
        return 1;
    }

    if (!backup_file) {
        // Print generated filename - extend API to return it if needed
        printf("Backup created: %s.backup.timestamp (extend API for exact name)\n", g_config.db_file);
    } else {
        printf("Backup created: %s\n", backup_file);
    }
    return 0;
}

static int cmd_config(int argc, char** argv) {
    if (argc == 1) {
        printf("Current configuration:\n");
        printf("  capacity: %zu\n", g_config.capacity);
        printf("  db_file: %s\n", g_config.db_file);
        printf("  auto_save: %s\n", g_config.auto_save ? "true" : "false");
        printf("  auto_save_interval: %d seconds\n", g_config.auto_save_interval);
        return 0;
    }

    if (argc != 3) {
        printf("Usage: config <key> <value>\n");
        return 1;
    }

    const char* key     = argv[1];
    const char* value   = argv[2];
    bool config_changed = false;
    char* endptr        = NULL;

    if (strcmp(key, "capacity") == 0) {
        long new_capacity_long = strtol(value, &endptr, 10);
        if (endptr == value || *endptr != '\0' || new_capacity_long <= 0) {
            printf("Error: capacity must be a positive integer\n");
            return 1;
        }
        if ((unsigned long)new_capacity_long > SIZE_MAX) {
            printf("Error: capacity too large (max: %zu)\n", SIZE_MAX);
            return 1;
        }
        size_t new_capacity = (size_t)new_capacity_long;
        // Note: Resizing at runtime not supported in current API; log only
        g_config.capacity = new_capacity;
        config_changed    = true;
        printf("Capacity set to %zu (note: requires restart for effect)\n", g_config.capacity);

    } else if (strcmp(key, "db_file") == 0) {
        printf("Error: db_file cannot be changed at runtime. Restart with -f option.\n");
        return 1;

    } else if (strcmp(key, "auto_save") == 0) {
        if (strcmp(value, "true") == 0 || strcmp(value, "1") == 0) {
            g_config.auto_save = true;
            config_changed     = true;
            printf("Auto-save enabled\n");
        } else if (strcmp(value, "false") == 0 || strcmp(value, "0") == 0) {
            g_config.auto_save = false;
            config_changed     = true;
            printf("Auto-save disabled\n");
        } else {
            printf("Error: auto_save must be 'true', 'false', '1', or '0'\n");
            return 1;
        }

    } else if (strcmp(key, "auto_save_interval") == 0) {
        long interval = strtol(value, &endptr, 10);
        if (endptr == value || *endptr != '\0' || interval <= 0) {
            printf("Error: auto_save_interval must be a positive integer\n");
            return 1;
        }
        if (interval > INT_MAX) {
            printf("Error: auto_save_interval too large (max: %d)\n", INT_MAX);
            return 1;
        }
        g_config.auto_save_interval = (int)interval;
        config_changed              = true;
        printf("Auto-save interval set to %d seconds\n", g_config.auto_save_interval);

    } else {
        printf("Error: unknown configuration key '%s'\n", key);
        printf("Available keys: capacity, auto_save, auto_save_interval\n");
        return 1;
    }

    if (config_changed) {
        kv_log(LOG_INFO, "Configuration changed: %s = %s", key, value);
    }

    return 0;
}

static int cmd_quit(int argc, char** argv) {
    (void)argc;
    (void)argv;

    g_running = false;
    printf("Goodbye!\n");
    return 0;
}

// Main command dispatcher
static int execute_command(char* line) {
    if (!line || strlen(line) == 0) return 0;

    int argc;
    char** argv = kv_split_args(line, &argc);

    if (argc == 0) return 0;

    // Find and execute command
    for (const command_t* cmd = commands; cmd->name; cmd++) {
        if (strcmp(cmd->name, argv[0]) == 0) {
            return cmd->handler(argc, argv);
        }
    }

    printf("Unknown command: %s. Type 'help' for available commands.\n", argv[0]);
    return 1;
}

// Setup functions
static void setup_signals(void) {
    struct sigaction sa;

    // SIGINT handler
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGINT, &sa, NULL) != 0) {
        kv_log(LOG_ERROR, "Failed to set up SIGINT handler");
    }

    // SIGTERM handler
    if (sigaction(SIGTERM, &sa, NULL) != 0) {
        kv_log(LOG_ERROR, "Failed to set up SIGTERM handler");
    }

    // Ignore SIGPIPE
    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) != 0) {
        kv_log(LOG_ERROR, "Failed to set up SIGPIPE handler");
    }

    kv_log(LOG_DEBUG, "Signal handlers set up");
}

static void setup_readline(void) {
    // Set history file
    char* home = getenv("HOME");
    if (home) {
        char history_file[256];
        snprintf(history_file, sizeof(history_file), "%s/.kvstore_history", home);
        using_history();
        read_history(history_file);
        stifle_history(HISTORY_FILE_SIZE);
        kv_log(LOG_DEBUG, "Readline history loaded from %s", history_file);
    }

    kv_log(LOG_DEBUG, "Readline setup completed");
}

static void save_history(void) {
    char* home = getenv("HOME");
    if (home) {
        char history_file[256];
        snprintf(history_file, sizeof(history_file), "%s/.kvstore_history", home);
        write_history(history_file);
        kv_log(LOG_DEBUG, "Readline history saved to %s", history_file);
    }
}

static int load_config(const char* config_file) {
    FILE* file = fopen(config_file, "r");
    if (!file) {
        kv_log(LOG_DEBUG, "Config file %s not found, using defaults", config_file);
        return 0;
    }
    return kv_load_config(config_file, &g_config);
}

static void cleanup(void) {
    // Save history
    save_history();
    kv_cleanup(&g_config, g_api);
}

// Interactive REPL
static void repl(void) {
    printf("KV Store CLI v%d.%d.%d (using KVAPI)\n", KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR,
           KVSTORE_VERSION_PATCH);
    printf("Type 'help' for available commands.\n\n");

    while (g_running) {
        char* line = readline("kv> ");

        if (!line) {  // EOF (Ctrl+D)
            printf("\n");
            break;
        }

        if (strlen(line) > 0) {
            add_history(line);
            execute_command(line);
        }

        free(line);
    }
}

// Batch mode execution
static int execute_batch(const char* filename) {
    FILE* file = strcmp(filename, "-") == 0 ? stdin : fopen(filename, "r");
    if (!file) {
        kv_log(LOG_ERROR, "Failed to open batch file: %s", filename);
        return 1;
    }

    char line[MAX_COMMAND_LEN];
    int line_num  = 0;
    int exit_code = 0;

    while (fgets(line, sizeof(line), file) && g_running) {
        line_num++;

        // Remove trailing newline
        size_t len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }

        // Skip empty lines and comments
        if (strlen(line) == 0 || line[0] == '#') {
            continue;
        }

        printf("kv> %s\n", line);
        int result = execute_command(line);
        if (result != 0) {
            kv_log(LOG_ERROR, "Error on line %d", line_num);
            exit_code = result;
            if (file != stdin) {
                // Stop on first error in batch files (but not stdin)
                break;
            }
        }
    }

    if (file != stdin) {
        fclose(file);
    }

    return exit_code;
}

// Usage information
static void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("Options:\n");
    printf("  -f, --file <file>     Database file (default: %s)\n", DEFAULT_DB_FILE);
    printf("  -c, --capacity <n>    Initial capacity (default: %d)\n", KVSTORE_DEFAULT_CAPACITY);
    printf("  -b, --batch <file>    Execute commands from file ('-' for stdin)\n");
    printf("  -h, --help            Show this help\n");
    printf("  -v, --version         Show version information\n");
    printf("  --no-auto-save        Disable auto-save on exit\n");
}

static void print_version(void) {
    printf("KV Store CLI v%d.%d.%d (KVAPI wrapper)\n", KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR,
           KVSTORE_VERSION_PATCH);
}

int main(int argc, char** argv) {
    const char* batch_file  = NULL;
    const char* config_file = ".kvstore.conf";

    // Setup atexit handler
    if (atexit(cleanup) != 0) {
        fprintf(stderr, "Failed to set up exit handler\n");
        return 1;
    }

    // Parse command line options
    static struct option long_options[] = {
        {"file", required_argument, 0, 'f'},
        {"capacity", required_argument, 0, 'c'},
        {"batch", required_argument, 0, 'b'},
        {"help", no_argument, 0, 'h'},
        {"version", no_argument, 0, 'v'},
        {"no-auto-save", no_argument, 0, 'n'},
        {0, 0, 0, 0},
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "f:c:b:hv", long_options, NULL)) != -1) {
        switch (opt) {
            case 'f':
                g_config.db_file = optarg;
                break;
            case 'c':
                g_config.capacity = (size_t)atoi(optarg);
                if (g_config.capacity == 0) {
                    fprintf(stderr, "Invalid capacity: %s\n", optarg);
                    return 1;
                }
                break;
            case 'b':
                batch_file = optarg;
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 'v':
                print_version();
                return 0;
            case 'n':
                g_config.auto_save = false;
                break;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    // Setup logging and signals
    kv_log(LOG_INFO, "KV Store CLI starting (KVAPI mode)");
    setup_signals();

    // Load configuration
    load_config(config_file);

    // Setup readline
    setup_readline();

    // Create API handle with config
    kvapi_config_t api_config = {
        .capacity           = g_config.capacity,
        .db_file            = g_config.db_file,
        .auto_save          = g_config.auto_save,
        .auto_save_interval = g_config.auto_save_interval,
    };
    g_api = kvapi_init(&api_config);
    if (!g_api) {
        kv_log(LOG_ERROR, "Failed to create KVAPI handle with capacity %zu", g_config.capacity);
        return 1;
    }

    kv_log(LOG_INFO, "KVAPI handle created with capacity %zu", g_config.capacity);

    int exit_code = 0;

    // Execute in batch mode or interactive mode
    if (batch_file) {
        exit_code = execute_batch(batch_file);
    } else {
        repl();
    }
    return exit_code;
}
