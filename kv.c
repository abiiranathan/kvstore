#include "kvstore.h"

#include <errno.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>

#define DEFAULT_DB_FILE   "kvstore.db"
#define MAX_COMMAND_LEN   4096
#define MAX_CONFIG_LINE   256
#define HISTORY_FILE_SIZE 1000

// Configuration structure
typedef struct {
    size_t capacity;
    const char* db_file;
    bool auto_save;
    int auto_save_interval;
} config_t;

// Global state
static kvstore_t* g_store = NULL;
static config_t g_config  = {
     .capacity           = KVSTORE_DEFAULT_CAPACITY,
     .db_file            = DEFAULT_DB_FILE,
     .auto_save          = true,
     .auto_save_interval = 60,
};

static bool g_running              = true;
static pthread_mutex_t store_mutex = PTHREAD_MUTEX_INITIALIZER;
// Global flag for immediate exit
static volatile sig_atomic_t g_immediate_exit = 0;

// Logging levels
typedef enum { LOG_DEBUG, LOG_INFO, LOG_WARNING, LOG_ERROR } log_level_t;

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

// Utility function declarations
static void logger(log_level_t level, const char* format, ...);
static void print_error(const char* cmd, kvstore_error_t error, const char* details);
static bool validate_key(const char* key);
static bool validate_value_len(size_t value_len);
static char** split_args(char* line, int* argc);
static void setup_signals(void);
static void setup_readline(void);
static void save_history(void);
static int load_config(const char* config_file);
static void cleanup(void);

// Command table
static const command_t commands[] = {
    {"help", "help [command]", "Show help for commands", cmd_help},
    {"set", "set <key> <value>", "Set key to value", cmd_set},
    {"get", "get <key>", "Get value for key", cmd_get},
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

// Thread-safe store access macros
#define LOCK_STORE()                                                                                         \
    do {                                                                                                     \
        if (pthread_mutex_lock(&store_mutex) != 0) {                                                         \
            logger(LOG_ERROR, "Failed to lock store mutex");                                                 \
        }                                                                                                    \
    } while (0)

#define UNLOCK_STORE()                                                                                       \
    do {                                                                                                     \
        if (pthread_mutex_unlock(&store_mutex) != 0) {                                                       \
            logger(LOG_ERROR, "Failed to unlock store mutex");                                               \
        }                                                                                                    \
    } while (0)

// Logging implementation
static void logger(log_level_t level, const char* format, ...) {
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

    // Flush stderr for immediate error visibility
    if (output == stderr) {
        fflush(stderr);
    }
}

// Enhanced error printing
static void print_error(const char* cmd, kvstore_error_t error, const char* details) {
    if (details) {
        logger(LOG_ERROR, "Command %s failed: %s (%s)", cmd, kvstore_error_string(error), details);
    } else {
        logger(LOG_ERROR, "Command %s failed: %s", cmd, kvstore_error_string(error));
    }
}

// Input validation
static bool validate_key(const char* key) {
    if (!key || strlen(key) == 0) {
        logger(LOG_DEBUG, "Key validation failed: null or empty key");
        return false;
    }
    if (strlen(key) > KVSTORE_MAX_STRING_SIZE) {
        logger(LOG_DEBUG, "Key validation failed: key too long (%zu > %u)", strlen(key),
               KVSTORE_MAX_STRING_SIZE);
        return false;
    }
    // Add any other key restrictions here
    return true;
}

static bool validate_value_len(size_t value_len) {
    if (value_len > KVSTORE_MAX_STRING_SIZE) {
        logger(LOG_DEBUG, "Value validation failed: value too long (%zu > %u)", value_len,
               KVSTORE_MAX_STRING_SIZE);
        return false;
    }
    return true;
}

// Signal handler for graceful shutdown
static void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        // First signal - try graceful shutdown
        if (!g_immediate_exit) {
            logger(LOG_INFO, "Received signal %d, initiating graceful shutdown", sig);
            g_running        = false;
            g_immediate_exit = 1;

            // If we're in readline, we need to force exit
            rl_done = 1;  // Tell readline to return immediately
        } else {
            // Second signal - force immediate exit
            logger(LOG_INFO, "Received second signal %d, forcing immediate exit", sig);
            _exit(1);
        }
    }
}

// Argument splitting with bounds checking
static char** split_args(char* line, int* argc) {
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
            // If we reach end of line with unclosed quote, just accept it
            in_quote = false;
        }
    }

    args[*argc] = NULL;
    return args;
}

// Command implementations
static int cmd_help(int argc, char** argv) {
    if (argc == 1) {
        printf("Available commands:\n");
        for (const command_t* cmd = commands; cmd->name; cmd++) {
            printf("  %-20s %s\n", cmd->usage, cmd->description);
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

    if (!validate_key(argv[1])) {
        print_error("set", KVSTORE_ERROR_INVALID_KEY, "Invalid key format or length");
        return 1;
    }

    // Reconstruct the value from remaining arguments
    size_t value_len = 0;
    for (int i = 2; i < argc; i++) {
        value_len += strlen(argv[i]);
        if (i < argc - 1) value_len++;  // Space between args
    }

    if (!validate_value_len(value_len)) {
        print_error("set", KVSTORE_ERROR_STRING_TOO_LARGE, "Value too long");
        return 1;
    }

    // If there's only one value argument, use it directly
    if (argc == 3) {
        LOCK_STORE();
        kvstore_error_t error = kvstore_put_str(g_store, argv[1], argv[2]);
        UNLOCK_STORE();

        if (error != KVSTORE_OK) {
            print_error("set", error, NULL);
            return 1;
        }
    } else {
        // Multiple arguments, concatenate them
        char* value = malloc(value_len + 1);
        if (!value) {
            print_error("set", KVSTORE_ERROR_MEMORY, "Value allocation failed");
            return 1;
        }

        value[0] = '\0';
        for (int i = 2; i < argc; i++) {
            if (i > 2) {
                strncat(value, " ", value_len - strlen(value));
            }
            strncat(value, argv[i], value_len - strlen(value));
        }

        LOCK_STORE();
        kvstore_error_t error = kvstore_put_str(g_store, argv[1], value);
        UNLOCK_STORE();

        free(value);

        if (error != KVSTORE_OK) {
            print_error("set", error, NULL);
            return 1;
        }
    }

    printf("OK\n");
    return 0;
}

static int cmd_get(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: get <key>\n");
        return 1;
    }

    if (!validate_key(argv[1])) {
        print_error("get", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    const kvstore_string_t* value;
    LOCK_STORE();
    kvstore_error_t error = kvstore_get_str(g_store, argv[1], &value);
    UNLOCK_STORE();

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(nil)\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        print_error("get", error, NULL);
        return 1;
    }

    printf("\"%.*s\"\n", (int)value->len, value->data);
    return 0;
}

static int cmd_del(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: del <key>\n");
        return 1;
    }

    if (!validate_key(argv[1])) {
        print_error("del", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    LOCK_STORE();
    kvstore_error_t error = kvstore_delete_str(g_store, argv[1]);
    UNLOCK_STORE();

    if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("(integer) 0\n");
        return 0;
    } else if (error != KVSTORE_OK) {
        print_error("del", error, NULL);
        return 1;
    }

    printf("(integer) 1\n");
    return 0;
}

static int cmd_exists(int argc, char** argv) {
    if (argc != 2) {
        printf("Usage: exists <key>\n");
        return 1;
    }

    if (!validate_key(argv[1])) {
        print_error("exists", KVSTORE_ERROR_INVALID_KEY, "Invalid key format");
        return 1;
    }

    LOCK_STORE();
    bool exists = kvstore_exists_str(g_store, argv[1]);
    UNLOCK_STORE();

    printf("(integer) %d\n", exists ? 1 : 0);
    return 0;
}

static int cmd_keys(int argc, char** argv) {
    (void)argc;
    (void)argv;

    LOCK_STORE();
    size_t count = kvstore_size(g_store);
    if (count == 0) {
        UNLOCK_STORE();
        printf("(empty list or set)\n");
        return 0;
    }

    printf("%zu keys found:\n", count);

    kvstore_iterator_t iter = kvstore_iter_begin(g_store);
    size_t i                = 1;

    while (kvstore_iter_valid(&iter)) {
        const kvstore_pair_t* pair = kvstore_iter_get(&iter);
        printf("  %zu) \"%.*s\"\n", i++, (int)pair->key.len, pair->key.data);
        kvstore_iter_next(&iter);
    }
    UNLOCK_STORE();

    return 0;
}

static int cmd_clear(int argc, char** argv) {
    (void)argc;
    (void)argv;

    LOCK_STORE();
    kvstore_error_t error = kvstore_clear(g_store);
    UNLOCK_STORE();

    if (error != KVSTORE_OK) {
        print_error("clear", error, NULL);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_stats(int argc, char** argv) {
    (void)argc;
    (void)argv;

    LOCK_STORE();
    kvstore_print_stats(g_store);
    UNLOCK_STORE();
    return 0;
}

static int cmd_save(int argc, char** argv) {
    const char* filename = (argc > 1) ? argv[1] : g_config.db_file;

    LOCK_STORE();
    kvstore_error_t error = kvstore_save(g_store, filename);
    UNLOCK_STORE();

    if (error != KVSTORE_OK) {
        print_error("save", error, filename);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_load(int argc, char** argv) {
    const char* filename = (argc > 1) ? argv[1] : g_config.db_file;

    LOCK_STORE();
    kvstore_error_t error = kvstore_load(g_store, filename);
    UNLOCK_STORE();

    if (error != KVSTORE_OK) {
        print_error("load", error, filename);
        return 1;
    }

    printf("OK\n");
    return 0;
}

static int cmd_backup(int argc, char** argv) {
    const char* backup_file = (argc > 1) ? argv[1] : NULL;
    char default_backup[256];
    time_t now   = time(NULL);
    struct tm* t = localtime(&now);

    if (!backup_file) {
        snprintf(default_backup, sizeof(default_backup), "%s.backup.%04d%02d%02d-%02d%02d%02d",
                 g_config.db_file, t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour, t->tm_min,
                 t->tm_sec);
        backup_file = default_backup;
    }

    LOCK_STORE();
    kvstore_error_t error = kvstore_save(g_store, backup_file);
    UNLOCK_STORE();

    if (error != KVSTORE_OK) {
        print_error("backup", error, backup_file);
        return 1;
    }

    printf("Backup created: %s\n", backup_file);
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
        if (new_capacity < kvstore_size(g_store)) {
            printf("Error: new capacity (%zu) cannot be less than current size (%zu)\n", new_capacity,
                   kvstore_size(g_store));
            return 1;
        }
        g_config.capacity = new_capacity;
        config_changed    = true;
        printf("Capacity set to %zu\n", g_config.capacity);

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
        printf(
            "Available keys: capacity, auto_save, auto_save_interval, enable_monitoring, monitor_interval\n");
        return 1;
    }

    if (config_changed) {
        logger(LOG_INFO, "Configuration changed: %s = %s", key, value);
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
    char** argv = split_args(line, &argc);

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
        logger(LOG_ERROR, "Failed to set up SIGINT handler");
    }

    // SIGTERM handler
    if (sigaction(SIGTERM, &sa, NULL) != 0) {
        logger(LOG_ERROR, "Failed to set up SIGTERM handler");
    }

    // Ignore SIGPIPE
    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) != 0) {
        logger(LOG_ERROR, "Failed to set up SIGPIPE handler");
    }

    logger(LOG_DEBUG, "Signal handlers set up");
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
        logger(LOG_DEBUG, "Readline history loaded from %s", history_file);
    }

    logger(LOG_DEBUG, "Readline setup completed");
}

static void save_history(void) {
    char* home = getenv("HOME");
    if (home) {
        char history_file[256];
        snprintf(history_file, sizeof(history_file), "%s/.kvstore_history", home);
        write_history(history_file);
        logger(LOG_DEBUG, "Readline history saved to %s", history_file);
    }
}

static int load_config(const char* config_file) {
    FILE* file = fopen(config_file, "r");
    if (!file) {
        logger(LOG_DEBUG, "Config file %s not found, using defaults", config_file);
        return 0;
    }

    char line[MAX_CONFIG_LINE];
    int line_num = 0;

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

        // Parse config lines
        if (strncmp(line, "capacity=", 9) == 0) {
            g_config.capacity = (size_t)atoi(line + 9);
        } else if (strncmp(line, "db_file=", 8) == 0) {
            // For string values, we'd need to allocate memory
            // For simplicity, we'll just log it for now
            logger(LOG_INFO, "Config db_file setting ignored (runtime only)");
        } else if (strncmp(line, "auto_save=", 10) == 0) {
            g_config.auto_save = (strcmp(line + 10, "true") == 0);
        } else if (strncmp(line, "auto_save_interval=", 19) == 0) {
            g_config.auto_save_interval = atoi(line + 19);
        } else {
            logger(LOG_WARNING, "Unknown config option on line %d: %s", line_num, line);
        }
    }

    fclose(file);
    logger(LOG_INFO, "Configuration loaded from %s", config_file);
    return 0;
}

static void cleanup(void) {
    logger(LOG_INFO, "Cleaning up resources");

    // Save history
    save_history();

    // Auto-save if enabled
    if (g_config.auto_save && g_store) {
        LOCK_STORE();
        size_t store_size = kvstore_size(g_store);
        if (store_size > 0) {
            logger(LOG_INFO, "Auto-saving %zu key-value pairs", store_size);
            kvstore_error_t error = kvstore_save(g_store, g_config.db_file);
            if (error != KVSTORE_OK) {
                logger(LOG_ERROR, "Failed to auto-save: %s", kvstore_error_string(error));
            } else {
                logger(LOG_INFO, "Auto-save completed successfully");
            }
        }
        UNLOCK_STORE();
    }

    // Destroy store
    if (g_store) {
        logger(LOG_DEBUG, "Destroying key-value store");
        kvstore_destroy(g_store);
        g_store = NULL;
    }

    // Cleanup mutex
    if (pthread_mutex_destroy(&store_mutex) != 0) {
        logger(LOG_ERROR, "Failed to destroy store mutex");
    }

    logger(LOG_INFO, "Cleanup completed");
}

// Interactive REPL
static void repl(void) {
    printf("KV Store CLI v%d.%d.%d\n", KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR, KVSTORE_VERSION_PATCH);
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
        logger(LOG_ERROR, "Failed to open batch file: %s", filename);
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
            logger(LOG_ERROR, "Error on line %d", line_num);
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
    printf("  --no-monitoring       Disable health monitoring\n");
}

static void print_version(void) {
    printf("KV Store CLI v%d.%d.%d\n", KVSTORE_VERSION_MAJOR, KVSTORE_VERSION_MINOR, KVSTORE_VERSION_PATCH);
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
    logger(LOG_INFO, "KV Store CLI starting");
    setup_signals();

    // Load configuration
    load_config(config_file);

    // Setup readline
    setup_readline();

    // Create store with error checking
    g_store = kvstore_create(g_config.capacity);
    if (!g_store) {
        logger(LOG_ERROR, "Failed to create store with capacity %zu", g_config.capacity);
        return 1;
    }

    logger(LOG_INFO, "Store created with capacity %zu", g_config.capacity);

    // Load existing data
    LOCK_STORE();
    kvstore_error_t error = kvstore_load(g_store, g_config.db_file);
    UNLOCK_STORE();

    if (error != KVSTORE_OK && error != KVSTORE_ERROR_IO) {
        logger(LOG_ERROR, "Failed to load database: %s", kvstore_error_string(error));
        return 1;
    } else if (error == KVSTORE_OK) {
        size_t loaded = kvstore_size(g_store);
        if (loaded > 0) {
            logger(LOG_INFO, "Loaded %zu key-value pairs from %s", loaded, g_config.db_file);
        }
    }

    int exit_code = 0;

    // Execute in batch mode or interactive mode
    if (batch_file) {
        exit_code = execute_batch(batch_file);
    } else {
        repl();
    }

    return exit_code;
}
