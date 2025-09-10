#include <stdio.h>

#include <getopt.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kv_client.h"

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 7379
#define HISTORY_FILE ".kvcli_history"  // File in user's home directory

void print_usage(const char* prog_name) {
    printf("Usage: %s [OPTIONS]\n", prog_name);
    printf("Options:\n");
    printf("  -h, --host <hostname>    Server hostname (default: %s)\n", DEFAULT_HOST);
    printf("  -p, --port <port>        Server port (default: %d)\n", DEFAULT_PORT);
    printf("      --help               Show this help message\n");
}

int main(int argc, char** argv) {
    const char* host = DEFAULT_HOST;
    int port         = DEFAULT_PORT;

    static struct option long_options[] = {
        {"host", required_argument, 0, 'h'},
        {"port", required_argument, 0, 'p'},
        {"help", no_argument, 0, 1001},
        {0, 0, 0, 0},
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "h:p:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'h':
                host = optarg;
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 1001:
                print_usage(argv[0]);
                return 0;
            default:
                print_usage(argv[0]);
                return 1;
        }
    }

    // --- History file setup ---
    char history_path[1024];
    const char* home_dir = getenv("HOME");
    if (home_dir) {
        snprintf(history_path, sizeof(history_path), "%s/%s", home_dir, HISTORY_FILE);
        // Load command history from the file
        read_history(history_path);
    }

    kv_client_t* client = kv_connect(host, port);
    if (!client) {
        fprintf(stderr, "Could not connect to %s:%d. Connection failed.\n", host, port);
        return 1;
    }

    printf("Connected to %s:%d. Use up-arrow for history. Type 'quit' or 'exit' to leave.\n", host, port);

    char prompt[256];
    snprintf(prompt, sizeof(prompt), "%s:%d> ", host, port);

    char* line = NULL;
    while ((line = readline(prompt)) != NULL) {
        // If the line has content, add it to history
        if (*line) {
            add_history(line);
        }

        // Check for exit commands
        if (strcasecmp(line, "quit") == 0 || strcasecmp(line, "exit") == 0) {
            free(line);
            break;
        }

        // Send the command to the server
        kv_response_t* response = kv_command(client, line);
        if (!response) {
            fprintf(stderr, "Error: %s\n", kv_get_last_error(client));
            // If the error indicates a disconnect, we should exit.
            if (strstr(kv_get_last_error(client), "closed the connection") != NULL ||
                strstr(kv_get_last_error(client), "failed") != NULL) {
                free(line);
                break;
            }
        } else {
            kv_print_response(response);
            kv_free_response(response);
        }

        // readline allocates memory, so we must free it
        free(line);
    }

    // Save history to file on exit
    if (home_dir) {
        write_history(history_path);
    }

    kv_disconnect(client);
    printf("\nDisconnected.\n");

    return 0;
}
