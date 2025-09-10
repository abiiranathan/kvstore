#ifndef KV_CLIENT_H
#define KV_CLIENT_H

#include <stddef.h>
#include <stdint.h>

// Opaque client handle structure
typedef struct kv_client_t kv_client_t;

// Enum for the different response types
typedef enum {
    KV_RESPONSE_NIL,
    KV_RESPONSE_INTEGER,
    KV_RESPONSE_STRING,
    KV_RESPONSE_ERROR,
    KV_RESPONSE_ARRAY
} kv_response_type_e;

// Structure to hold a parsed response
typedef struct kv_response_t {
    kv_response_type_e type;
    union {
        int64_t integer;
        char* str;  // Used for STRING and ERROR types
        struct {
            size_t count;
            struct kv_response_t** elements;
        } array;
    } value;
} kv_response_t;

/**
 * @brief Connects to the KV server.
 *
 * @param host The hostname or IP address of the server.
 * @param port The port number of the server.
 * @return A pointer to a new kv_client_t handle on success, NULL on failure.
 */
kv_client_t* kv_connect(const char* host, int port);

/**
 * @brief Disconnects from the server and frees the client handle.
 *
 * @param client The client handle to disconnect.
 */
void kv_disconnect(kv_client_t* client);

/**
 * @brief Sends a command to the server and waits for a response.
 *
 * @param client The client handle.
 * @param cmd The command string to send (e.g., "GET mykey").
 * @return A pointer to a kv_response_t on success, NULL on failure.
 *         The caller is responsible for freeing the response with kv_free_response().
 */
kv_response_t* kv_command(kv_client_t* client, const char* cmd);

/**
 * @brief Frees a response object returned by kv_command.
 *
 * @param response The response object to free.
 */
void kv_free_response(kv_response_t* response);

/**
 * @brief Retrieves the last error message from the client handle.
 *
 * @param client The client handle.
 * @return A pointer to the null-terminated error string.
 */
const char* kv_get_last_error(const kv_client_t* client);

/**
 * @brief A utility function to print a response to stdout in a human-readable format.
 *
 * @param response The response object to print.
 */
void kv_print_response(const kv_response_t* response);

#endif /* KV_CLIENT_H */
