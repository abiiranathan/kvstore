#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../kvstore.h"

int main(void) {
    printf("=== KV Store Library Example ===\n\n");

    // Create a new store
    kvstore_t* store = kvstore_create(100);
    if (!store) {
        fprintf(stderr, "Failed to create store\n");
        return 1;
    }

    printf("1. Created store with capacity %zu\n", kvstore_capacity(store));

    // Add some key-value pairs
    printf("\n2. Adding key-value pairs...\n");

    kvstore_error_t error;

    error = kvstore_put_str(store, "name", "Alice");
    if (error != KVSTORE_OK) {
        fprintf(stderr, "Failed to set 'name': %s\n", kvstore_error_string(error));
    } else {
        printf("   Set: name -> Alice\n");
    }

    error = kvstore_put_str(store, "age", "30");
    if (error != KVSTORE_OK) {
        fprintf(stderr, "Failed to set 'age': %s\n", kvstore_error_string(error));
    } else {
        printf("   Set: age -> 30\n");
    }

    error = kvstore_put_str(store, "city", "New York");
    if (error != KVSTORE_OK) {
        fprintf(stderr, "Failed to set 'city': %s\n", kvstore_error_string(error));
    } else {
        printf("   Set: city -> New York\n");
    }

    // Binary data example
    const char binary_data[] = {0x01, 0x02, 0x03, 0x04, 0x00, 0x05};
    error                    = kvstore_put(store, "binary", 6, binary_data, sizeof(binary_data));
    if (error != KVSTORE_OK) {
        fprintf(stderr, "Failed to set binary data: %s\n", kvstore_error_string(error));
    } else {
        printf("   Set: binary -> (6 bytes of binary data)\n");
    }

    // Retrieve values
    printf("\n3. Retrieving values...\n");

    const kvstore_string_t* value;

    error = kvstore_get_str(store, "name", &value);
    if (error == KVSTORE_OK) {
        printf("   name = '%.*s'\n", (int)value->len, value->data);
    } else if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("   name = (not found)\n");
    } else {
        fprintf(stderr, "Error getting 'name': %s\n", kvstore_error_string(error));
    }

    error = kvstore_get_str(store, "age", &value);
    if (error == KVSTORE_OK) {
        printf("   age = '%.*s'\n", (int)value->len, value->data);
    } else if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("   age = (not found)\n");
    } else {
        fprintf(stderr, "Error getting 'age': %s\n", kvstore_error_string(error));
    }

    error = kvstore_get(store, "binary", 6, &value);
    if (error == KVSTORE_OK) {
        printf("   binary = (");
        for (uint32_t i = 0; i < value->len; i++) {
            printf("0x%02x", (unsigned char)value->data[i]);
            if (i < value->len - 1) printf(" ");
        }
        printf(")\n");
    } else {
        fprintf(stderr, "Error getting binary data: %s\n", kvstore_error_string(error));
    }

    // Test key existence
    printf("\n4. Testing key existence...\n");
    printf("   'name' exists: %s\n", kvstore_exists_str(store, "name") ? "yes" : "no");
    printf("   'email' exists: %s\n", kvstore_exists_str(store, "email") ? "yes" : "no");

    // Update existing key
    printf("\n5. Updating existing key...\n");
    error = kvstore_put_str(store, "age", "31");
    if (error == KVSTORE_OK) {
        printf("   Updated age to 31\n");

        error = kvstore_get_str(store, "age", &value);
        if (error == KVSTORE_OK) {
            printf("   age = '%.*s' (after update)\n", (int)value->len, value->data);
        }
    }

    // Iterate through all pairs
    printf("\n6. Iterating through all key-value pairs...\n");
    kvstore_iterator_t iter = kvstore_iter_begin(store);
    int index               = 1;

    while (kvstore_iter_valid(&iter)) {
        const kvstore_pair_t* pair = kvstore_iter_get(&iter);
        printf("   [%d] '%.*s' -> '%.*s'\n", index++, (int)pair->key.len, pair->key.data,
               (int)pair->value.len, pair->value.data);
        kvstore_iter_next(&iter);
    }

    // Show statistics
    printf("\n7. Store statistics:\n");
    kvstore_print_stats(store);

    // Save to file
    printf("\n8. Saving to file...\n");
    error = kvstore_save(store, "example.db");
    if (error == KVSTORE_OK) {
        printf("   Saved successfully to example.db\n");
    } else {
        fprintf(stderr, "   Failed to save: %s\n", kvstore_error_string(error));
    }

    // Clear and reload
    printf("\n9. Clearing store and reloading...\n");
    size_t old_count = kvstore_size(store);

    error = kvstore_clear(store);
    if (error == KVSTORE_OK) {
        printf("   Cleared store (was %zu pairs, now %zu pairs)\n", old_count, kvstore_size(store));
    }

    error = kvstore_load(store, "example.db");
    if (error == KVSTORE_OK) {
        printf("   Reloaded from file (%zu pairs restored)\n", kvstore_size(store));

        // Verify one key
        error = kvstore_get_str(store, "name", &value);
        if (error == KVSTORE_OK) {
            printf("   Verification: name = '%.*s'\n", (int)value->len, value->data);
        }
    } else {
        fprintf(stderr, "   Failed to reload: %s\n", kvstore_error_string(error));
    }

    // Delete a key
    printf("\n10. Deleting a key...\n");
    error = kvstore_delete_str(store, "city");
    if (error == KVSTORE_OK) {
        printf("   Deleted 'city' key\n");
        printf("   Store now has %zu pairs\n", kvstore_size(store));
    } else if (error == KVSTORE_ERROR_KEY_NOT_FOUND) {
        printf("   Key 'city' was not found\n");
    } else {
        fprintf(stderr, "   Error deleting key");
    }
    return 0;
}
